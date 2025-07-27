package tracker

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"math/rand/v2"
	"net/netip"
	"sync"
	"time"

	"github.com/bitmagnet-io/bitmagnet/internal/protocol"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol/dht/server"
)

type Client struct {
	stopped   chan struct{}
	mutex     sync.Mutex
	localAddr netip.AddrPort
	socket    server.Socket
	queries   map[int32]chan []byte
}

func New() (*Client, error) {
	c := Client{
		stopped: make(chan struct{}),
		localAddr: netip.AddrPortFrom(
			netip.IPv4Unspecified(),
			3335,
		),
		socket:  server.NewSocket(),
		queries: make(map[int32]chan []byte),
	}

	if err := c.start(); err != nil {
		return &c, err
	}

	return &c, nil
}

func (c *Client) start() error {
	if err := c.socket.Open(c.localAddr); err != nil {
		return fmt.Errorf("could not open socket: %w", err)
	}

	go func() {
		ctx, cancel := context.WithCancel(context.Background())
		go c.read(ctx)
		<-c.stopped
		cancel()

		_ = c.socket.Close()
	}()

	return nil
}

type Header struct {
	Action        int32
	TransactionId int32
}

func (c *Client) read(ctx context.Context) {
	buffer := make([]byte, 65507)

	for {
		if ctx.Err() != nil {
			return
		}

		n, _, err := c.socket.Receive(buffer)
		if err != nil {
			// Socket is probably closed; if we're not shutting down then panic
			if ctx.Err() == nil {
				panic(fmt.Errorf("socket read error: %w", err))
			}

			return
		}

		if n == 0 {
			/* Datagram sockets in various domains  (e.g., the UNIX and Internet domains) permit
			 * zero-length datagrams. When such a datagram is received, the return value (n) is 0.
			 */
			continue
		}

		if n < 8 {
			// Not enough bytes to read transaction ID
			continue
		}

		reader := bytes.NewReader(buffer)

		var header Header

		if err := binary.Read(reader, binary.BigEndian, &header); err != nil {
			return
		}

		c.mutex.Lock()
		ch, ok := c.queries[header.TransactionId]
		c.mutex.Unlock()

		if ok {
			ch <- buffer[:n]
		}
	}
}

type ConnectRequest struct {
	ProtocolId    int64
	Action        int32
	TransactionId int32
}

type ConnectResponse struct {
	Action        int32
	TransactionId int32
	ConnectionId  int64
}

type AnnounceRequest struct {
	ConnectionId  int64
	Action        int32
	TransactionId int32
	InfoHash      protocol.ID
	PeerId        protocol.ID
	Downloaded    int64
	Left          int64
	Uploaded      int64
	Event         int32
	IpAddress     int32
	Key           int32
	NumWant       int32
	Port          int16
}

type AnnounceResponse struct {
	Action        int32
	TransactionId int32
	Interval      int32
	Leechers      int32
	Seeders       int32
}

type IpAndPort struct {
	Ip   [4]byte
	Port uint16
}

func (c *Client) GetPeers(ctx context.Context, tracker netip.AddrPort, infoHash protocol.ID) ([]netip.AddrPort, error) {
	ctx, cancel := context.WithTimeout(ctx, time.Second*10)

	transaction_id := rand.Int32()

	ch := make(chan []byte)
	c.mutex.Lock()
	c.queries[transaction_id] = ch
	c.mutex.Unlock()

	defer func() {
		c.mutex.Lock()
		delete(c.queries, transaction_id)
		c.mutex.Unlock()

		cancel()
	}()

	buf := &bytes.Buffer{}

	connect_request := ConnectRequest{
		ProtocolId:    0x41727101980,
		Action:        0,
		TransactionId: transaction_id,
	}

	if err := binary.Write(buf, binary.BigEndian, connect_request); err != nil {
		return nil, err
	}
	c.socket.Send(tracker, buf.Bytes())

	var resp []byte

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case resp = <-ch:
	}

	var connect_response ConnectResponse
	if err := binary.Read(bytes.NewReader(resp), binary.BigEndian, &connect_response); err != nil {
		return nil, err
	}

	connection_id := connect_response.ConnectionId

	announce_request := AnnounceRequest{
		ConnectionId:  connection_id,
		Action:        1,
		TransactionId: transaction_id,
		InfoHash:      infoHash,
		PeerId:        protocol.RandomNodeID(),
		Downloaded:    0,
		Left:          0,
		Uploaded:      0,
		Event:         0,
		IpAddress:     0,
		Key:           rand.Int32(),
		NumWant:       -1,
		Port:          0,
	}

	buf.Reset()
	if err := binary.Write(buf, binary.BigEndian, announce_request); err != nil {
		return nil, err
	}
	c.socket.Send(tracker, buf.Bytes())

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case resp = <-ch:
	}

	reader := bytes.NewReader(resp)
	var announce_response AnnounceResponse
	if err := binary.Read(reader, binary.BigEndian, &announce_response); err != nil {
		return nil, err
	}

	peers := make([]netip.AddrPort, 0)

	var ip_and_port IpAndPort
	for err := binary.Read(reader, binary.BigEndian, &ip_and_port); err == nil; err = binary.Read(reader, binary.BigEndian, &ip_and_port) {
		peer := netip.AddrPortFrom(netip.AddrFrom4(ip_and_port.Ip), ip_and_port.Port)
		peers = append(peers, peer)
	}

	return peers, nil
}
