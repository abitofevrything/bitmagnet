package dhtcrawler

import (
	"context"
	"fmt"
	"net"
	"net/netip"
	"net/url"
	"time"

	"github.com/bitmagnet-io/bitmagnet/internal/protocol/dht/ktable"
	"github.com/prometheus/client_golang/prometheus"
)

func (c *crawler) runGetPeers(ctx context.Context) {
	trackers := make([]netip.AddrPort, 0)
	for _, raw_url := range c.trackers {
		url, err := url.Parse(raw_url)
		if err != nil {
			continue
		}

		addr, err := net.ResolveUDPAddr("udp", url.Host)
		if err != nil {
			continue
		}

		trackers = append(trackers, addr.AddrPort())
	}

	for _, tracker := range trackers {
		fmt.Printf("tracker: %s on port %d\n", tracker.Addr(), tracker.Port())
	}

	_ = c.getPeers.Run(ctx, func(req nodeHasPeersForHash) {

		tracker_peers := make([]netip.AddrPort, 0)

		for _, tracker := range trackers {
			// fmt.Printf("Requesting peers for %s from %s\n", req.infoHash, tracker)

			res, err := c.tracker_client.GetPeers(ctx, tracker, req.infoHash)
			if err != nil {
				fmt.Printf("error from tracker: %s\n", err)
			}

			// fmt.Printf("Got %d peers from %s\n", len(res), tracker)

			tracker_peers = append(tracker_peers, res...)
		}

		pfh, pfhErr := c.requestPeersForHash(ctx, req)
		if pfhErr != nil {
			return
		}

		peers := append(tracker_peers, pfh.peers...)

		unique_peers := make([]netip.AddrPort, 0)
		peer_map := make(map[netip.AddrPort]struct{})

		for _, peer := range peers {
			if _, ok := peer_map[peer]; !ok {
				peer_map[peer] = struct{}{}
				unique_peers = append(unique_peers, peer)
			}
		}

		if len(unique_peers) == 0 {
			return
		}

		fmt.Printf("Total unique peers for %s: %d (%d from DHT)\n", req.infoHash, len(unique_peers), len(pfh.peers))

		hashPeers := make([]ktable.HashPeer, 0, len(unique_peers))

		for _, p := range unique_peers {
			hashPeers = append(hashPeers, ktable.HashPeer{
				Addr: p,
			})
		}

		c.kTable.BatchCommand(
			ktable.PutHash{ID: req.infoHash, Peers: hashPeers},
		)
		select {
		case <-ctx.Done():
			return
		case c.requestMetaInfo.In() <- infoHashWithPeers{
			nodeHasPeersForHash: req,
			peers:               unique_peers,
		}:
			return
		}
	})
}

func (c *crawler) requestPeersForHash(
	ctx context.Context,
	req nodeHasPeersForHash,
) (infoHashWithPeers, error) {
	res, err := c.client.GetPeers(ctx, req.node, req.infoHash)
	if err != nil {
		c.kTable.BatchCommand(ktable.DropAddr{
			Addr:   req.node.Addr(),
			Reason: fmt.Errorf("failed to get peers: %w", err),
		})

		return infoHashWithPeers{}, err
	}

	c.kTable.BatchCommand(ktable.PutNode{
		ID:      res.ID,
		Addr:    req.node,
		Options: []ktable.NodeOption{ktable.NodeResponded()},
	})

	c.getPeersPeerCount.Observe(float64(len(res.Values)))
	c.getPeersNodeCount.Observe(float64(len(res.Nodes)))

	if len(res.Nodes) > 0 {
		// block the channel for up to a second in an attempt to add the nodes to the discoveredNodes channel
		cancelCtx, cancel := context.WithTimeout(ctx, time.Second)

		processed := 0

	nodes:
		for _, n := range res.Nodes {
			select {
			case <-cancelCtx.Done():
				break nodes
			case c.discoveredNodes.In() <- ktable.NewNode(n.ID, n.Addr):
				processed++
			}
		}

		c.getPeersNodeTotal.With(prometheus.Labels{"result": "discovered_nodes"}).Add(float64(processed))
		c.getPeersNodeTotal.With(prometheus.Labels{"result": "skipped"}).Add(float64(len(res.Nodes) - processed))

		cancel()
	}

	return infoHashWithPeers{
		nodeHasPeersForHash: req,
		peers:               res.Values,
	}, nil
}
