package dhtcrawler

import (
	"context"
	"errors"
	"fmt"
	"net/netip"
	"sync"

	"github.com/bitmagnet-io/bitmagnet/internal/protocol"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol/metainfo/metainforequester"
	"github.com/prometheus/client_golang/prometheus"
)

func (c *crawler) runRequestMetaInfo(ctx context.Context) {
	_ = c.requestMetaInfo.Run(ctx, func(req infoHashWithPeers) {
		mi, reqErr := c.doRequestMetaInfo(ctx, req.infoHash, req.peers)
		if reqErr != nil {
			return
		}
		select {
		case <-ctx.Done():
		case c.persistTorrents.In() <- infoHashWithMetaInfo{
			nodeHasPeersForHash: req.nodeHasPeersForHash,
			metaInfo:            mi.Info,
		}:
		}
	})
}

var (
	success = 0
	err     = 0
)

func (c *crawler) doRequestMetaInfo(
	ctx context.Context,
	hash protocol.ID,
	peers []netip.AddrPort,
) (metainforequester.Response, error) {
	var errs []error

	errsMutex := sync.Mutex{}
	addErr := func(err error) {
		errsMutex.Lock()
		errs = append(errs, err)
		errsMutex.Unlock()
	}

	ch := make(chan *metainforequester.Response)
	remaining := len(peers)
	blocked := false
	closed := false
	remaining_mutex := &sync.Mutex{}

	for _, p := range peers {
		go func() {
			defer func() {
				remaining_mutex.Lock()
				remaining--

				if remaining <= 0 && !closed {
					closed = true
					close(ch)
				}

				remaining_mutex.Unlock()
			}()

			res, err := c.metainfoRequester.Request(ctx, hash, p)
			if err != nil {
				addErr(err)
				return
			}

			remaining_mutex.Lock()
			defer remaining_mutex.Unlock()

			if closed {
				return
			} else {
				closed = true
				defer close(ch)
			}

			if banErr := c.banningChecker.Check(res.Info); banErr != nil {
				blocked = true
				_ = c.blockingManager.Block(ctx, []protocol.ID{hash}, false)
				return
			}

			ch <- &res
		}()
	}

	res := <-ch

	fmt.Printf("%d/%d\n", success, err)

	if res == nil {
		err++
		if !blocked {
			c.requestMetaInfoTotal.With(prometheus.Labels{"result": "error"}).Inc()
		} else {
			c.requestMetaInfoTotal.With(prometheus.Labels{"result": "blocked"}).Inc()
		}

		return metainforequester.Response{}, errors.Join(errs...)
	}

	success++
	c.requestMetaInfoTotal.With(prometheus.Labels{"result": "persist_torrents"}).Inc()
	return *res, nil
}
