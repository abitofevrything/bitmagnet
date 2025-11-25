package dhtcrawler

import (
	"context"
	"database/sql/driver"
	"fmt"
	"net/netip"
	"strconv"
	"time"

	"github.com/bitmagnet-io/bitmagnet/internal/concurrency"
	"github.com/bitmagnet-io/bitmagnet/internal/model"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol/dht/ktable"
	"github.com/prometheus/client_golang/prometheus"
)

func (c *crawler) handleDiscoveredInfohashes(ctx context.Context) {
	processBatch := concurrency.NewBatchingChannel[protocol.ID](100, databaseBatchSize, databaseBatchInterval)

	prevWaitingNodes := make(map[protocol.ID][]ktable.Node)
	waitingNodes := make(map[protocol.ID][]ktable.Node)

	prevPendingMetaInfoNodes := make(map[protocol.ID][]ktable.Node)
	pendingMetaInfoNodes := make(map[protocol.ID][]ktable.Node)

	prevPendingScrapeNodes := make(map[protocol.ID][]ktable.Node)
	pendingScrapeNodes := make(map[protocol.ID][]ktable.Node)

	rotate := time.After(c.hashRotationInterval)
	nextRequestMetaInfo := time.After(c.requestMetaInfoLimit.lim.Reserve().Delay())
	nextScrape := time.After(c.scrapeLimit.lim.Reserve().Delay())

	for {
		select {
		case <-ctx.Done():
			return
		case batch := <-processBatch.Out():
			go c.processInfohashes(ctx, batch)
		case <-nextRequestMetaInfo:
			var bestInfoHash protocol.ID
			var bestNodes []ktable.Node

			for hash, nodes := range prevPendingMetaInfoNodes {
				if len(nodes) > len(bestNodes) {
					bestInfoHash = hash
					bestNodes = nodes
				}
			}

			if len(bestNodes) == 0 {
				for hash, nodes := range pendingMetaInfoNodes {
					if len(nodes) > len(bestNodes) {
						bestInfoHash = hash
						bestNodes = nodes
					}
				}
			}

			if len(bestNodes) == 0 {
				nextRequestMetaInfo = time.After(100 * time.Millisecond)
				continue
			}

			delete(prevPendingMetaInfoNodes, bestInfoHash)
			delete(pendingMetaInfoNodes, bestInfoHash)

			c.recentlyProcessedInfoHashes.Add(bestInfoHash.Bytes())

			c.totalProcessedHashes.With(prometheus.Labels{"result": "request_metainfo", "num_nodes": strconv.Itoa(len(bestNodes))}).Inc()

			go c.requestMetaInfo(ctx, bestInfoHash, bestNodes)

			nextRequestMetaInfo = time.After(c.requestMetaInfoLimit.lim.Reserve().Delay())
		case <-nextScrape:
			var bestInfoHash protocol.ID
			var bestNodes []ktable.Node

			for hash, nodes := range prevPendingScrapeNodes {
				if len(nodes) > len(bestNodes) {
					bestInfoHash = hash
					bestNodes = nodes
				}
			}

			if len(bestNodes) == 0 {
				for hash, nodes := range pendingScrapeNodes {
					if len(nodes) > len(bestNodes) {
						bestInfoHash = hash
						bestNodes = nodes
					}
				}
			}

			if len(bestNodes) == 0 {
				nextScrape = time.After(100 * time.Millisecond)
				continue
			}

			delete(prevPendingScrapeNodes, bestInfoHash)
			delete(pendingScrapeNodes, bestInfoHash)

			c.recentlyProcessedInfoHashes.Add(bestInfoHash.Bytes())

			c.totalProcessedHashes.With(prometheus.Labels{"result": "scrape", "num_nodes": strconv.Itoa(len(bestNodes))}).Inc()

			go c.scrape(ctx, bestInfoHash, bestNodes)

			nextScrape = time.After(c.scrapeLimit.lim.Reserve().Delay())
		case hash := <-c.infoHashesToRequestMetaInfo:
			if nodes, ok := prevWaitingNodes[hash]; ok {
				pendingMetaInfoNodes[hash] = nodes
				delete(prevWaitingNodes, hash)
			} else {
				pendingMetaInfoNodes[hash] = waitingNodes[hash]
				delete(waitingNodes, hash)
			}
		case hash := <-c.infoHashesToScrape:
			if nodes, ok := prevWaitingNodes[hash]; ok {
				pendingScrapeNodes[hash] = nodes
				delete(prevWaitingNodes, hash)
			} else {
				pendingScrapeNodes[hash] = waitingNodes[hash]
				delete(waitingNodes, hash)
			}
		case <-rotate:
			for _, nodes := range prevWaitingNodes {
				c.totalProcessedHashes.With(prometheus.Labels{"result": "skipped", "num_nodes": strconv.Itoa(len(nodes))}).Inc()
			}
			for _, nodes := range prevPendingMetaInfoNodes {
				c.totalProcessedHashes.With(prometheus.Labels{"result": "dropped_pending_metainfo", "num_nodes": strconv.Itoa(len(nodes))}).Inc()
			}
			for _, nodes := range prevPendingScrapeNodes {
				c.totalProcessedHashes.With(prometheus.Labels{"result": "dropped_pending_scrape", "num_nodes": strconv.Itoa(len(nodes))}).Inc()
			}

			prevWaitingNodes = waitingNodes
			waitingNodes = make(map[protocol.ID][]ktable.Node)

			prevPendingMetaInfoNodes = pendingMetaInfoNodes
			pendingMetaInfoNodes = make(map[protocol.ID][]ktable.Node)

			prevPendingScrapeNodes = pendingScrapeNodes
			pendingScrapeNodes = make(map[protocol.ID][]ktable.Node)

			rotate = time.After(c.hashRotationInterval)
		case req := <-c.discoveredInfoHashes:
			c.totalDiscoveredHashes.Inc()

			if c.recentlyProcessedInfoHashes.Test(req.infoHash.Bytes()) {
				c.totalProcessedHashes.With(prometheus.Labels{"result": "already_processed", "num_nodes": "1"}).Inc()
				continue
			}

			if _, ok := prevPendingMetaInfoNodes[req.infoHash]; ok {
				prevPendingMetaInfoNodes[req.infoHash] = append(prevPendingMetaInfoNodes[req.infoHash], req.node)
			} else if _, ok := pendingMetaInfoNodes[req.infoHash]; ok {
				pendingMetaInfoNodes[req.infoHash] = append(pendingMetaInfoNodes[req.infoHash], req.node)
			} else if _, ok := prevPendingScrapeNodes[req.infoHash]; ok {
				prevPendingScrapeNodes[req.infoHash] = append(prevPendingScrapeNodes[req.infoHash], req.node)
			} else if _, ok := pendingScrapeNodes[req.infoHash]; ok {
				pendingScrapeNodes[req.infoHash] = append(pendingScrapeNodes[req.infoHash], req.node)
			} else {
				if _, ok := prevWaitingNodes[req.infoHash]; ok {
					prevWaitingNodes[req.infoHash] = append(prevWaitingNodes[req.infoHash], req.node)
				} else {
					waitingNodes[req.infoHash] = append(waitingNodes[req.infoHash], req.node)
				}

				if c.recentlyScheduledInfoHashes.TestAndAdd(req.infoHash.Bytes()) {
					continue
				}

				processBatch.In() <- req.infoHash
			}
		}
	}
}

type triageResult struct {
	InfoHash    protocol.ID
	FilesStatus model.FilesStatus
	FilesCount  model.NullUint
	Seeders     model.NullUint
	Leechers    model.NullUint
	UpdatedAt   time.Time
}

func (c *crawler) processInfohashes(ctx context.Context, reqs []protocol.ID) {
	filteredHashes, filterErr := c.blockingManager.Filter(ctx, reqs)
	if filterErr != nil {
		c.logger.Errorf("failed to filter infohashes: %s", filterErr.Error())
		return
	}

	if len(filteredHashes) == 0 {
		return
	}

	valuers := make([]driver.Valuer, 0, len(filteredHashes))

	for _, h := range filteredHashes {
		valuers = append(valuers, h)
	}

	var result []*triageResult
	if queryErr := c.dao.Torrent.WithContext(ctx).Select(
		c.dao.Torrent.InfoHash,
		c.dao.Torrent.FilesStatus,
		c.dao.Torrent.FilesCount,
		c.dao.TorrentsTorrentSource.Seeders,
		c.dao.TorrentsTorrentSource.Leechers,
		c.dao.TorrentsTorrentSource.UpdatedAt,
	).LeftJoin(
		c.dao.TorrentsTorrentSource,
		c.dao.Torrent.InfoHash.EqCol(c.dao.TorrentsTorrentSource.InfoHash),
		c.dao.TorrentsTorrentSource.Source.Eq("dht"),
	).Where(
		c.dao.Torrent.InfoHash.In(valuers...),
	).UnderlyingDB().Find(&result).Error; queryErr != nil {
		c.logger.Errorf("failed to search existing torrents: %s", queryErr.Error())
		return
	}

	foundTorrents := make(map[protocol.ID]triageResult)
	for _, t := range result {
		foundTorrents[t.InfoHash] = *t
	}

	for _, h := range filteredHashes {
		if t, ok := foundTorrents[h]; !ok ||
			t.FilesStatus == model.FilesStatusNoInfo ||
			(t.FilesStatus != model.FilesStatusSingle && !t.FilesCount.Valid) ||
			(t.FilesStatus == model.FilesStatusOverThreshold && t.FilesCount.Uint <= c.saveFilesThreshold) {

			c.infoHashesToRequestMetaInfo <- h
		} else if (!t.Seeders.Valid || !t.Leechers.Valid) ||
			t.UpdatedAt.Before(time.Now().Add(-c.rescrapeThreshold)) {

			c.infoHashesToScrape <- h
		} else {
			// Skip
			c.recentlyProcessedInfoHashes.Add(h.Bytes())
		}
	}
}

func (c *crawler) requestMetaInfo(ctx context.Context, infoHash protocol.ID, nodes []ktable.Node) {
	seenNodes := make(map[netip.AddrPort]struct{})
	seenPeers := make(map[netip.AddrPort]struct{})

	for _, node := range nodes {
		if _, ok := seenNodes[node.Addr()]; ok {
			continue
		}
		seenNodes[node.Addr()] = struct{}{}

		peersRes, err := c.client.GetPeersScrape(ctx, node.Addr(), infoHash)
		if err != nil {
			c.kTable.BatchCommand(ktable.DropAddr{
				Addr:   node.Addr().Addr(),
				Reason: fmt.Errorf("failed to get peers: %w", err),
			})

			continue
		}

		c.kTable.BatchCommand(ktable.PutNode{
			ID:      peersRes.ID,
			Addr:    node.Addr(),
			Options: []ktable.NodeOption{ktable.NodeResponded()},
		})

		peers := peersRes.Values
		// Some nodes don't return peers when doing a DHT scrape
		// (see for example https://github.com/arvidn/libtorrent/issues/8005)
		// Try again without scrape.
		if len(peers) == 0 {
			newPeersRes, err := c.client.GetPeers(ctx, node.Addr(), infoHash)
			if err != nil {
				c.kTable.BatchCommand(ktable.DropAddr{
					Addr:   node.Addr().Addr(),
					Reason: fmt.Errorf("failed to get peers: %w", err),
				})

				continue
			}

			peers = newPeersRes.Values
		}

		for _, node := range peersRes.Nodes {
			c.discoveredNodes <- ktable.NewNode(node.ID, node.Addr)
		}

		for _, p := range peers {
			if _, ok := seenPeers[p]; ok {
				continue
			}
			seenPeers[p] = struct{}{}

			res, err := c.metainfoRequester.Request(ctx, infoHash, p)
			if err != nil {
				continue
			}

			if banErr := c.banningChecker.Check(res.Info); banErr != nil {
				_ = c.blockingManager.Block(ctx, []protocol.ID{infoHash}, false)
				return
			}

			var scrape *hashWithScrape
			if peersRes.BfPeers != nil && peersRes.BfSeeders != nil {
				scrape = &hashWithScrape{
					infoHash: infoHash,
					seeders:  peersRes.BfSeeders.ApproximatedSize(),
					leechers: peersRes.BfPeers.ApproximatedSize(),
				}
			}

			c.torrentsToPersist.In() <- hashWithMetaInfo{
				infoHash: infoHash,
				metaInfo: res.Info,
				scrape:   scrape,
			}

			return
		}
	}
}

func (c *crawler) scrape(ctx context.Context, infoHash protocol.ID, nodes []ktable.Node) {
	seenNodes := make(map[netip.AddrPort]struct{})

	for _, node := range nodes {
		if _, ok := seenNodes[node.Addr()]; ok {
			continue
		}
		seenNodes[node.Addr()] = struct{}{}

		res, err := c.client.GetPeersScrape(ctx, node.Addr(), infoHash)
		if err != nil {
			c.kTable.BatchCommand(ktable.DropAddr{
				Addr:   node.Addr().Addr(),
				Reason: fmt.Errorf("failed to get peers from p: %w", err),
			})

			continue
		}

		c.kTable.BatchCommand(ktable.PutNode{
			ID:      res.ID,
			Addr:    node.Addr(),
			Options: []ktable.NodeOption{ktable.NodeResponded()},
		})

		for _, node := range res.Nodes {
			c.discoveredNodes <- ktable.NewNode(node.ID, node.Addr)
		}

		if res.BfPeers == nil || res.BfSeeders == nil {
			continue
		}

		c.scrapesToPersist.In() <- hashWithScrape{
			infoHash: infoHash,
			seeders:  res.BfSeeders.ApproximatedSize(),
			leechers: res.BfPeers.ApproximatedSize(),
		}

		return
	}
}
