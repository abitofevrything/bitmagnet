package dhtcrawler

import (
	"context"
	"database/sql/driver"
	"fmt"
	"time"

	"github.com/bitmagnet-io/bitmagnet/internal/concurrency"
	"github.com/bitmagnet-io/bitmagnet/internal/model"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol/dht/ktable"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol/dht/ktable/btree"
)

func (c *crawler) handleDiscoveredInfohashes(ctx context.Context) {
	batchedChannel := concurrency.NewBatchingChannel[nodeWithHash](100, databaseBatchSize, databaseBatchInterval)

	for {
		select {
		case <-ctx.Done():
			return
		case batch := <-batchedChannel.Out():
			go c.processInfohashes(ctx, batch)
		case req := <-c.discoveredInfoHashes:
			c.totalDiscoveredHashes.Inc()

			if c.recentlyProcessedInfoHashes.TestAndAdd(req.infoHash.Bytes()) && c.processInfoHashLimit.isSaturated() {
				continue
			}

			if !c.processInfoHashLimit.allow() {
				continue
			}

			c.totalProcessedHashes.Inc()

			batchedChannel.In() <- req
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

func (c *crawler) processInfohashes(ctx context.Context, reqs []nodeWithHash) {
	allHashes := make([]protocol.ID, 0, len(reqs))

	reqMap := make(map[protocol.ID]nodeWithHash, len(reqs))
	for _, r := range reqs {
		if _, ok := reqMap[r.infoHash]; ok {
			continue
		}

		allHashes = append(allHashes, r.infoHash)
		reqMap[r.infoHash] = r
	}

	filteredHashes, filterErr := c.blockingManager.Filter(ctx, allHashes)
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

	// Spread out processing each individual infohash to avoid batching network
	// operations.
	interval := time.Duration(int(databaseBatchInterval) / len(filteredHashes))

	for _, h := range filteredHashes {
		r := reqMap[h]
		if t, ok := foundTorrents[r.infoHash]; !ok ||
			t.FilesStatus == model.FilesStatusNoInfo ||
			(t.FilesStatus != model.FilesStatusSingle && !t.FilesCount.Valid) ||
			(t.FilesStatus == model.FilesStatusOverThreshold && t.FilesCount.Uint <= c.saveFilesThreshold) {

			go c.requestMetaInfo(ctx, r)
		} else if (!t.Seeders.Valid || !t.Leechers.Valid) ||
			t.UpdatedAt.Before(time.Now().Add(-c.rescrapeThreshold)) {
			go c.scrape(ctx, r)
		}

		select {
		case <-time.After(interval):
			continue
		case <-ctx.Done():
			return
		}
	}
}

func (c *crawler) requestMetaInfo(ctx context.Context, req nodeWithHash) {
	infohash := req.infoHash
	pendingNodes := []ktable.Node{req.node}

	for len(pendingNodes) > 0 {
		// Try the last currentNode first as it is the most recently obtained "close"
		// node.
		currentNode := pendingNodes[len(pendingNodes)-1]
		pendingNodes = pendingNodes[:len(pendingNodes)-1]

		peersRes, err := c.client.GetPeers(ctx, currentNode.Addr(), req.infoHash)
		if err != nil {
			c.kTable.BatchCommand(ktable.DropAddr{
				Addr:   currentNode.Addr().Addr(),
				Reason: fmt.Errorf("failed to get peers: %w", err),
			})

			return
		}

		c.kTable.BatchCommand(ktable.PutNode{
			ID:      peersRes.ID,
			Addr:    currentNode.Addr(),
			Options: []ktable.NodeOption{ktable.NodeResponded()},
		})

		discoveredNodeDistance := btree.NodeID(infohash.Bytes()).MustXor(btree.NodeID(currentNode.ID().Bytes())).Bits()

		for _, node := range peersRes.Nodes {
			discoveredNode := ktable.NewNode(node.ID, node.Addr)
			c.discoveredNodes <- discoveredNode

			nodeDistance := btree.NodeID(infohash.Bytes()).MustXor(discoveredNode.ID().Bytes()).Bits()

			// Only try nodes that are closer than the current node.
			if discoveredNodeDistance.Cmp(nodeDistance) > 0 {
				pendingNodes = append(pendingNodes, discoveredNode)
			}
		}

		for _, p := range peersRes.Values {
			res, err := c.metainfoRequester.Request(ctx, req.infoHash, p)
			if err != nil {
				continue
			}

			if banErr := c.banningChecker.Check(res.Info); banErr != nil {
				_ = c.blockingManager.Block(ctx, []protocol.ID{req.infoHash}, false)
				return
			}

			c.pendingTorrentPersistsLock.Lock()
			c.pendingTorrentPersists[req.infoHash] = make(chan struct{})
			c.pendingTorrentPersistsLock.Unlock()
			c.torrentsToPersist.In() <- hashWithMetaInfo{infoHash: req.infoHash, metaInfo: res.Info}

			c.scrape(ctx, req)
			return
		}
	}
}

func (c *crawler) scrape(ctx context.Context, req nodeWithHash) {
	res, err := c.client.GetPeersScrape(ctx, req.node.Addr(), req.infoHash)
	if err != nil {
		c.kTable.BatchCommand(ktable.DropAddr{
			Addr:   req.node.Addr().Addr(),
			Reason: fmt.Errorf("failed to get peers from p: %w", err),
		})

		return
	}

	c.kTable.BatchCommand(ktable.PutNode{
		ID:      res.ID,
		Addr:    req.node.Addr(),
		Options: []ktable.NodeOption{ktable.NodeResponded()},
	})

	for _, node := range res.Nodes {
		c.discoveredNodes <- ktable.NewNode(node.ID, node.Addr)
	}

	c.scrapesToPersist.In() <- hashWithScrape{
		infoHash: req.infoHash,
		seeders:  res.BfSeeders.ApproximatedSize(),
		leechers: res.BfPeers.ApproximatedSize(),
	}
}
