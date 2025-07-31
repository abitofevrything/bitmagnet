package dhtcrawler

import (
	"context"
	"net"
	"time"

	"github.com/bitmagnet-io/bitmagnet/internal/blocking"
	"github.com/bitmagnet-io/bitmagnet/internal/concurrency"
	"github.com/bitmagnet-io/bitmagnet/internal/database/dao"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol/dht/client"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol/dht/ktable"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol/metainfo"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol/metainfo/banning"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol/metainfo/metainforequester"
	"github.com/prometheus/client_golang/prometheus"
	boom "github.com/tylertreat/BoomFilters"
	"go.uber.org/zap"
	"golang.org/x/time/rate"
)

type crawler struct {
	kTable            ktable.Table
	client            client.Client
	metainfoRequester metainforequester.Requester
	banningChecker    banning.Checker
	dao               *dao.Query
	blockingManager   blocking.Manager

	bootstrapNodes               []string
	rescrapeThreshold            time.Duration
	reseedBootstrapNodesInterval time.Duration
	saveFilesThreshold           uint
	savePieces                   bool
	maxProcessInfoHashRate       int

	processNodeLimit     limiter
	processInfoHashLimit limiter

	recentlyProcessedNodes      *boom.StableBloomFilter
	recentlyProcessedInfoHashes *boom.StableBloomFilter

	discoveredNodes      chan ktable.Node
	discoveredInfoHashes chan nodeWithHash
	torrentsToPersist    concurrency.BatchingChannel[hashWithMetaInfo]
	scrapesToPersist     concurrency.BatchingChannel[hashWithScrape]

	nodeRatio *nodeRatio

	soughtNodeID *concurrency.AtomicValue[protocol.ID]

	logger *zap.SugaredLogger

	totalDiscoveredNodes     prometheus.Counter
	totalProcessedNodes      prometheus.Counter
	totalDiscoveredHashes    prometheus.Counter
	totalProcessedHashes     *prometheus.CounterVec
	totalPersisted           *prometheus.CounterVec
	processNodeRate          prometheus.Gauge
	processHashRate          prometheus.Gauge
	recentNodeRatioCollector prometheus.Gauge
	nodeRatioCollector       prometheus.Gauge
}

type nodeWithHash struct {
	node     ktable.Node
	infoHash protocol.ID
}

type hashWithMetaInfo struct {
	infoHash protocol.ID
	metaInfo metainfo.Info
	scrape   *hashWithScrape
}

type hashWithScrape struct {
	infoHash protocol.ID
	seeders  uint32
	leechers uint32
}

func (c *crawler) start(ctx context.Context) {
	go c.handleDiscoveredNodes(ctx)
	go c.handleDiscoveredInfohashes(ctx)
	go c.rotateSoughtNodeID(ctx)
	go c.reseedBootstrapNodes(ctx)
	go c.runPersistTorrents(ctx)
	go c.runPersistScrapes(ctx)
	go c.adjustNodeLimit(ctx)
	go c.adjustInfoHashLimit(ctx)
}

func (c *crawler) reseedBootstrapNodes(ctx context.Context) {
	for {
		for _, node := range c.bootstrapNodes {
			addr, err := net.ResolveUDPAddr("udp", node)
			if err != nil {
				c.logger.Warnf("failed to resolve bootstrap node address: %s", err)
				continue
			}

			if !c.processNodeLimit.isSaturated() {
				// Don't discover the bootstrap nodes seeing as most of them do
				// not like sample_infohashes requests.
				c.runFindNode(ctx, ktable.NewNode(ktable.ID{}, addr.AddrPort()))
			}
		}

		select {
		case <-ctx.Done():
			return
		case <-time.After(c.reseedBootstrapNodesInterval):
			continue
		}
	}
}

func (c *crawler) rotateSoughtNodeID(ctx context.Context) {
	for {
		c.soughtNodeID.Set(protocol.RandomNodeID())

		select {
		case <-ctx.Done():
			return
		case <-time.After(10 * time.Second):
		}
	}
}

func (c *crawler) adjustNodeLimit(ctx context.Context) {
	for {
		c.processNodeRate.Set(float64(c.processNodeLimit.limit()))
		c.recentNodeRatioCollector.Set(c.nodeRatio.recentRatio())
		c.nodeRatioCollector.Set(c.nodeRatio.ratio())

		select {
		case <-ctx.Done():
			return
		case <-time.After(time.Second * 10):
			currentLimit := c.processNodeLimit.limit()
			newLimit := currentLimit

			if c.nodeRatio.recentRatio() < c.nodeRatio.ratio()*0.75 {
				// Most likely network overload causing increased failure rates
				// in processNode. Scale down.
				newLimit *= 0.9
			} else if c.processInfoHashLimit.isOverloaded() {
				newLimit *= 0.99
			} else if !c.processInfoHashLimit.isSaturated() {
				newLimit *= 1.1
			}

			newLimit = max(newLimit, rate.Limit(10))
			// Each node should provide on average more than one infohash.
			// If we try to scale past the infohash processing limit, something
			// is going wrong (e.g no internet causing 100% failure rate on
			// processNode).
			// Add this limit to prevent the process node limit from scaling to
			// infinity.
			newLimit = min(newLimit, c.processInfoHashLimit.limit())

			if newLimit != currentLimit {
				c.processNodeLimit.setLimit(newLimit)
			}
		}
	}
}

func (c *crawler) adjustInfoHashLimit(ctx context.Context) {
	for {
		c.processHashRate.Set(float64(c.processInfoHashLimit.limit()))

		select {
		case <-ctx.Done():
			return
		case <-time.After(time.Minute):
			currentLimit := c.processInfoHashLimit.limit()
			newLimit := currentLimit

			if !c.processInfoHashLimit.isSaturated() {
				newLimit *= 0.99
			} else {
				newLimit *= 1.01
			}

			newLimit = max(newLimit, rate.Limit(10))
			if c.maxProcessInfoHashRate > 0 {
				newLimit = min(newLimit, rate.Limit(c.maxProcessInfoHashRate))
			}

			if newLimit != currentLimit {
				c.processInfoHashLimit.setLimit(newLimit)
			}
		}
	}
}
