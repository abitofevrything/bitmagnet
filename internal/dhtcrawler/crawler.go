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

	processNodeLimit     limiter
	requestMetaInfoLimit limiter

	recentlyProcessedNodes      *boom.StableBloomFilter
	recentlyProcessedInfoHashes *boom.StableBloomFilter

	discoveredNodes      chan ktable.Node
	discoveredInfoHashes chan nodeWithHash
	torrentsToPersist    concurrency.BatchingChannel[hashWithMetaInfo]
	scrapesToPersist     concurrency.BatchingChannel[hashWithScrape]

	soughtNodeID *concurrency.AtomicValue[protocol.ID]

	logger *zap.SugaredLogger

	totalDiscoveredNodes  prometheus.Counter
	totalProcessedNodes   prometheus.Counter
	totalDiscoveredHashes prometheus.Counter
	totalProcessedHashes  *prometheus.CounterVec
	totalPersisted        *prometheus.CounterVec
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
