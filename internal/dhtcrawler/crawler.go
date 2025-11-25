package dhtcrawler

import (
	"context"
	"fmt"
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
	nodeLingerInterval           time.Duration
	hashRotationInterval         time.Duration

	processNodeLimit     limiter
	requestMetaInfoLimit limiter
	scrapeLimit          limiter

	recentlyProcessedNodes      *boom.StableBloomFilter
	recentlyProcessedInfoHashes *boom.StableBloomFilter
	recentlyScheduledInfoHashes *boom.StableBloomFilter

	discoveredNodes             chan ktable.Node
	discoveredInfoHashes        chan nodeWithHash
	infoHashesToScrape          chan protocol.ID
	infoHashesToRequestMetaInfo chan protocol.ID
	torrentsToPersist           concurrency.BatchingChannel[hashWithMetaInfo]
	scrapesToPersist            concurrency.BatchingChannel[hashWithScrape]

	soughtNodeID *concurrency.AtomicValue[protocol.ID]

	logger *zap.SugaredLogger

	totalDiscoveredNodes  prometheus.Counter
	totalProcessedNodes   prometheus.Counter
	totalDiscoveredHashes prometheus.Counter
	totalProcessedHashes  *prometheus.CounterVec
	totalPersisted        *prometheus.CounterVec

	discoveredNodeCount     int
	metaInfosRequestedCount int
	scrapeCount             int
	droppedMetaInfoHashes   int
	droppedScrapeHashes     int
	skippedHashes           int
	successfulScrapes       int
	successfulMetaInfos     int
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
	go c.refreshKTable(ctx)

	go func() {
		processNodeLimits := []int{300, 400, 200}
		requestMetaInfoLimits := []int{500, 450, 550, 400, 600}
		scrapeLimits := []int{1000, 300, 400}
		lingerIntervals := []time.Duration{time.Second * 10, time.Second * 20, time.Second * 30}
		hashRotationIntervals := []time.Duration{time.Second * 10, time.Second * 30, time.Second * 60}

		for {
			for _, lingerInterval := range lingerIntervals {
				for _, processNodeLimit := range processNodeLimits {
					for _, requestMetaInfoLimit := range requestMetaInfoLimits {
						for _, scrapeLimit := range scrapeLimits {
							for _, hashRotationInterval := range hashRotationIntervals {
								c.processNodeLimit.lim.SetLimit(rate.Limit(processNodeLimit))
								c.processNodeLimit.lim.SetBurst(processNodeLimit * 3)

								c.requestMetaInfoLimit.lim.SetLimit(rate.Limit(requestMetaInfoLimit))
								c.requestMetaInfoLimit.lim.SetBurst(requestMetaInfoLimit * 3)

								c.scrapeLimit.lim.SetLimit(rate.Limit(scrapeLimit))
								c.scrapeLimit.lim.SetBurst(scrapeLimit * 3)

								c.nodeLingerInterval = lingerInterval
								c.hashRotationInterval = hashRotationInterval

								fmt.Printf("processNodeLimit=%d, requestMetaInfoLimit=%d, scrapeLimit=%d, lingerInterval=%s, hashRotationInterval=%s\n", processNodeLimit, requestMetaInfoLimit, scrapeLimit, lingerInterval.String(), hashRotationInterval.String())

								<-time.After(time.Minute)

								c.discoveredNodeCount = 0
								c.metaInfosRequestedCount = 0
								c.scrapeCount = 0
								c.droppedMetaInfoHashes = 0
								c.droppedScrapeHashes = 0
								c.skippedHashes = 0
								c.successfulScrapes = 0
								c.successfulMetaInfos = 0

								<-time.After(time.Minute * 5)

								fmt.Printf("discoveredNodeCount=%d, metaInfosRequestedCount=%d, scrapeCount=%d, droppedMetaInfoHashes=%d, droppedScrapeHashes=%d, skippedHashes=%d, successfulScrapes=%d, successfulMetaInfos=%d\n", c.discoveredNodeCount,
									c.metaInfosRequestedCount,
									c.scrapeCount,
									c.droppedMetaInfoHashes,
									c.droppedScrapeHashes,
									c.skippedHashes,
									c.successfulScrapes,
									c.successfulMetaInfos,
								)
							}
						}
					}
				}
			}
		}
	}()
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
		case <-time.After(c.nodeLingerInterval):
		}
	}
}

func (c *crawler) refreshKTable(ctx context.Context) {
	for {
		lastRun := time.Now()

		select {
		case <-ctx.Done():
			return
		case <-time.After(15 * time.Minute):
			oldNodes := c.kTable.GetOldestNodes(lastRun, 10)
			for _, node := range oldNodes {
				// We could just ping the node, but if we're issuing a KRPC
				// call regardless, we might as well run a find_node.
				go c.runFindNode(ctx, node)
			}
		}
	}
}
