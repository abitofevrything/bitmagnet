package dhtcrawler

import (
	"context"
	"time"

	"github.com/bitmagnet-io/bitmagnet/internal/blocking"
	"github.com/bitmagnet-io/bitmagnet/internal/concurrency"
	"github.com/bitmagnet-io/bitmagnet/internal/database/dao"
	"github.com/bitmagnet-io/bitmagnet/internal/lazy"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol/dht/client"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol/dht/ktable"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol/metainfo/banning"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol/metainfo/metainforequester"
	"github.com/bitmagnet-io/bitmagnet/internal/worker"
	"github.com/prometheus/client_golang/prometheus"
	boom "github.com/tylertreat/BoomFilters"
	"go.uber.org/fx"
	"go.uber.org/zap"
)

type Params struct {
	fx.In
	Config            Config
	KTable            ktable.Table
	Client            lazy.Lazy[client.Client]
	MetainfoRequester metainforequester.Requester
	BanningChecker    banning.Checker `name:"metainfo_banning_checker"`
	Dao               lazy.Lazy[*dao.Query]
	BlockingManager   lazy.Lazy[blocking.Manager]
	Logger            *zap.SugaredLogger
}

type Result struct {
	fx.Out
	Worker worker.Worker `group:"workers"`

	DhtCrawlerActive *concurrency.AtomicValue[bool] `name:"dht_crawler_active"`

	TotalDiscoveredNodes  prometheus.Collector `group:"prometheus_collectors"`
	TotalProcessedNodes   prometheus.Collector `group:"prometheus_collectors"`
	TotalDiscoveredHashes prometheus.Collector `group:"prometheus_collectors"`
	TotalProcessedHashes  prometheus.Collector `group:"prometheus_collectors"`
	TotalPersisted        prometheus.Collector `group:"prometheus_collectors"`
}

const (
	databaseBatchSize     = 1000
	databaseBatchInterval = 20 * time.Second
	namespace             = "bitmagnet"
	subsystem             = "dht_crawler"
)

func New(params Params) Result {
	var cancel func()
	active := &concurrency.AtomicValue[bool]{}

	totalDiscoveredNodes := prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: namespace,
		Subsystem: subsystem,
		Name:      "discovered_nodes_total",
		Help:      "The total number of nodes discovered by the crawler.",
	})

	totalProcessedNodes := prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: namespace,
		Subsystem: subsystem,
		Name:      "processed_nodes_total",
		Help:      "The total number of nodes processed by the crawler.",
	})

	totalDiscoveredHashes := prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: namespace,
		Subsystem: subsystem,
		Name:      "discovered_hashes_total",
		Help:      "The total number of infohashes discovered by the crawler.",
	})

	totalProcessedHashes := prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Subsystem: subsystem,
		Name:      "processed_hashes_total",
		Help:      "The total number of infohashes processed by the crawler.",
	}, []string{"result"})

	totalPersisted := prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Subsystem: subsystem,
		Name:      "persisted_total",
		Help:      "A counter of persisted database entities.",
	}, []string{"entity"})

	return Result{
		DhtCrawlerActive:      active,
		TotalDiscoveredNodes:  totalDiscoveredNodes,
		TotalProcessedNodes:   totalProcessedNodes,
		TotalDiscoveredHashes: totalDiscoveredHashes,
		TotalProcessedHashes:  totalProcessedHashes,
		TotalPersisted:        totalPersisted,
		Worker: worker.NewWorker(
			"dht_crawler",
			fx.Hook{
				OnStart: func(ctx context.Context) error {
					active.Set(true)

					client, err := params.Client.Get()
					if err != nil {
						return err
					}
					dao, err := params.Dao.Get()
					if err != nil {
						return err
					}
					blockingManager, err := params.BlockingManager.Get()
					if err != nil {
						return err
					}

					ctx, cancel = context.WithCancel(ctx)

					c := crawler{
						kTable:            params.KTable,
						client:            client,
						metainfoRequester: params.MetainfoRequester,
						banningChecker:    params.BanningChecker,
						dao:               dao,
						blockingManager:   blockingManager,

						bootstrapNodes:               params.Config.BootstrapNodes,
						rescrapeThreshold:            params.Config.RescrapeThreshold,
						reseedBootstrapNodesInterval: params.Config.ReseedBootstrapNodesInterval,
						saveFilesThreshold:           params.Config.SaveFilesThreshold,
						savePieces:                   params.Config.SavePieces,

						processNodeLimit:     newLimiter(params.Config.ProcessNodeLimit),
						requestMetaInfoLimit: newLimiter(params.Config.RequestMetaInfoLimit),

						recentlyProcessedNodes:      boom.NewStableBloomFilter(10_000_000, 2, 0.001),
						recentlyProcessedInfoHashes: boom.NewStableBloomFilter(10_000_000, 2, 0.001),

						discoveredNodes:      make(chan ktable.Node),
						discoveredInfoHashes: make(chan nodeWithHash),
						torrentsToPersist:    concurrency.NewBatchingChannel[hashWithMetaInfo](100, databaseBatchSize, databaseBatchInterval),
						scrapesToPersist:     concurrency.NewBatchingChannel[hashWithScrape](100, databaseBatchSize, databaseBatchInterval),

						soughtNodeID: &concurrency.AtomicValue[protocol.ID]{},

						logger: params.Logger,

						totalDiscoveredNodes:  totalDiscoveredNodes,
						totalProcessedNodes:   totalProcessedNodes,
						totalDiscoveredHashes: totalDiscoveredHashes,
						totalProcessedHashes:  totalProcessedHashes,
						totalPersisted:        totalPersisted,
					}

					go c.start(ctx)
					return nil
				},
				OnStop: func(context.Context) error {
					active.Set(false)
					cancel()
					return nil
				},
			},
		),
	}
}

type DiscoveredNodesParams struct {
	fx.In
	Config Config
}

type DiscoveredNodesResult struct {
	fx.Out
	DiscoveredNodes chan ktable.Node `name:"dht_discovered_nodes"`
}

func NewDiscoveredNodes(params DiscoveredNodesParams) DiscoveredNodesResult {
	return DiscoveredNodesResult{
		DiscoveredNodes: make(chan ktable.Node, 100),
	}
}
