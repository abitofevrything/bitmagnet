package dhtcrawlerfx

import (
	"github.com/bitmagnet-io/bitmagnet/internal/config/configfx"
	"github.com/bitmagnet-io/bitmagnet/internal/dhtcrawler"
	"github.com/bitmagnet-io/bitmagnet/internal/dhtcrawler/dhtcrawlerhealthcheck"
	"go.uber.org/fx"
)

func New() fx.Option {
	return fx.Module(
		"dht_crawler",
		configfx.NewConfigModule[dhtcrawler.Config]("dht_crawler", dhtcrawler.NewDefaultConfig()),
		fx.Provide(
			dhtcrawler.New,
			dhtcrawler.NewDiscoveredNodes,
			dhtcrawlerhealthcheck.New,
		),
	)
}
