package dhtcrawler

import (
	"context"
	"net/netip"
	"time"

	"github.com/bitmagnet-io/bitmagnet/internal/concurrency"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol/dht/ktable"
	"go.uber.org/fx"
)

type DiscoveredNodesParams struct {
	fx.In
	Config Config
}

type DiscoveredNodesResult struct {
	fx.Out
	DiscoveredNodes concurrency.BatchingChannel[ktable.Node] `name:"dht_discovered_nodes"`
}

// NewDiscoveredNodes creates the channel for discovered nodes.
// It receives nodes discovered by the crawler, as well as nodes from incoming requests to the DHT server.
// It is provided as a separate service to avoid a circular dependency with the DHT server.
func NewDiscoveredNodes(params DiscoveredNodesParams) DiscoveredNodesResult {
	return DiscoveredNodesResult{
		DiscoveredNodes: concurrency.NewBatchingChannel[ktable.Node](
			int(100*params.Config.ScalingFactor), 10, time.Second/100),
	}
}

func (c *crawler) runDiscoveredNodes(ctx context.Context) {
	counts := make(map[netip.Addr]int)
	total_discovered := 0
	newly_discovered := 0

	for {
		select {
		case <-ctx.Done():
			return
		case ps := <-c.discoveredNodes.Out():
			addrs := make([]netip.Addr, 0, 1)

			m := make(map[string]ktable.Node, 1)
			for _, p := range ps {
				if _, ok := m[p.Addr().Addr().String()]; !ok {
					m[p.Addr().Addr().String()] = p
					addrs = append(addrs, p.Addr().Addr())
				}
			}
			// for any discovered node not already in the routing table,
			// we will block until it can be sent to any one of the pipeline channels.
			unknownAddrs := c.kTable.FilterKnownAddrs(addrs)
			for _, addr := range unknownAddrs {
				existing_count := counts[addr]
				if existing_count == 0 {
					newly_discovered++
				}
				total_discovered++

				existing_count++
				counts[addr] = existing_count

				c.logger.Infof("New node ratio: %d/%d (%.3f)", newly_discovered, total_discovered, float64(newly_discovered)/float64(total_discovered))

				p := m[addr.String()]
				select {
				case <-ctx.Done():
					return
				case c.nodesForFindNode.In() <- p:
				case c.nodesForSampleInfoHashes.In() <- p:
				case c.nodesForPing.In() <- p:
				}
			}
		}
	}
}
