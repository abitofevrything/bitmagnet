package dhtcrawler

import (
	"context"
	"fmt"
	"time"

	"github.com/bitmagnet-io/bitmagnet/internal/protocol/dht"
	"github.com/bitmagnet-io/bitmagnet/internal/protocol/dht/ktable"
)

func (c *crawler) handleDiscoveredNodes(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case node := <-c.discoveredNodes:
			c.totalDiscoveredNodes.Inc()

			if c.recentlyProcessedNodes.TestAndAdd(node.ID().Bytes()) && c.processNodeLimit.isSaturated() {
				continue
			}

			if !c.processNodeLimit.allow() {
				continue
			}

			c.totalProcessedNodes.Inc()

			go c.processNode(ctx, node)
		}
	}
}

func (c *crawler) processNode(ctx context.Context, node ktable.Node) {
	if !node.IsSampleInfoHashesCandidate() {
		return
	}

	res, err := c.client.SampleInfoHashes(ctx, node.Addr(), c.soughtNodeID.Get())
	if err != nil {
		// Only attempt to find_node if the node did respond and we are short of nodes to process.
		if err, ok := err.(dht.Error); ok && !c.processNodeLimit.isSaturated() && err.Code == dht.ErrorCodeMethodUnknown {
			c.runFindNode(ctx, node)
		} else {
			c.kTable.BatchCommand(ktable.DropNode{
				ID:     node.ID(),
				Reason: fmt.Errorf("sample_infohashes failed: %w", err),
			})
		}

		return
	}

	var discoveredHashes []nodeWithHash

	for _, s := range res.Samples {
		discoveredHashes = append(discoveredHashes, nodeWithHash{
			node:     node,
			infoHash: s,
		})
	}

	interval := res.Interval
	// most nodes request a 6 hour backoff time(!)
	// if we're still discovering info hashes from them then let's set a respectful interval instead
	if len(discoveredHashes) > 0 && interval > 300 {
		interval = 60
	}

	c.kTable.BatchCommand(ktable.PutNode{ID: node.ID(), Addr: node.Addr(), Options: []ktable.NodeOption{
		ktable.NodeResponded(),
		ktable.NodeBep51Support(true),
		ktable.NodeSampleInfoHashesRes(
			len(discoveredHashes),
			res.Num,
			time.Now().Add(time.Duration(interval)*time.Second),
		),
	}})

	for _, info := range discoveredHashes {
		c.discoveredInfoHashes <- info
	}

	for _, node := range res.Nodes {
		c.discoveredNodes <- ktable.NewNode(node.ID, node.Addr)
	}
}

func (c *crawler) runFindNode(ctx context.Context, node ktable.Node) {
	res, err := c.client.FindNode(ctx, node.Addr(), c.soughtNodeID.Get())
	if err != nil {
		c.kTable.BatchCommand(ktable.DropNode{
			ID:     node.ID(),
			Reason: fmt.Errorf("find_node failed: %w", err),
		})

		return
	}

	c.kTable.BatchCommand(ktable.PutNode{
		ID:      node.ID(),
		Addr:    node.Addr(),
		Options: []ktable.NodeOption{ktable.NodeResponded()},
	})

	for _, n := range res.Nodes {
		c.discoveredNodes <- ktable.NewNode(n.ID, n.Addr)
	}
}
