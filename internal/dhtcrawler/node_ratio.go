package dhtcrawler

type nodeRatio struct {
	recentSampleSize uint64
	recentNodes      uint64
	recentHashes     uint64

	sampleSize uint64
	nodes      uint64
	hashes     uint64
}

func newNodeRatio(recentSampleSize int, sampleSize int) *nodeRatio {
	return &nodeRatio{
		recentSampleSize: uint64(recentSampleSize),
		sampleSize:       uint64(sampleSize),
	}
}

func (n *nodeRatio) addNode() {
	n.nodes++
	n.recentNodes++

	if n.nodes > n.sampleSize {
		n.nodes /= 2
		n.hashes /= 2
	}

	if n.recentNodes > n.recentSampleSize {
		n.recentNodes /= 2
		n.recentHashes /= 2
	}
}

func (n *nodeRatio) addHashes(hashes int) {
	n.hashes += uint64(hashes)
	n.recentHashes += uint64(hashes)
}

func (n *nodeRatio) recentRatio() float64 {
	return float64(n.recentHashes) / float64(n.recentNodes)
}

func (n *nodeRatio) ratio() float64 {
	return float64(n.hashes) / float64(n.nodes)
}
