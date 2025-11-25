package dhtcrawler

import (
	"time"
)

type Config struct {
	ProcessNodeLimit             int
	RequestMetaInfoLimit         int
	ScrapeLimit                  int
	NodeLingerInterval           time.Duration
	HashRotationInterval         time.Duration
	BootstrapNodes               []string
	ReseedBootstrapNodesInterval time.Duration
	// SaveFilesThreshold specifies a maximum number of files in a torrent before file information is discarded.
	// Some torrents contain thousands of files which can severely impact performance and uses a lot of disk space.
	SaveFilesThreshold uint
	// SavePieces when true, torrent pieces will be persisted to the database.
	// The pieces take up quite a lot of space, and aren't currently very useful,
	// but they may be used by future features.
	SavePieces bool
	// RescrapeThreshold is the amount of time that must pass before a torrent is rescraped
	// to count seeders and leechers.
	RescrapeThreshold time.Duration
}

func NewDefaultConfig() Config {
	return Config{
		ProcessNodeLimit:             100,
		RequestMetaInfoLimit:         100,
		ScrapeLimit:                  100,
		NodeLingerInterval:           time.Second * 10,
		HashRotationInterval:         time.Second * 20,
		BootstrapNodes:               defaultBootstrapNodes,
		ReseedBootstrapNodesInterval: time.Minute,
		SaveFilesThreshold:           100,
		SavePieces:                   false,
		RescrapeThreshold:            time.Hour * 24 * 30,
	}
}

// https://github.com/anacrolix/dht/blob/92b36a3fa7a37a15e08684337b47d8d0fb322ab6/dht.go#L106
var defaultBootstrapNodes = []string{
	"router.utorrent.com:6881",
	"router.bittorrent.com:6881",
	"dht.transmissionbt.com:6881",
	"dht.aelitis.com:6881",     // Vuze
	"router.silotis.us:6881",   // IPv6
	"dht.libtorrent.org:25401", // @arvidn's
}
