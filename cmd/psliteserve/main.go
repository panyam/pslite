package main

import (
	"flag"
	"github.com/panyam/pslite/cli"
	"github.com/panyam/pslite/config"
)

var (
	port        = flag.Int("port", config.DefaultServerPort(), "Port on which gRPC server should listen TCP conn.")
	topics_root = flag.String("topics_root", "~/.pslite", "Root location where all topics are created")
)

func main() {
	flag.Parse()
	cli.PSLServe(*port, *topics_root)
}
