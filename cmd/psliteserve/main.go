package main

import (
	"flag"
	"github.com/panyam/pslite/cli"
	"github.com/panyam/pslite/utils"
)

var (
	port        = flag.Int("port", utils.DefaultServerPort(), "Port on which gRPC server should listen TCP conn.")
	topics_root = flag.String("topics_root", "~/.pslite", "Root location where all topics are created")
)

func main() {
	flag.Parse()
	cli.PSLServe(*port, *topics_root)
}
