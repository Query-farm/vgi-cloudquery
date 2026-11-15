// Command vgi-cloudquery-worker serves the cloudquery catalog to DuckDB.
//
//	vgi-cloudquery-worker                                  # stdio (what DuckDB launches)
//	vgi-cloudquery-worker --http --host 0.0.0.0 --port 8000  # HTTP, GET /health
//	vgi-cloudquery-worker --unix /tmp/cloudquery.sock         # AF_UNIX (launch: transport)
//
// Attach it from DuckDB:
//
//	ATTACH 'cloudquery' (TYPE vgi, LOCATION '/abs/path/to/bin/vgi-cloudquery-worker');
package main

import (
	"os"

	"github.com/Query-farm/vgi-go/vgi"

	"github.com/Query-farm/vgi-cloudquery/internal/cloudquery"
	"github.com/Query-farm/vgi-cloudquery/internal/serve"
)

func main() {
	serve.Main("vgi-cloudquery-worker", os.Args[1:], func() *vgi.Worker { return cloudquery.NewWorker() })
}
