// Command vgi-cloudquery-worker serves the cloudquery catalog to DuckDB.
//
//	vgi-cloudquery-worker                                    # stdio (what DuckDB launches)
//	vgi-cloudquery-worker --http --host 0.0.0.0 --port 8000  # HTTP, GET /health
//	vgi-cloudquery-worker --unix /tmp/cloudquery.sock        # AF_UNIX (launch: transport)
//
// Attach it from DuckDB:
//
//	ATTACH 'cloudquery' (TYPE vgi, LOCATION '/abs/path/to/bin/vgi-cloudquery-worker',
//	    plugins 'github:cloudquery/test@v4.1.0');
//
// Server-side defaults for ATTACHes that do not set the option (useful for an
// HTTP deployment that serves a fixed plugin set):
//
//	--plugins 'github:cloudquery/test@v4.1.0'  --plugin-spec '{"test": {"num_rows": 5}}'
//	--cache-dir DIR  --hub-url URL  --github-url URL
package main

import (
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/Query-farm/vgi-go/vgi"

	"github.com/Query-farm/vgi-cloudquery/internal/cloudquery"
	"github.com/Query-farm/vgi-cloudquery/internal/serve"
)

func main() {
	if len(os.Args) > 1 && os.Args[1] == cloudquery.SuperviseArg {
		os.Exit(cloudquery.Supervise(os.Args[2:]))
	}
	if exe, err := os.Executable(); err == nil {
		cloudquery.EnableSupervisor(exe)
	}
	// Plugin processes are children of the worker: stop them when it exits.
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sig
		cloudquery.StopPlugins()
		os.Exit(1)
	}()
	defer cloudquery.StopPlugins()
	cloudquery.SetAttachDefaults(pluginFlags(os.Args[1:]))
	serve.Main("vgi-cloudquery-worker", os.Args[1:], func() *vgi.Worker { return cloudquery.NewWorker() })
}

// pluginFlags reads the worker's own flags (--plugins, --plugin-spec,
// --cache-dir, --hub-url, --github-url, as --flag value or --flag=value);
// serve.Main ignores them.
func pluginFlags(args []string) map[string]string {
	names := map[string]string{"plugins": "plugins", "plugin-spec": "spec", "cache-dir": "cache_dir", "hub-url": "hub_url", "github-url": "github_url"}
	out := map[string]string{}
	for i := 0; i < len(args); i++ {
		name, value, inline := strings.Cut(strings.TrimLeft(args[i], "-"), "=")
		key, ok := names[name]
		if !ok || !strings.HasPrefix(args[i], "-") {
			continue
		}
		if !inline && i+1 < len(args) {
			i++
			value = args[i]
		}
		out[key] = value
	}
	return out
}
