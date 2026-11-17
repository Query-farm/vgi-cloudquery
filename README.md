<p align="center">
  <img src="https://query.farm/media-kit/products/vgi/vgi-logo.png" alt="Vector Gateway Interface (VGI)" width="320">
</p>

<p align="center"><em>A <a href="https://query.farm">Query.Farm</a> VGI worker for DuckDB.</em></p>

# Query Every CloudQuery Source Plugin's Tables in DuckDB

> **vgi-cloudquery** · a [Query.Farm](https://query.farm) VGI worker · powered by [plugin-pb-go](https://github.com/cloudquery/plugin-pb-go)

[![CI](https://github.com/Query-farm/vgi-cloudquery/actions/workflows/ci.yml/badge.svg)](https://github.com/Query-farm/vgi-cloudquery/actions/workflows/ci.yml)
[![vgi worker: query.farm](https://query.farm/media-kit/shields/vgi-worker.svg)](https://query.farm)

[CloudQuery](https://www.cloudquery.io) source plugins turn cloud and SaaS APIs (AWS,
GCP, GitHub, Kubernetes, Hacker News, ...) into tables, normally synced into a
destination database by the CloudQuery CLI. This worker hosts those plugins
**directly**: it starts each plugin binary, speaks the plugin protocol v3 (gRPC +
Apache Arrow) itself, and streams the plugin's record batches into DuckDB. There is no
CloudQuery CLI and no destination database.

```sql
ATTACH 'cloudquery' AS cq (TYPE vgi, LOCATION 'github-auto://Query-farm/vgi-cloudquery@v0.1.4',
    plugins 'github:cloudquery/test@v4.1.0, crowdstrike=github:justmiles/crowdstrike@v2.1.0',
    spec    '{"test": {"num_rows": 3}}');

SELECT p.resource_id, count(*) AS subs
FROM cq.test.test_some_table p JOIN cq.test.test_sub_table s ON s._cq_parent_id = p._cq_id
GROUP BY ALL;
```

- **Each plugin is a schema, each plugin table a table**, child relations included.
- **Arrow end to end**: the plugin's record batches reach DuckDB with their buffers
  intact; CloudQuery's `uuid` and `json` columns arrive as `UUID` and `JSON`.
- **Plugins come from** a local binary, a GitHub release, the CloudQuery Hub (API key
  required, see below) or an already-running plugin server. Downloads are sha256-verified
  and cached.
- **Specs with credentials live in DuckDB secrets** (`TYPE cloudquery`), never in the
  ATTACH string.

## Quick start

Download the release archive for your platform from the
[releases page](https://github.com/Query-farm/vgi-cloudquery/releases), or let the VGI
extension fetch it with `github-auto://` (change `@v0.1.4` to the release you want):

```sql
INSTALL vgi FROM community;
LOAD vgi;

ATTACH 'cloudquery' AS cq (TYPE vgi, LOCATION 'github-auto://Query-farm/vgi-cloudquery@v0.1.4',
    plugins 'github:cloudquery/test@v4.1.0');

FROM cq.main.plugin;          -- what was attached
FROM cq.main.plugin_table;    -- every table, its parent relation and primary key
FROM cq.main.plugin_column;   -- every column, its CloudQuery and DuckDB types
FROM cq.test.test_some_table;
```

Other ways to run it: point `LOCATION` at the extracted binary
(`/opt/vgi-cloudquery-worker`), run it as an HTTP service (`vgi-cloudquery-worker --http
--port 8000`, then `LOCATION 'http://host:8000'`), or build from source (`make build`,
Go 1.26+, pure Go).

## Which plugins can you use?

Checked on 2026-11-15 against the public Hub API and GitHub:

| Source | Anonymous download | How to use it here |
|---|---|---|
| **CloudQuery Hub** (`api.cloudquery.io`): 162 source plugins, 119 paid (`aws`, `gcp`, `azure`, `github`, `k8s`, `postgresql`, `file`, ...) and 16 free (official: `airtable`, `bitly`, `hackernews`, `square`, `typeform`, `xkcd`; 10 community) | **No.** The asset endpoint answers 401 to anonymous callers for every plugin, free ones included. | Create a CloudQuery account and API key yourself, `CREATE SECRET (TYPE cloudquery_hub, API_KEY '...')`, then `SELECT * FROM cq.main.install_plugin('cloudquery/xkcd@v1.6.1')`. Paid plugins and paid tables also need the key at run time (they report usage) and a plan that covers them. |
| **CloudQuery monorepo GitHub releases** (`plugins-source-<name>-<version>`) | Yes, but frozen: binaries were attached until early 2024 (e.g. `test` v4.1.0, `hackernews` v3.0.23, `aws` v22.19.2, `gcp` v10.0.0, `k8s` v6.0.4, `github` v8.0.0) | `github:cloudquery/<name>@<version>`. Old official plugins may now require a login of their own; `test` v4.1.0 works offline. |
| **Community GitHub releases** (`<team>/cq-source-<name>`) | Yes | `github:<team>/<name>@<version>`, e.g. `github:justmiles/crowdstrike@v2.1.0`, `github:hermanschaaf/chess-com@v1.0.1`. Plugins built with plugin-sdk v1-v3 speak older protocols and are refused with a clear error (`coinpaprika` v1.0.1, `onfleet` v1.0.0). |
| **Source code** | The cloudquery monorepo (MPL-2.0) still contains `airtable`, `bitbucket`, `hackernews`, `square`, `test`, `typeform` and `xkcd` | Build the plugin yourself and use its local path. |
| **Docker-packaged plugins** (e.g. Hub `typeform`) | n/a | Not started by this worker. Run the container yourself (`docker run -p 7777:7777 <image> serve --address 0.0.0.0:7777`) and attach `grpc:127.0.0.1:7777`. |

The worker never uses a `cloudquery login` session: plugins run with a private
`XDG_DATA_HOME` and without any `CLOUDQUERY_API_KEY` from the environment, so the only
CloudQuery key a plugin ever sees is the one in a `cloudquery_hub` secret.

## ATTACH options

| Option | Meaning |
|---|---|
| `plugins` | Comma-separated `[schema=]ref` entries. Each plugin becomes a schema, named after the plugin unless `schema=` is given. |
| `spec` | Non-secret plugin specs: a JSON or YAML object keyed by schema, each value the plugin's own `spec:` block (`{"hackernews": {"item_concurrency": 10}}`). Used to discover tables at ATTACH and for every query. |
| `cache_dir` | Where downloaded plugins are kept (default `<user cache dir>/vgi-cloudquery/plugins`). |
| `hub_url` | CloudQuery Hub API base URL (default `https://api.cloudquery.io`), for mirrors and tests. |
| `github_url` | Base URL for GitHub release downloads (default `https://github.com`), for mirrors and tests. |

Plugin refs:

| Ref | Means |
|---|---|
| `/opt/cq-source-xkcd` | a local binary (also `./`, `../`, `~/`) |
| `cloudquery/xkcd@v1.6.1` | CloudQuery Hub (the CLI's default `cloudquery` registry) |
| `hub:cloudquery/xkcd` | Hub, latest version (resolved through the public plugin endpoint) |
| `github:cloudquery/test@v4.1.0` | a GitHub release asset (the CLI's `github` registry) |
| `...#sha256:<hex>` | pins the archive's sha256 (required when a release publishes no checksum) |
| `grpc:127.0.0.1:7777` | a plugin server that is already running |

A worker can also be started with defaults for ATTACHes that name no plugins, which suits
a hosted HTTP worker that serves a fixed set: `vgi-cloudquery-worker --http --plugins
'github:cloudquery/test@v4.1.0' --plugin-spec '{"test": {"num_rows": 5}}'` (also
`--cache-dir`, `--hub-url`, `--github-url`).

## Credentials

Put a plugin's credentials in a DuckDB secret. Its `SPEC` is the JSON or YAML of the
plugin's `spec:` block, deep-merged over the ATTACH `spec` at query time (a key set in
both is taken from the secret):

```sql
CREATE SECRET crowdstrike (TYPE cloudquery, PLUGIN 'crowdstrike',
    SPEC '{"client_id": "...", "client_secret": "..."}', SCOPE 'cloudquery://crowdstrike');
SELECT hostname, platform_name FROM cq.crowdstrike.crowdstrike_falcon_hosts LIMIT 10;
```

- The worker looks the secret up in scope `cloudquery://<schema>`; give each plugin's
  secret that scope when you have more than one.
- `PLUGIN` names the schema or plugin the secret configures; a secret naming another
  plugin is ignored, so credentials never reach the wrong plugin.
- `SPEC` is redacted in `duckdb_secrets()` and never stored in scan state or logs.
- Cloud SDK credential chains (`~/.aws`, `AWS_PROFILE`, `GOOGLE_APPLICATION_CREDENTIALS`,
  `~/.kube/config`) work as they do under the CloudQuery CLI: plugins inherit the
  worker's environment.

`CREATE SECRET (TYPE cloudquery_hub, API_KEY '...', TEAM '...')` holds a CloudQuery API
key for `install_plugin` and for paid plugins.

## How queries run

- **One sync per query.** Each table scan runs a plugin `Sync` of just that table
  (`skip_dependent_tables`), so a query costs the same API calls as a CloudQuery sync of
  the table. Reading a child table makes the plugin walk its parent too; the parent's
  rows are dropped.
- **Pushdown.** Table selection and projection: only the selected columns are sent to
  DuckDB. CloudQuery plugins accept **no per-query filters**, so every `WHERE` clause is
  applied by DuckDB after the rows arrive (exactly, for every type and session time
  zone, because the worker declares no filter pushdown).
- **Limit.** DuckDB stops reading once a `LIMIT` is met and the worker cancels the
  plugin's sync. `cloudquery.main.scan(schema, table_name, max_rows := N)` stops the sync
  after N rows of the table.
- **Relations.** Child tables join their parent with `child._cq_parent_id =
  parent._cq_id`. The worker asks for deterministic cq ids, so separate syncs of a parent
  and a child line up whenever the parent has a primary key.
- **Sync messages.** `insert` batches are projected and streamed; `migrate_table` and
  `delete_record` (destination concerns) are skipped. A plugin's per-table `error` is
  logged and the rows fetched so far are returned, as the CloudQuery CLI does;
  `strict := true` makes it fail the query.
- **Streaming.** Record batches are coalesced into batches of up to 2048 rows, flushed
  after 250 ms, so a slow API still shows its first rows promptly.
- **Processes.** Plugins are pooled per (plugin, schema, spec); a plugin serves one sync
  at a time, so concurrent scans start more processes (at most 16). Idle plugins stop
  after 10 minutes. Each plugin runs under a small supervisor that kills it when the
  worker exits, however the worker ends. Plugin logs go to the worker's stderr.

Type mapping:

| CloudQuery (Arrow) | DuckDB |
|---|---|
| `uuid` extension | `UUID` (same buffers) |
| `json` extension | `JSON` (same buffers) |
| `inet` extension | `VARCHAR`, e.g. `10.0.0.1/32` (same buffers) |
| `mac` extension | `VARCHAR`, e.g. `02:00:00:00:00:01` |
| `timestamp[*, UTC]` | `TIMESTAMP WITH TIME ZONE` (`TIMESTAMPTZ_NS` for ns) |
| lists, structs, maps | `LIST`, `STRUCT`, `MAP`, element types mapped the same way |
| everything else | the Arrow type's DuckDB equivalent (`int64` to `BIGINT`, `decimal128` to `DECIMAL`, ...) |

The published `cloudquery/test` plugin's `test_testdata_table` (79 columns, one of every
Arrow type CloudQuery uses) reads cleanly.

## Functions and tables

| Object | What it is |
|---|---|
| `<schema>.<table>` | One per plugin table. |
| `main.plugin` | The attached plugins: ref, registry, version, verified digests, table count, spec JSON Schema. |
| `main.plugin_table` | Every plugin table: parent relation, child tables, primary key columns, incremental/paid flags. |
| `main.plugin_column` | Every column: CloudQuery type, DuckDB type, nullability, key flags. |
| `main.scan(schema, table_name, max_rows := 0, strict := false)` | Reads one plugin table directly. Use it for plugins whose tables depend on credentials only a secret holds, since catalog tables are discovered with the ATTACH spec. |
| `main.install_plugin(ref)` | Downloads a Hub or GitHub plugin into the verified cache. |

## Plugin downloads

1. **GitHub**: `checksums.txt` is fetched from the release and the archive's sha256 must
   match its entry (or the ref's `#sha256:` pin); a release with no checksum is refused
   unless pinned.
2. **Hub**: the asset endpoint returns a download location and the archive's sha256; the
   archive must match it. The download URL's signed query string is never logged or
   recorded.
3. The plugin binary is unpacked only after verification, and its own sha256 is recorded
   and re-checked whenever the cache is reused, so a cached plugin that changed on disk is
   downloaded again rather than run.

A ref with a version that is already cached never touches the network.

## Known limitations

- **No filter pushdown exists** in the CloudQuery plugin protocol: a query reads the
  whole table (and, for a child table, its parents) from the API.
- **Incremental tables** are always read in full: the worker passes no state backend.
- **Dynamic-schema plugins** (for example `postgresql`, `sharepoint`) report their tables
  only once connected. Catalog tables are discovered with the ATTACH spec and no
  connection; read the others with `scan()`, which connects with the secret applied.
- **HTTP scale-out**: a scan resumed on another worker process re-syncs and skips the rows
  already returned. Plugins sync concurrently, so row order can differ between syncs; use
  sticky routing when running several HTTP worker processes.
- Plugins built with plugin-sdk v1-v3 (protocols v1/v2) are not supported.

## Testing

`make check` runs gofmt/vet/golangci-lint, the Go unit tests, the SQL tests over stdio,
launch and HTTP, and the metadata lint. The SQL tests use:

- **`test/fixture-plugin/`**: a small source plugin built with the real CloudQuery
  plugin-sdk v4 (its own Go module). It has a parent table with a child relation, every
  extension type (`uuid`, `json`, `inet`, `mac`, `list<uuid>`), NULLs, a table whose
  resolver fails partway, and a spec echo for the secret tests, so the suite needs no
  network or CloudQuery account.
- **`test/mock_registry/server.py`**: a mock CloudQuery Hub API (401 without the test key,
  as the real Hub) and GitHub release host serving that plugin as archives, including a
  tampered archive and a release without checksums.
- **`test/sql/live_test_plugin.test`**: the published `cloudquery/test` v4.1.0 from its
  GitHub release. It is cached under `.tools/plugin-cache` and skipped when GitHub is
  unreachable and nothing is cached.

## Support

Open an issue on this repository. For VGI itself, see [query.farm](https://query.farm).

## Authorship & License

Written by [Query.Farm](https://query.farm). Released under the
[BSD Zero Clause License](LICENSE) (0BSD).

Copyright 2026 Query Farm LLC - https://query.farm

The worker links [plugin-pb-go](https://github.com/cloudquery/plugin-pb-go) (MPL-2.0)
for the plugin protocol; the test fixture plugin uses
[plugin-sdk](https://github.com/cloudquery/plugin-sdk) (MPL-2.0). CloudQuery and its
plugins are products of CloudQuery, Inc.: each plugin you fetch is distributed under its
own license and terms, and Hub plugins under your CloudQuery account's terms.
