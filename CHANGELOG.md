# Changelog

## 0.1.3 (2026-11-16)

- CI tooling (haybarn-unittest, lint helpers) runs on Python 3.14.

## 0.1.2 (2026-11-16)

- Bump vgi-go to 0.42.2.

## 0.1.1

First complete release (v0.1.0 shipped macOS and Windows binaries only: the Linux builds
failed a release-workflow glibc check that wrongly rejected statically linked Go binaries).

- Catalog and schema descriptions rewritten for vgi-lint 2.8 (no object lists; distinct comments).
- Fix: a sync stopped early (LIMIT / `max_rows`) no longer leaves the plugin process locked for the next scan.

## 0.1.0

- Host CloudQuery source plugins (plugin protocol v3) from local binaries, GitHub releases,
  the CloudQuery Hub (API key via a `cloudquery_hub` secret) or a running `grpc:` server.
- One schema per plugin, one table per plugin table (child relations included), Arrow
  record batches streamed with CloudQuery extension types mapped to `UUID`/`JSON`/`VARCHAR`.
- Plugin specs from the `spec` ATTACH option and `cloudquery` secrets (deep-merged).
- `main.plugin`, `main.plugin_table`, `main.plugin_column`, `main.scan`, `main.install_plugin`.

Targets: vgi-go 0.42 · VGI protocol 3.3 · Haybarn 2.0.0rc11 · vgi-lint-check 2.x (unpinned) ·
plugin-pb-go 1.27.22 (worker) · plugin-sdk 4.96.4 (fixture plugin)
