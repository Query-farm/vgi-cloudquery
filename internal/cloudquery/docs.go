// Discovery metadata: catalog, schema, secret types and the main schema's
// functions and tables.

package cloudquery

import (
	"github.com/Query-farm/vgi-go/vgi"
	"github.com/apache/arrow-go/v18/arrow"
)

// SecretType is the DuckDB secret type holding a plugin's spec (credentials).
const SecretType = "cloudquery"

// HubSecretType is the DuckDB secret type holding a CloudQuery API key.
const HubSecretType = "cloudquery_hub"

var secretSchemaMetadata = arrow.NewMetadata([]string{"vgi_doc_url"}, []string{SourceURL + "#credentials"})

// PluginSecretType is registered with DuckDB so users can run
// CREATE SECRET (TYPE cloudquery, PLUGIN 'github', SPEC '{"access_token": "..."}').
var PluginSecretType = vgi.SecretTypeSpec{
	Name: SecretType,
	Description: "Plugin spec for a hosted CloudQuery source plugin, typically its credentials: the JSON or YAML of the plugin's " +
		"`spec:` block. Deep-merged over the ATTACH spec option at query time.",
	Schema: arrow.NewSchema([]arrow.Field{
		{Name: "plugin", Type: arrow.BinaryTypes.String, Nullable: true,
			Metadata: arrow.NewMetadata([]string{"vgi_doc"},
				[]string{"Schema or plugin name the secret configures (e.g. 'github'); the secret is ignored for other plugins. Empty applies to any."})},
		{Name: "spec", Type: arrow.BinaryTypes.String, Nullable: true,
			Metadata: arrow.NewMetadata([]string{"redact", "vgi_doc"},
				[]string{"true", "The plugin spec as a JSON object ({\"access_token\": \"...\"}) or YAML (access_token: ...)."})},
	}, &secretSchemaMetadata),
}

// HubSecretSpec is the cloudquery_hub secret type: CREATE SECRET (TYPE cloudquery_hub, API_KEY '...').
var HubSecretSpec = vgi.SecretTypeSpec{
	Name: HubSecretType,
	Description: "CloudQuery Hub API key (the CLOUDQUERY_API_KEY equivalent): lets install_plugin download Hub plugins, which the Hub " +
		"refuses to anonymous callers, and is passed to paid plugins that report usage.",
	Schema: arrow.NewSchema([]arrow.Field{
		{Name: "api_key", Type: arrow.BinaryTypes.String, Nullable: true,
			Metadata: arrow.NewMetadata([]string{"redact", "vgi_doc"}, []string{"true", "CloudQuery API key, created in the CloudQuery Cloud console."})},
		{Name: "team", Type: arrow.BinaryTypes.String, Nullable: true,
			Metadata: arrow.NewMetadata([]string{"vgi_doc"}, []string{"CloudQuery team name, for downloads made on a team's behalf (optional)."})},
		{Name: "hub_url", Type: arrow.BinaryTypes.String, Nullable: true,
			Metadata: arrow.NewMetadata([]string{"vgi_doc"}, []string{"Hub API base URL (default https://api.cloudquery.io); for mirrors and tests."})},
	}, &secretSchemaMetadata),
}

const catalogComment = "Host CloudQuery source plugins and query their tables from DuckDB: each plugin is a schema, each plugin table a table, " +
	"streamed as Arrow from the plugin's sync. No CloudQuery CLI or destination database needed."

const catalogDocLLM = "This worker hosts CloudQuery source plugins (the cloudquery/plugin-sdk ecosystem: aws, gcp, github, k8s, hackernews " +
	"and many more) directly over their gRPC plugin protocol v3 and exposes their tables as DuckDB tables. Pick plugins with the plugins " +
	"ATTACH option, a comma-separated list of [schema=]ref where ref is a local plugin binary path, a CloudQuery Hub plugin such as " +
	"cloudquery/xkcd@v1.6.1, a GitHub release such as github:cloudquery/test@v4.1.0, or grpc:host:port for a plugin server that is already " +
	"running. Each plugin becomes a schema of the attached catalog (named after the plugin unless schema= is given) and each plugin table a " +
	"table there, child relations included (join a child's _cq_parent_id to its parent's _cq_id; ids are deterministic). Non-secret " +
	"plugin settings go in the spec ATTACH option as a JSON or YAML object keyed by schema. Credentials go in a DuckDB secret of type " +
	"cloudquery whose PLUGIN field names the plugin, whose SPEC field holds the JSON or YAML spec, and whose SCOPE is " +
	"cloudquery://<schema>; it is deep-merged over the ATTACH spec at query time. Every query runs a plugin sync of just that table, so it " +
	"costs the same API calls as a CloudQuery sync of the table: CloudQuery plugins accept no per-query filters, and WHERE clauses are " +
	"applied by DuckDB afterwards. Only the selected columns are returned, LIMIT stops the sync early, and scan(schema, table_name, " +
	"max_rows := N) caps the rows fetched. The main schema introspects what is attached (resolved plugins, tables, relations, " +
	"column type mappings) and can pre-install Hub plugins using an API key held in a cloudquery_hub secret. Replace cloudquery with " +
	"the ATTACH alias."

// CatalogDocMD is the catalog's long human-facing description.
const CatalogDocMD = "# CloudQuery source plugins in DuckDB\n\n" +
	"Hosts [CloudQuery](https://www.cloudquery.io) source plugins and turns their tables into DuckDB tables, talking to each plugin " +
	"directly over the plugin protocol v3 (gRPC + Arrow): no CloudQuery CLI, no destination database. Name plugins in the `plugins` " +
	"ATTACH option; each becomes a schema, and its tables are queried like any other.\n\n" +
	"- **Plugins**: local binaries, GitHub release archives, CloudQuery Hub downloads (API key required), or a running plugin server. " +
	"Archives are sha256-verified and cached.\n" +
	"- **Credentials**: a DuckDB secret of type `cloudquery`, never the ATTACH string.\n" +
	"- **Arrow end to end**: record batches from the plugin's sync go to DuckDB with their buffers intact; `UUID` and `JSON` columns keep " +
	"their types.\n" +
	"- **Pushdown**: table selection and projection; CloudQuery plugins take no filters, so WHERE runs in DuckDB, and LIMIT cancels the sync.\n"

const schemaDocLLM = "The main schema introspects and reads the plugins of the current ATTACH; the plugin tables themselves live " +
	"in one schema per plugin. Its tables report what the plugins option resolved to (versions, verified sha256 digests, spec " +
	"JSON Schema), every plugin table with its parent relation and primary key, and every column with its CloudQuery and DuckDB " +
	"types. scan(schema, table_name, max_rows := 0, strict := false) reads a plugin table directly, which is the way in when a " +
	"table needs credentials only a secret holds; a plugin can also be downloaded into the cache ahead of use. Everything here " +
	"reflects the current ATTACH and is empty when it named no plugins."

// SchemaComment is the main schema's one-line comment.
const SchemaComment = "Introspection of the CloudQuery plugins this ATTACH hosts, plus direct scans and plugin installs."

// SchemaDocMD is the main schema's human-facing description.
const SchemaDocMD = "## main\n\n" +
	"Introspection of the plugins this ATTACH hosts: what the `plugins` option resolved to (version, source, verified sha256 " +
	"digest, spec JSON Schema), which tables exist and how child relations hang off their parents (`_cq_parent_id` to " +
	"`_cq_id`), and how each CloudQuery column type maps to DuckDB (`UUID` and `JSON` keep their types).\n\n" +
	"The plugin tables themselves live in one schema per plugin. Every read runs a plugin sync of just that table: plugins take " +
	"no filters, so `WHERE` runs in DuckDB, while projection is pushed down and `LIMIT` stops the sync early. When a plugin's " +
	"tables depend on credentials that only a `cloudquery` secret holds, read them with `scan` instead. Everything here is " +
	"empty when the ATTACH named no plugins."

type agentTask struct {
	Name   string `json:"name"`
	Prompt string `json:"prompt"`
}

var agentTestTasks = []agentTask{
	{Name: "attached_plugin_count", Prompt: "Using the cloudquery worker, how many CloudQuery plugins are attached to the cloudquery catalog?"},
	{Name: "child_relation", Prompt: "In the cloudquery catalog, which plugin table is a child relation of another table? Give the child table and its parent."},
	{Name: "uuid_columns", Prompt: "Which columns of the fixture plugin's fixture_item table are read as DuckDB UUID values?"},
	{Name: "items_by_category", Prompt: "Using the cloudquery fixture plugin, how many fixture items are there in each category?"},
	{Name: "software_restocks", Prompt: "In the cloudquery fixture plugin, how many restocked events belong to software items?"},
	{Name: "fixture_greeting", Prompt: "What greeting is the cloudquery fixture plugin configured with?"},
	{Name: "flaky_rows", Prompt: "The cloudquery fixture plugin's flaky table fails partway through its sync. How many rows does it return before the failure?"},
	{Name: "install_test_plugin", Prompt: "Install the CloudQuery test plugin (github:cloudquery/test@v4.1.0) into the cloudquery worker's plugin cache. Which registry and version was it installed from?"},
	{Name: "capped_scan", Prompt: "Using the cloudquery worker's scan function, read at most 5 rows of the fixture plugin's item table, stopping the plugin's sync early. How many rows come back?"},
}

// CatalogTags are the catalog-level discovery tags.
var CatalogTags = map[string]string{
	"vgi.title":              "CloudQuery Source Plugins",
	"vgi.subtitle":           "Every CloudQuery source plugin's tables as DuckDB tables, streamed as Arrow, with secret-held specs.",
	"vgi.doc_llm":            catalogDocLLM,
	"vgi.doc_md":             CatalogDocMD,
	"vgi.source_url":         SourceURL,
	"vgi.icon_url":           "https://query.farm/media-kit/products/vgi/vgi-logo.png",
	"vgi.license":            "0BSD",
	"vgi.author":             "Query.Farm",
	"vgi.copyright":          "Copyright 2026 Query Farm LLC - https://query.farm",
	"vgi.keywords":           keywordsArray("cloudquery, cloudquery plugins, source plugins, cloud asset inventory, elt, aws, gcp, github, grpc, arrow"),
	"vgi.support_contact":    SourceURL + "/issues",
	"vgi.support_policy_url": SourceURL + "#support",
	"vgi.agent_test_tasks":   mustJSON(agentTestTasks),
}

func schemaTags(allExamples []exampleQuery) map[string]string {
	return map[string]string{
		"vgi.title":           "CloudQuery Plugin Introspection",
		"domain":              "integrations",
		"vgi.keywords":        keywordsArray("cloudquery, plugins, tables, columns, relations, scan, install"),
		"vgi.doc_llm":         schemaDocLLM,
		"vgi.doc_md":          SchemaDocMD,
		"vgi.categories":      mustJSON(SchemaCategories),
		"vgi.example_queries": mustJSON(allExamples),
	}
}

var scanExamples = []vgi.CatalogExample{
	{SQL: "SELECT category, count(*) AS items FROM cloudquery.main.scan('fixture', 'fixture_item') GROUP BY category ORDER BY category",
		Description: "Read a plugin table directly and aggregate it"},
	{SQL: "SELECT count(*) AS fetched FROM cloudquery.main.scan('fixture', 'fixture_item', max_rows := 10)",
		Description: "Stop the plugin's sync after 10 rows"},
}

func scanTags() map[string]string {
	tags := objectTags(docs{
		Title:    "Scan Plugin Table",
		Category: "scan",
		Keywords: "cloudquery, plugin table, sync, query, limit, arrow",
		DocLLM: "Read one table of an attached CloudQuery source plugin: scan(schema, table_name, max_rows := 0, strict := false). schema is " +
			"a plugin schema of this ATTACH (see the plugin table), table_name a table of that plugin (see plugin_table). The output " +
			"columns are the plugin table's own columns. Each call runs a plugin sync of just that table; CloudQuery plugins take no " +
			"filters, so WHERE is applied by DuckDB afterwards. max_rows stops the sync after that many rows. strict := true fails the " +
			"query when the plugin reports an error for a table (by default errors are logged and the rows fetched are returned). The " +
			"tables in each plugin schema are the usual way to query; call scan() directly when a plugin's tables only appear once the " +
			"credentials held in a secret are applied.",
		DocMD: "## scan(schema, table_name, max_rows := 0, strict := false)\n\nReads one plugin table by running a sync of just that " +
			"table; the output columns are the table's own. The tables in each plugin schema are backed by this function, so you rarely " +
			"need it, except for plugins whose tables only appear once the credentials in a secret are applied.",
	})
	tags["vgi.result_dynamic_columns_md"] = "The output columns are the plugin table's own columns, as listed for the table in " +
		"main.plugin_column: Arrow types pass through (utf8 is VARCHAR, int64 BIGINT, timestamp[us, UTC] TIMESTAMP WITH TIME ZONE, " +
		"lists and structs as LIST and STRUCT), and CloudQuery's extension types map to UUID (uuid), JSON (json) and VARCHAR (inet, mac). " +
		"Plugin tables usually start with these two columns. For example, scan('fixture', 'fixture_item_event') returns:\n\n" +
		"| Name | Type | Description |\n|---|---|---|\n" +
		"| _cq_id | UUID | Row id, derived from the primary key. |\n" +
		"| _cq_parent_id | UUID | The parent row's _cq_id (fixture_item). |\n" +
		"| item_id | BIGINT | Parent item id. |\n" +
		"| seq | BIGINT | Event number within the item. |\n" +
		"| kind | VARCHAR | created or restocked. |\n" +
		"| at | TIMESTAMP WITH TIME ZONE | When it happened. |\n"
	tags["vgi.executable_examples"] = mustJSON([]map[string]any{
		{"description": "Items per category",
			"sql":             "SELECT category, count(*) AS items FROM cloudquery.main.scan('fixture', 'fixture_item') GROUP BY category ORDER BY category",
			"expected_result": [][]any{{"hardware", 8}, {"services", 8}, {"software", 9}}},
		{"description": "A sync stopped after 10 rows",
			"sql":             "SELECT count(*) AS fetched FROM cloudquery.main.scan('fixture', 'fixture_item', max_rows := 10)",
			"expected_result": [][]any{{10}}},
	})
	return tags
}

var installExamples = []vgi.CatalogExample{
	{SQL: "SELECT ref, plugin_version, archive_sha256, from_cache FROM cloudquery.main.install_plugin('github:cloudquery/test@v4.1.0')",
		Description: "Download the CloudQuery test plugin from its GitHub release into the cache"},
}

func installTags() map[string]string {
	tags := objectTags(docs{
		Title:    "Install Plugin",
		Category: "plugins",
		Keywords: "cloudquery hub, download, install, plugin cache, api key, sha256",
		DocLLM: "Download a CloudQuery plugin into the worker's verified cache: install_plugin(ref). ref is a Hub plugin (team/name@version, " +
			"or hub:team/name for the latest version) or a GitHub release (github:team/name@version), optionally pinned with " +
			"#sha256:<hex>. The CloudQuery Hub refuses anonymous downloads, so Hub refs need CREATE SECRET (TYPE cloudquery_hub, " +
			"API_KEY '...') first; GitHub refs need no key. Returns one row: the resolved ref and version, the verified archive and binary " +
			"sha256, the cached binary path, and from_cache. After installing, ATTACH with the same ref starts the plugin without the " +
			"network. Already-cached plugins are not downloaded again.",
		DocMD: "## install_plugin(ref)\n\nDownloads a Hub or GitHub plugin archive, verifies its sha256 against the registry's " +
			"published checksum, and caches the binary so an ATTACH can use it offline. Hub downloads read the API key from a " +
			"`cloudquery_hub` secret.",
	})
	tags["vgi.result_columns_schema"] = mustJSON(installResultColumns())
	return tags
}

// tableDocs is the discovery metadata of one of the main schema's tables.
type tableDocs struct {
	Title, Comment, Keywords, DocLLM, DocMD string
	Examples                                []vgi.CatalogExample
	Executable                              []map[string]any
}

func (d tableDocs) tags() map[string]string {
	tags := map[string]string{
		"vgi.title":    d.Title,
		"vgi.category": "introspection",
		"domain":       "integrations",
		"vgi.keywords": keywordsArray(d.Keywords),
		"vgi.doc_llm":  d.DocLLM,
		"vgi.doc_md":   d.DocMD,
	}
	tags = withExamples(tags, d.Examples)
	if len(d.Executable) > 0 {
		tags["vgi.executable_examples"] = mustJSON(d.Executable)
	}
	return tags
}

var pluginsDocs = tableDocs{
	Title:    "Attached Plugin Inventory",
	Comment:  "The CloudQuery plugins of this ATTACH, one row per plugin schema, with versions, verified digests and table counts.",
	Keywords: "cloudquery, plugins, attached plugins, version, sha256, spec schema",
	DocLLM: "One row per CloudQuery plugin of the current ATTACH: schema_name (where its tables live), plugin (the name it reports), ref " +
		"(what the plugins option named), registry (cloudquery for the Hub, github, local or grpc), plugin_version, archive_sha256 and " +
		"binary_sha256 (verified digests, NULL for a local binary), binary_path, table_count (child relations included) and spec_schema " +
		"(the JSON Schema of the plugin's spec, when it publishes one). Use it to see what an ATTACH resolved its plugins option to and " +
		"which spec keys a plugin accepts.",
	DocMD: "## plugin\n\nWhat the ATTACH's `plugins` option resolved to: one row per plugin schema, with the verified digests of " +
		"downloaded plugins and the JSON Schema of each plugin's spec.",
	Examples: []vgi.CatalogExample{{SQL: "SELECT schema_name, plugin, plugin_version, table_count FROM cloudquery.main.plugin ORDER BY schema_name",
		Description: "The attached plugins and how many tables each serves"}},
	Executable: []map[string]any{{"description": "The plugin the linter's worker launches with",
		"sql":             "SELECT schema_name, plugin, plugin_version, table_count FROM cloudquery.main.plugin ORDER BY schema_name",
		"expected_result": [][]any{{"fixture", "fixture", "v1.0.0", 4}}}},
}

var pluginTablesDocs = tableDocs{
	Title:    "Plugin Table Catalog",
	Comment:  "Every table of the attached plugins, with its parent relation, child tables and primary key.",
	Keywords: "cloudquery, plugin tables, relations, parent table, child tables, primary key",
	DocLLM: "One row per table of the attached plugins: schema_name, table_name, title, description, parent_table (the table this one " +
		"is a child relation of, whose rows it is fetched under; NULL for top-level tables), child_tables, column_count, primary_key_columns, " +
		"is_incremental, is_paid and permissions_needed. A child table's _cq_parent_id joins its parent's _cq_id. Reading a child " +
		"table makes the plugin walk its parent too, so it costs at least as many API calls as the parent.",
	DocMD: "## plugin_table\n\nThe tables each plugin serves and how they relate. CloudQuery fetches *child* tables once per parent " +
		"row; join `child._cq_parent_id = parent._cq_id`.",
	Examples: []vgi.CatalogExample{{SQL: "SELECT schema_name, table_name, parent_table FROM cloudquery.main.plugin_table WHERE parent_table IS NOT NULL ORDER BY ALL",
		Description: "Child relations and their parents"}},
	Executable: []map[string]any{{"description": "Child relations of the fixture plugin",
		"sql":             "SELECT table_name, parent_table FROM cloudquery.main.plugin_table WHERE parent_table IS NOT NULL ORDER BY table_name",
		"expected_result": [][]any{{"fixture_item_event", "fixture_item"}}}},
}

var pluginColumnsDocs = tableDocs{
	Title:    "Plugin Column Types",
	Comment:  "Every column of every attached plugin table, with its CloudQuery type, the DuckDB type it is read as, and its key flags.",
	Keywords: "cloudquery, columns, types, uuid, json, inet, mac, primary key",
	DocLLM: "One row per column per plugin table: schema_name, table_name, column_name, ordinal, cloudquery_type (the plugin's Arrow " +
		"type, with CloudQuery's extension types named uuid, json, inet and mac), duckdb_type (the type the column is read as: `UUID`, " +
		"`JSON`, `VARCHAR` for inet and mac, otherwise the Arrow type's DuckDB equivalent), nullable, is_primary_key, is_unique and " +
		"incremental_key. Use it to find columns by type or to see a table's primary key.",
	DocMD: "## plugin_column\n\nThe columns of every plugin table and how their CloudQuery types map to DuckDB.",
	Examples: []vgi.CatalogExample{{SQL: "SELECT table_name, column_name, cloudquery_type, duckdb_type FROM cloudquery.main.plugin_column WHERE cloudquery_type IN ('uuid', 'json', 'inet', 'mac') ORDER BY ALL",
		Description: "Columns with CloudQuery extension types and their DuckDB types"}},
	Executable: []map[string]any{{"description": "Extension-typed columns of fixture_item",
		"sql":             "SELECT column_name, duckdb_type FROM cloudquery.main.plugin_column WHERE table_name = 'fixture_item' AND cloudquery_type IN ('uuid', 'json', 'inet', 'mac') AND column_name NOT LIKE '\\_cq%' ESCAPE '\\' ORDER BY ordinal",
		"expected_result": [][]any{{"item_uuid", "UUID"}, {"attributes", "JSON"}, {"ip", "VARCHAR"}, {"mac", "VARCHAR"}}}},
}
