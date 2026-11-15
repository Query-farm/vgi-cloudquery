// The per-ATTACH catalog: one schema per plugin, one table per plugin table.
//
//	ATTACH 'cloudquery' AS cq (TYPE vgi, LOCATION '...',
//	    plugins 'github:cloudquery/test@v4.1.0, fx=/opt/cq-source-fixture',
//	    spec    '{"fx": {"item_count": 100}}');
//
// At ATTACH the worker resolves every plugin (fetching Hub or GitHub archives
// into the verified cache), starts it with the ATTACH's non-secret spec and
// no_connection set (the way `cloudquery tables` lists tables without
// credentials), and asks it for its tables. The schemas are served through
// catalog_contents, so they exist only for the ATTACH that named them. Each
// table's scan is cloudquery.main.scan(schema, table).

package cloudquery

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/Query-farm/vgi-go/vgi"
	"github.com/Query-farm/vgi-go/vgi/generated"
	"github.com/Query-farm/vgi-rpc-go/vgirpc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

// attachTimeout bounds plugin fetch + start + table discovery at ATTACH.
const attachTimeout = 10 * time.Minute

// attachedPlugin is one plugin of an ATTACH, as recorded in the attach data.
type attachedPlugin struct {
	Ref     PluginRef     `json:"ref"`
	Fetched FetchedPlugin `json:"fetched"`
	// Spec is the ATTACH's non-secret spec for it.
	Spec map[string]any `json:"spec,omitempty"`
}

// attachState is the worker's per-ATTACH data (attach_opaque_data). It holds
// no credentials: secret specs are resolved per query.
type attachState struct {
	Marker    string           `json:"vgi_cloudquery"`
	Plugins   []attachedPlugin `json:"plugins"`
	CacheDir  string           `json:"cache_dir,omitempty"`
	HubURL    string           `json:"hub_url,omitempty"`
	GitHubURL string           `json:"github_url,omitempty"`
}

const attachMarker = "v1"

func decodeAttachState(data []byte) (*attachState, bool) {
	if len(data) == 0 || data[0] != '{' {
		return nil, false
	}
	var st attachState
	if err := json.Unmarshal(data, &st); err != nil || st.Marker != attachMarker {
		return nil, false
	}
	return &st, true
}

func (st *attachState) plugin(schema string) (*attachedPlugin, bool) {
	if st == nil {
		return nil, false
	}
	schema = strings.ToLower(schema)
	for i := range st.Plugins {
		if st.Plugins[i].Ref.Schema == schema {
			return &st.Plugins[i], true
		}
	}
	return nil, false
}

func (st *attachState) schemaNames() []string {
	var out []string
	if st != nil {
		for _, p := range st.Plugins {
			out = append(out, p.Ref.Schema)
		}
	}
	return out
}

func (st *attachState) fetchConfig() fetchConfig {
	if st == nil {
		return fetchConfig{CacheDir: attachDefaults[optCacheDir], HubURL: attachDefaults[optHubURL], GitHubURL: attachDefaults[optGitHubURL]}
	}
	return fetchConfig{CacheDir: st.CacheDir, HubURL: st.HubURL, GitHubURL: st.GitHubURL}
}

// Attach option names.
const (
	optPlugins   = "plugins"
	optSpec      = "spec"
	optCacheDir  = "cache_dir"
	optHubURL    = "hub_url"
	optGitHubURL = "github_url"
)

// AttachOptionSpecs are the ATTACH options this worker accepts.
var AttachOptionSpecs = []vgi.AttachOptionSpec{
	{Name: optPlugins, Type: arrow.BinaryTypes.String, Description: "Comma-separated CloudQuery source plugins to host, each `[schema=]ref`. A ref is a local " +
		"plugin binary path, a CloudQuery Hub plugin (`cloudquery/xkcd@v1.6.1`, `hub:team/name`), a GitHub release " +
		"(`github:cloudquery/test@v4.1.0`), or a running plugin server (`grpc:host:port`); append `#sha256:<hex>` to pin an archive. " +
		"Each plugin becomes a schema named after it (or `schema`), holding one table per plugin table."},
	{Name: optSpec, Type: arrow.BinaryTypes.String, Description: "Non-secret plugin specs: a JSON or YAML object keyed by schema, each value the plugin's own " +
		"`spec:` object (`{\"hackernews\": {\"item_concurrency\": 10}}`). Used to discover tables at ATTACH and for every query. " +
		"Never put credentials here: use CREATE SECRET (TYPE cloudquery, ...)."},
	{Name: optCacheDir, Type: arrow.BinaryTypes.String, Description: "Directory for downloaded plugins (default: <user cache dir>/vgi-cloudquery/plugins). " +
		"Binaries are kept with their verified sha256 and reused offline."},
	{Name: optHubURL, Type: arrow.BinaryTypes.String, Description: "CloudQuery Hub API base URL (default https://api.cloudquery.io); for mirrors and tests."},
	{Name: optGitHubURL, Type: arrow.BinaryTypes.String, Description: "Base URL GitHub release downloads come from (default https://github.com); for mirrors and tests."},
}

// optionStrings reads the string ATTACH options from the request batch.
func optionStrings(req *vgi.CatalogAttachRequestWire) (map[string]string, error) {
	out := map[string]string{}
	if req.Options == nil || len(*req.Options) == 0 {
		return out, nil
	}
	batch, err := vgi.DeserializeRecordBatch(*req.Options)
	if err != nil {
		return nil, fmt.Errorf("reading ATTACH options: %w", err)
	}
	defer batch.Release()
	if batch.NumRows() == 0 {
		return out, nil
	}
	for i, f := range batch.Schema().Fields() {
		col := batch.Column(i)
		if col.IsNull(0) {
			continue
		}
		if s, ok := col.(*array.String); ok {
			out[strings.ToLower(f.Name)] = s.Value(0)
		}
	}
	return out, nil
}

// attachDefaults are the worker's launch-time defaults for ATTACH options
// (see SetAttachDefaults).
var attachDefaults = map[string]string{}

// SetAttachDefaults sets the values used for ATTACH options an ATTACH leaves
// unset (keys: plugins, spec, cache_dir, hub_url, github_url). When an ATTACH
// sets plugins, the default spec does not apply to it.
func SetAttachDefaults(d map[string]string) { attachDefaults = d }

// resolveBinary returns the binary a ref runs, fetching it when needed.
func resolveBinary(ctx context.Context, r PluginRef, cfg fetchConfig) (FetchedPlugin, error) {
	switch r.Registry {
	case RegistryLocal:
		return FetchedPlugin{Binary: r.Path}, nil
	case RegistryGRPC:
		return FetchedPlugin{}, nil
	}
	fp, err := FetchPlugin(ctx, r, cfg)
	if err != nil {
		return FetchedPlugin{}, err
	}
	return *fp, nil
}

// validateAttach resolves and starts every plugin of an ATTACH.
func validateAttach(req *vgi.CatalogAttachRequestWire, _ *vgirpc.CallContext) (*vgi.AttachDecision, error) {
	opts, err := optionStrings(req)
	if err != nil {
		return nil, err
	}
	if opts[optPlugins] == "" && attachDefaults[optPlugins] != "" {
		opts[optPlugins] = attachDefaults[optPlugins]
		if opts[optSpec] == "" {
			opts[optSpec] = attachDefaults[optSpec]
		}
	}
	for _, k := range []string{optCacheDir, optHubURL, optGitHubURL} {
		if opts[k] == "" {
			opts[k] = attachDefaults[k]
		}
	}
	refs, err := ParsePluginRefs(opts[optPlugins])
	if err != nil {
		return nil, err
	}
	specs, err := parseAttachSpec(opts[optSpec])
	if err != nil {
		return nil, err
	}
	known := map[string]bool{}
	for _, r := range refs {
		known[r.Schema] = true
	}
	for schema := range specs {
		if !known[schema] {
			return nil, fmt.Errorf("spec option has settings for %q, which is not a schema in the plugins option (%s)", schema, strings.Join(keys(known), ", "))
		}
	}
	st := &attachState{Marker: attachMarker, CacheDir: opts[optCacheDir], HubURL: opts[optHubURL], GitHubURL: opts[optGitHubURL]}
	if st.CacheDir == "" {
		st.CacheDir = DefaultCacheDir()
	}
	ctx, cancel := context.WithTimeout(context.Background(), attachTimeout)
	defer cancel()
	for _, r := range refs {
		ap := attachedPlugin{Ref: r, Spec: specs[r.Schema]}
		if ap.Fetched, err = resolveBinary(ctx, r, st.fetchConfig()); err != nil {
			return nil, err
		}
		if _, err := discoverTables(ctx, &ap); err != nil {
			return nil, fmt.Errorf("schema %s: %w", r.Schema, err)
		}
		st.Plugins = append(st.Plugins, ap)
	}
	data, err := json.Marshal(st)
	if err != nil {
		return nil, err
	}
	return &vgi.AttachDecision{AttachOpaqueData: data}, nil
}

func keys(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

// discoveryLaunch starts a plugin with the ATTACH's non-secret spec and no
// connection: enough to list its tables.
func discoveryLaunch(ap *attachedPlugin) (launch, error) {
	spec, err := specJSON(ap.Spec)
	if err != nil {
		return launch{}, err
	}
	return launch{Ref: ap.Ref, Binary: ap.Fetched.Binary, Spec: spec, NoConnection: true}, nil
}

// discoverTables returns the plugin's tables under the ATTACH spec.
func discoverTables(ctx context.Context, ap *attachedPlugin) (*pluginInfo, error) {
	ln, err := discoveryLaunch(ap)
	if err != nil {
		return nil, err
	}
	return host.info(ctx, ln)
}

// tableItem serializes one plugin table as a catalog TableInfo whose scan is
// cloudquery.main.scan(schema, table).
func tableItem(ap *attachedPlugin, s *arrow.Schema) ([]byte, error) {
	table := tableName(s)
	scanBytes, err := vgi.SerializeScanFunctionResult(scanFunctionFor(ap.Ref.Schema, table))
	if err != nil {
		return nil, err
	}
	constraints, err := notNullConstraints(ap.Ref.Schema, table, s)
	if err != nil {
		return nil, err
	}
	return vgi.SerializeTableInfo(&vgi.TableInfo{
		Name:         table,
		SchemaPath:   []string{ap.Ref.Schema},
		Comment:      tableComment(s),
		Tags:         pluginTableTags(ap, s),
		Columns:      outputSchema(s),
		Constraints:  constraints,
		ScanFunction: scanBytes,
	})
}

// notNullConstraints attests the plugin's NOT NULL columns. The scan
// enforces them: a NULL in such a column fails the query.
func notNullConstraints(schema, table string, s *arrow.Schema) ([][]byte, error) {
	var out [][]byte
	for _, f := range s.Fields() {
		if f.Nullable {
			continue
		}
		rec, err := vgi.ConstraintRecord(vgi.ConstraintRecordSpec{
			SchemaPath: []string{schema}, Table: table, ID: table + ":not_null:" + f.Name,
			Kind: vgi.ConstraintNotNull, Columns: [][]string{{f.Name}},
			Enforcement: vgi.ConstraintEnforced, Validation: vgi.ConstraintAllRows,
		})
		if err != nil {
			return nil, err
		}
		out = append(out, rec)
	}
	return out, nil
}

func tableComment(s *arrow.Schema) string {
	md := s.Metadata()
	c := strings.TrimSpace(mdValue(md, mdTableDescription))
	if c == "" {
		c = "CloudQuery table " + tableName(s) + "."
	}
	if p := mdValue(md, mdTableDependsOn); p != "" {
		c = strings.TrimRight(c, ".") + ". Child of " + p + " (join _cq_parent_id to its _cq_id)."
	}
	return c
}

func scanFunctionFor(schema, table string) *vgi.ScanFunctionResult {
	main := []string{"main"}
	return &vgi.ScanFunctionResult{
		FunctionName: scanFunctionName,
		PositionalArguments: []vgi.ScanArg{
			{Value: schema, Type: arrow.BinaryTypes.String},
			{Value: table, Type: arrow.BinaryTypes.String},
		},
		SchemaPath: &main,
	}
}

// pluginSchemaContents builds one plugin schema's catalog_contents entry.
func pluginSchemaContents(ap *attachedPlugin) (generated.SchemaContents, error) {
	info, err := discoverTables(context.Background(), ap)
	if err != nil {
		return generated.SchemaContents{}, err
	}
	comment := schemaComment(ap, info)
	schemaInfo, err := vgi.SerializeSchemaInfo(&vgi.SchemaInfo{
		Path: []string{ap.Ref.Schema}, Comment: &comment,
		Tags: pluginSchemaTags(ap, info),
	})
	if err != nil {
		return generated.SchemaContents{}, err
	}
	tables := [][]byte{}
	for _, name := range info.tableNames() {
		s, _ := info.table(name)
		item, err := tableItem(ap, s)
		if err != nil {
			return generated.SchemaContents{}, fmt.Errorf("table %s.%s: %w", ap.Ref.Schema, name, err)
		}
		tables = append(tables, item)
	}
	empty := func() [][]byte { return [][]byte{} }
	return generated.SchemaContents{
		Path: []string{ap.Ref.Schema}, Schema: schemaInfo, Tables: tables,
		Views: empty(), ScalarFunctions: empty(), AggregateFunctions: empty(), TableFunctions: empty(),
		ScalarMacros: empty(), TableMacros: empty(), Indexes: empty(),
	}, nil
}

func schemaComment(ap *attachedPlugin, info *pluginInfo) string {
	name := info.Name
	if name == "" {
		name = ap.Ref.Name
	}
	c := fmt.Sprintf("CloudQuery source plugin %s (%s), %d tables.", name, ap.Ref.Display(), len(info.Tables))
	if info.Version != "" {
		c += " Version " + info.Version + "."
	}
	return c
}

// catalogContents serves the static main schema plus one schema per plugin.
func catalogContents(call *vgi.CatalogContentsCall) (vgi.CatalogContentsResult, error) {
	schemas, err := call.Contents()
	if err != nil {
		return vgi.CatalogContentsResult{}, err
	}
	if st, ok := decodeAttachState(call.AttachOpaqueData); ok {
		for i := range st.Plugins {
			sc, err := pluginSchemaContents(&st.Plugins[i])
			if err != nil {
				return vgi.CatalogContentsResult{}, fmt.Errorf("schema %s: %w", st.Plugins[i].Ref.Schema, err)
			}
			schemas = append(schemas, sc)
		}
	}
	return vgi.CatalogContentsResult{Schemas: schemas}, nil
}

// schemaTables is the per-schema fallback (catalog_schema_contents_tables).
func schemaTables(attach []byte, path vgi.SchemaPath) ([]vgi.SerializedSchemaItem, bool) {
	st, ok := decodeAttachState(attach)
	if !ok || len(path) != 1 {
		return nil, false
	}
	ap, ok := st.plugin(path[0])
	if !ok {
		return nil, false
	}
	sc, err := pluginSchemaContents(ap)
	if err != nil {
		return nil, false
	}
	items := make([]vgi.SerializedSchemaItem, len(sc.Tables))
	for i, t := range sc.Tables {
		items[i] = t
	}
	return items, true
}

// tableGet answers a lookup of one plugin table by name.
func tableGet(attach []byte, path vgi.SchemaPath, name string, _, _ *string) ([]byte, bool, error) {
	st, ok := decodeAttachState(attach)
	if !ok || len(path) != 1 {
		return nil, false, nil
	}
	ap, ok := st.plugin(path[0])
	if !ok {
		return nil, false, nil
	}
	info, err := discoverTables(context.Background(), ap)
	if err != nil {
		return nil, true, err
	}
	s, ok := info.table(name)
	if !ok {
		return nil, false, nil
	}
	data, err := tableItem(ap, s)
	return data, true, err
}

// scanFunctionGet names the scan of a plugin table.
func scanFunctionGet(attach []byte, path vgi.SchemaPath, name string, _, _ *string) (*vgi.ScanFunctionResult, bool, error) {
	st, ok := decodeAttachState(attach)
	if !ok || len(path) != 1 {
		return nil, false, nil
	}
	if _, ok := st.plugin(path[0]); !ok {
		return nil, false, nil
	}
	return scanFunctionFor(path[0], name), true, nil
}
