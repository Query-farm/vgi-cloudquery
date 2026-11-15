// Introspection tables over the plugins of the current ATTACH: plugin,
// plugin_table and plugin_column. They read the tables discovered with the
// ATTACH's (non-secret) spec.

package cloudquery

import (
	"context"
	"encoding/json"
	"sort"
	"strings"

	"github.com/Query-farm/vgi-go/vgi"
	"github.com/Query-farm/vgi-rpc-go/vgirpc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// InfoState records whether the single batch has been emitted.
type InfoState struct {
	Done bool
}

// infoRow holds values in schema order: string, *string, int64, bool,
// []string, or jsonText (for JSON columns).
type infoRow []any

type jsonText *string

// infoFunc is a no-argument table function that emits rows computed from the
// attach state.
type infoFunc struct {
	name   string
	meta   func() vgi.FunctionMetadata
	schema *arrow.Schema
	rows   func(st *attachState) ([]infoRow, error)
}

func (f *infoFunc) Name() string                                    { return f.name }
func (f *infoFunc) Metadata() vgi.FunctionMetadata                  { return f.meta() }
func (f *infoFunc) ArgumentSpecs() []vgi.ArgSpec                    { return nil }
func (f *infoFunc) NewState(*vgi.ProcessParams) (*InfoState, error) { return &InfoState{}, nil }
func (f *infoFunc) OnBind(*vgi.BindParams) (*vgi.BindResponse, error) {
	return vgi.BindSchema(f.schema)
}

func (f *infoFunc) Process(_ context.Context, params *vgi.ProcessParams, state *InfoState, out *vgirpc.OutputCollector) error {
	if state.Done {
		return out.Finish()
	}
	state.Done = true
	st, _ := decodeAttachState(params.AttachScope)
	rows, err := f.rows(st)
	if err != nil {
		return err
	}
	if len(rows) == 0 {
		return out.Finish()
	}
	cols := buildColumns(memory.NewGoAllocator(), f.schema, rows)
	defer func() {
		for _, c := range cols {
			c.Release()
		}
	}()
	if err := out.EmitArrays(cols, int64(len(rows))); err != nil {
		return err
	}
	return out.Finish()
}

// buildColumns turns rows into one array per schema field.
func buildColumns(mem memory.Allocator, schema *arrow.Schema, rows []infoRow) []arrow.Array {
	cols := make([]arrow.Array, schema.NumFields())
	for i, field := range schema.Fields() {
		b := array.NewBuilder(mem, field.Type)
		for _, r := range rows {
			switch v := r[i].(type) {
			case nil:
				b.AppendNull()
			case string:
				b.(*array.StringBuilder).Append(v)
			case *string:
				if v == nil {
					b.AppendNull()
				} else {
					b.(*array.StringBuilder).Append(*v)
				}
			case jsonText:
				if v == nil {
					b.AppendNull()
				} else {
					b.(*array.ExtensionBuilder).Builder.(*array.StringBuilder).Append(*v)
				}
			case int64:
				b.(*array.Int64Builder).Append(v)
			case bool:
				b.(*array.BooleanBuilder).Append(v)
			case []string:
				lb := b.(*array.ListBuilder)
				lb.Append(true)
				vb := lb.ValueBuilder().(*array.StringBuilder)
				for _, s := range v {
					vb.Append(s)
				}
			}
		}
		cols[i] = b.NewArray()
		b.Release()
	}
	return cols
}

func strPtr(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

// forEachPlugin discovers each attached plugin's tables.
func forEachPlugin(st *attachState, fn func(ap *attachedPlugin, info *pluginInfo)) error {
	if st == nil {
		return nil
	}
	for i := range st.Plugins {
		ap := &st.Plugins[i]
		info, err := discoverTables(context.Background(), ap)
		if err != nil {
			return err
		}
		fn(ap, info)
	}
	return nil
}

var listOfString = arrow.ListOf(arrow.BinaryTypes.String)

var pluginsSchema = arrow.NewSchema([]arrow.Field{
	column("schema_name", arrow.BinaryTypes.String, "Schema the plugin's tables live in.", false),
	column("plugin", arrow.BinaryTypes.String, "Plugin name, as the plugin reports it (e.g. hackernews).", false),
	column("ref", arrow.BinaryTypes.String, "The plugin reference the ATTACH resolved: Hub or GitHub plugin, local binary path, or grpc address.", false),
	column("registry", arrow.BinaryTypes.String, "Where the plugin came from: cloudquery (Hub), github, local or grpc.", false),
	column("plugin_version", arrow.BinaryTypes.String, "Version the plugin reports.", true),
	column("archive_sha256", arrow.BinaryTypes.String, "Verified sha256 of the downloaded archive; NULL for a local binary or grpc server.", true),
	column("binary_sha256", arrow.BinaryTypes.String, "sha256 of the unpacked binary, re-checked whenever the cache is reused; NULL when not downloaded.", true),
	column("binary_path", arrow.BinaryTypes.String, "Executable the worker runs; NULL for a grpc server.", true),
	column("table_count", arrow.PrimitiveTypes.Int64, "Number of tables the plugin serves (child relations included).", false),
	column("spec_schema", jsonType, "JSON Schema of the plugin's spec, when the plugin publishes one.", true),
}, nil)

var tablesSchema = arrow.NewSchema([]arrow.Field{
	column("schema_name", arrow.BinaryTypes.String, "Plugin schema.", false),
	column("table_name", arrow.BinaryTypes.String, "Plugin table name.", false),
	column("title", arrow.BinaryTypes.String, "The plugin's title for the table.", true),
	column("description", arrow.BinaryTypes.String, "The plugin's description of the table.", true),
	column("parent_table", arrow.BinaryTypes.String, "The table this one is a child relation of (rows are fetched per parent row); NULL for a top-level table.", true),
	column("child_tables", listOfString, "Tables fetched per row of this one.", false),
	column("column_count", arrow.PrimitiveTypes.Int64, "Number of columns.", false),
	column("primary_key_columns", listOfString, "The plugin's primary key columns, in table order (empty when the table has none).", false),
	column("is_incremental", arrow.FixedWidthTypes.Boolean, "Whether the table syncs incrementally in CloudQuery (this worker always reads it in full).", false),
	column("is_paid", arrow.FixedWidthTypes.Boolean, "Whether CloudQuery marks the table as paid.", false),
	column("permissions_needed", listOfString, "Cloud permissions the plugin says the table needs.", false),
}, nil)

var columnsSchema = arrow.NewSchema([]arrow.Field{
	column("schema_name", arrow.BinaryTypes.String, "Plugin schema.", false),
	column("table_name", arrow.BinaryTypes.String, "Plugin table name.", false),
	column("column_name", arrow.BinaryTypes.String, "The plugin column's identifier, as written in SQL against the plugin table.", false),
	column("ordinal", arrow.PrimitiveTypes.Int64, "Position of the column in the table, counting from 1 (the plugin's column order).", false),
	column("cloudquery_type", arrow.BinaryTypes.String, "The column's CloudQuery (Arrow) type, e.g. utf8, int64, uuid, json, inet, list<uuid>.", false),
	column("duckdb_type", arrow.BinaryTypes.String, "The DuckDB type it is read as, e.g. VARCHAR, UUID, JSON.", false),
	column("nullable", arrow.FixedWidthTypes.Boolean, "Whether the column can be NULL.", false),
	column("is_primary_key", arrow.FixedWidthTypes.Boolean, "Whether the column is part of the plugin's primary key.", false),
	column("is_unique", arrow.FixedWidthTypes.Boolean, "Whether the plugin declares the column unique.", false),
	column("incremental_key", arrow.FixedWidthTypes.Boolean, "Whether CloudQuery uses the column as an incremental cursor.", false),
}, nil)

func pluginsRows(st *attachState) ([]infoRow, error) {
	var rows []infoRow
	err := forEachPlugin(st, func(ap *attachedPlugin, info *pluginInfo) {
		name := info.Name
		if name == "" {
			name = ap.Ref.Name
		}
		var spec jsonText
		if info.SpecSchema != "" && json.Valid([]byte(info.SpecSchema)) {
			s := info.SpecSchema
			spec = &s
		}
		rows = append(rows, infoRow{ap.Ref.Schema, name, ap.Ref.Display(), ap.Ref.Registry, strPtr(info.Version),
			strPtr(ap.Fetched.ArchiveSHA256), strPtr(ap.Fetched.BinarySHA256), strPtr(ap.Fetched.Binary),
			int64(len(info.Tables)), spec})
	})
	return rows, err
}

func jsonList(md arrow.Metadata, key string) []string {
	out := []string{}
	if v := mdValue(md, key); v != "" && v != "null" {
		_ = json.Unmarshal([]byte(v), &out)
	}
	if out == nil {
		out = []string{}
	}
	return out
}

func primaryKey(s *arrow.Schema) []string {
	out := []string{}
	for _, f := range s.Fields() {
		if mdValue(f.Metadata, mdPrimaryKey) == "true" {
			out = append(out, f.Name)
		}
	}
	return out
}

func childTables(info *pluginInfo, table string) []string {
	out := []string{}
	for _, s := range info.Tables {
		if mdValue(s.Metadata(), mdTableDependsOn) == table {
			out = append(out, tableName(s))
		}
	}
	sort.Strings(out)
	return out
}

func tablesRows(st *attachState) ([]infoRow, error) {
	var rows []infoRow
	err := forEachPlugin(st, func(ap *attachedPlugin, info *pluginInfo) {
		for _, name := range info.tableNames() {
			s, _ := info.table(name)
			md := s.Metadata()
			rows = append(rows, infoRow{ap.Ref.Schema, name, strPtr(mdValue(md, mdTableTitle)), strPtr(strings.TrimSpace(mdValue(md, mdTableDescription))),
				strPtr(mdValue(md, mdTableDependsOn)), childTables(info, name), int64(s.NumFields()), primaryKey(s),
				mdValue(md, mdTableIncremental) == "true", mdValue(md, mdTablePaid) == "true", jsonList(md, mdTablePermissions)})
		}
	})
	return rows, err
}

func columnsRows(st *attachState) ([]infoRow, error) {
	var rows []infoRow
	err := forEachPlugin(st, func(ap *attachedPlugin, info *pluginInfo) {
		for _, name := range info.tableNames() {
			s, _ := info.table(name)
			for i, f := range s.Fields() {
				rows = append(rows, infoRow{ap.Ref.Schema, name, f.Name, int64(i + 1), cqTypeName(f.Type, f.Metadata),
					duckdbTypeName(mapType(f.Type, f.Metadata)), f.Nullable, mdValue(f.Metadata, mdPrimaryKey) == "true",
					mdValue(f.Metadata, mdUnique) == "true", mdValue(f.Metadata, mdIncremental) == "true"})
			}
		}
	})
	return rows, err
}

// NewInfoTables returns the main schema's plugin, plugin_table and
// plugin_column tables, each scanned by a table-only function.
func NewInfoTables() []vgi.CatalogTable {
	mk := func(table, fn string, schema *arrow.Schema, pk []string, rows func(*attachState) ([]infoRow, error), d tableDocs) vgi.CatalogTable {
		f := &infoFunc{name: fn, schema: schema, rows: rows, meta: func() vgi.FunctionMetadata {
			return vgi.FunctionMetadata{Description: d.Comment, Stability: vgi.StabilityVolatile}
		}}
		cols := make([][]string, len(pk))
		for i, c := range pk {
			cols[i] = []string{c}
		}
		record, err := vgi.ConstraintRecord(vgi.ConstraintRecordSpec{
			SchemaPath: []string{"main"}, Table: table, ID: table + ":primary_key",
			Kind: vgi.ConstraintPrimaryKey, Columns: cols,
			Enforcement: vgi.ConstraintEnforced, Validation: vgi.ConstraintAllRows,
		})
		if err != nil {
			panic(err)
		}
		records := [][]byte{record}
		inPK := map[string]bool{}
		for _, c := range pk {
			inPK[c] = true
		}
		for _, fld := range schema.Fields() {
			if fld.Nullable || inPK[fld.Name] {
				continue
			}
			nn, err := vgi.ConstraintRecord(vgi.ConstraintRecordSpec{
				SchemaPath: []string{"main"}, Table: table, ID: table + ":not_null:" + fld.Name,
				Kind: vgi.ConstraintNotNull, Columns: [][]string{{fld.Name}},
				Enforcement: vgi.ConstraintEnforced, Validation: vgi.ConstraintAllRows,
			})
			if err != nil {
				panic(err)
			}
			records = append(records, nn)
		}
		return vgi.CatalogTable{
			Name:        table,
			Columns:     schema,
			Comment:     d.Comment,
			Tags:        d.tags(),
			Function:    vgi.AsTableFunction[InfoState](f),
			Constraints: records,
		}
	}
	return []vgi.CatalogTable{
		mk("plugin", "plugin_scan", pluginsSchema, []string{"schema_name"}, pluginsRows, pluginsDocs),
		mk("plugin_table", "plugin_table_scan", tablesSchema, []string{"schema_name", "table_name"}, tablesRows, pluginTablesDocs),
		mk("plugin_column", "plugin_column_scan", columnsSchema, []string{"schema_name", "table_name", "column_name"}, columnsRows, pluginColumnsDocs),
	}
}
