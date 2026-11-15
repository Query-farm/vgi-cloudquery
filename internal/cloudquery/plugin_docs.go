// Discovery metadata generated for plugin schemas and tables, from what the
// plugin reports: titles, descriptions, relations and primary keys.

package cloudquery

import (
	"fmt"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
)

// pluginCategory is the single category of a plugin schema.
const pluginCategory = "tables"

func titleCase(s string) string {
	parts := strings.FieldsFunc(s, func(r rune) bool { return r == '_' || r == '-' })
	for i, p := range parts {
		parts[i] = strings.ToUpper(p[:1]) + p[1:]
	}
	return strings.Join(parts, " ")
}

// exampleSQL is a generated example query for a plugin table.
func exampleSQL(schema string, s *arrow.Schema) string {
	var cols []string
	for _, f := range s.Fields() {
		if len(cols) < 3 && !strings.HasPrefix(f.Name, "_cq_") {
			cols = append(cols, f.Name)
		}
	}
	if len(cols) == 0 {
		cols = []string{"*"}
	}
	return fmt.Sprintf("SELECT %s FROM %s.%s.%s LIMIT 10", strings.Join(cols, ", "), CatalogName, schema, tableName(s))
}

func pluginTableTags(ap *attachedPlugin, s *arrow.Schema) map[string]string {
	table := tableName(s)
	md := s.Metadata()
	desc := strings.TrimSpace(mdValue(md, mdTableDescription))
	if desc == "" {
		desc = "CloudQuery table " + table + "."
	}
	title := mdValue(md, mdTableTitle)
	if title == "" {
		title = titleCase(strings.TrimPrefix(table, ap.Ref.Schema+"_"))
	}
	llm := fmt.Sprintf("%s A table of the CloudQuery %s plugin, with %d columns; each query runs a plugin sync of this table, and WHERE "+
		"filters are applied by DuckDB afterwards.", strings.TrimRight(desc, ".")+".", ap.Ref.Name, s.NumFields())
	doc := fmt.Sprintf("## %s\n\n%s", table, desc)
	if pk := primaryKey(s); len(pk) > 0 {
		llm += " Primary key: " + strings.Join(pk, ", ") + "."
	}
	if p := mdValue(md, mdTableDependsOn); p != "" {
		llm += fmt.Sprintf(" It is a child relation of %s: rows are fetched once per %s row, and _cq_parent_id joins %s._cq_id.", p, p, p)
		doc += fmt.Sprintf("\n\nChild of `%s`: join `_cq_parent_id` to `%s._cq_id`.", p, p)
	}
	keywords := []string{"cloudquery", ap.Ref.Name, strings.ReplaceAll(strings.TrimPrefix(table, ap.Ref.Schema+"_"), "_", " ")}
	return map[string]string{
		"vgi.title":           title,
		"vgi.category":        pluginCategory,
		"provider":            ap.Ref.Name,
		"vgi.keywords":        mustJSON(keywords),
		"vgi.doc_llm":         llm,
		"vgi.doc_md":          doc,
		"vgi.example_queries": mustJSON([]exampleQuery{{Description: "A few rows of " + table, SQL: exampleSQL(ap.Ref.Schema, s)}}),
	}
}

func pluginSchemaTags(ap *attachedPlugin, info *pluginInfo) map[string]string {
	var examples []exampleQuery
	for _, t := range info.tableNames() {
		if len(examples) >= 5 {
			break
		}
		s, _ := info.table(t)
		examples = append(examples, exampleQuery{Description: "A few rows of " + t, SQL: exampleSQL(ap.Ref.Schema, s)})
	}
	name := info.Name
	if name == "" {
		name = ap.Ref.Name
	}
	llm := fmt.Sprintf("Tables of the CloudQuery %s source plugin (%s), %d in all. Each query runs a plugin sync of one table; "+
		"child tables join their parent through _cq_parent_id = _cq_id (see cloudquery.main.plugin_table).", name, ap.Ref.Display(), len(info.Tables))
	doc := fmt.Sprintf("## %s\n\nThe tables of the CloudQuery **%s** source plugin, loaded from `%s`: %d tables, each read by "+
		"running a plugin sync of just that table.\n\nChild tables are fetched once per parent row; join a child's `_cq_parent_id` "+
		"to its parent's `_cq_id` (see `main.plugin_table`). CloudQuery plugins take no filters, so `WHERE` runs in DuckDB after "+
		"the rows arrive; select only the columns you need and use `LIMIT` to stop a sync early.",
		ap.Ref.Schema, name, ap.Ref.Display(), len(info.Tables))
	return map[string]string{
		"vgi.title":              fmt.Sprintf("CloudQuery %s Plugin", titleCase(name)),
		"domain":                 "integrations",
		"vgi.keywords":           mustJSON([]string{"cloudquery", name, "plugin tables"}),
		"vgi.doc_llm":            llm,
		"vgi.doc_md":             doc,
		"vgi.categories":         mustJSON([]category{{Name: pluginCategory, Title: "Plugin tables", Description: "Tables served by the " + name + " plugin."}}),
		"vgi.example_queries":    mustJSON(examples),
		"cloudquery.plugin":      ap.Ref.Display(),
		"cloudquery.version":     info.Version,
		"cloudquery.registry":    ap.Ref.Registry,
		"cloudquery.table_count": fmt.Sprint(len(info.Tables)),
	}
}
