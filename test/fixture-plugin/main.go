// Command cq-source-fixture is a small CloudQuery source plugin built with the
// real plugin-sdk, used by the worker's tests so they need no network and no
// CloudQuery account.
//
// Tables:
//
//	fixture_item        item_count rows (spec, default 25) with every column
//	                     shape the worker maps: UUID, JSON, inet, MAC, lists of
//	                     UUIDs, timestamps, booleans, doubles
//	fixture_item_event  child relation of fixture_item: 2 events per item
//	fixture_whoami       one row describing the spec the plugin was initialised
//	                     with (whether a token arrived, never the token itself)
//	fixture_flaky        emits 3 rows, then its resolver fails
//
// Run it the way CloudQuery does: cq-source-fixture serve --network unix --address /tmp/x.sock
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"

	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/cloudquery/plugin-sdk/v4/message"
	"github.com/cloudquery/plugin-sdk/v4/plugin"
	"github.com/cloudquery/plugin-sdk/v4/scheduler"
	"github.com/cloudquery/plugin-sdk/v4/schema"
	"github.com/cloudquery/plugin-sdk/v4/serve"
	"github.com/cloudquery/plugin-sdk/v4/types"
	"github.com/google/uuid"
	"github.com/rs/zerolog"
)

// Version is the fixture plugin's version.
const Version = "v1.0.0"

// Spec is the plugin-specific spec (the `spec:` block of a CloudQuery source).
type Spec struct {
	ItemCount int    `json:"item_count,omitempty"`
	Greeting  string `json:"greeting,omitempty"`
	Token     string `json:"token,omitempty"`
}

const specSchema = `{"$schema":"https://json-schema.org/draft/2020-12/schema","type":"object","properties":{` +
	`"item_count":{"type":"integer","minimum":0,"description":"Rows in fixture_item (default 25)."},` +
	`"greeting":{"type":"string","description":"Greeting echoed by fixture_whoami."},` +
	`"token":{"type":"string","description":"A credential; fixture_whoami reports only whether it is set."}},` +
	`"additionalProperties":false}`

type client struct {
	plugin.UnimplementedDestination
	spec         Spec
	noConnection bool
	tables       schema.Tables
	sched        *scheduler.Scheduler
}

func (*client) ID() string { return "fixture" }

func (c *client) Close(context.Context) error { return nil }

func (c *client) Tables(_ context.Context, o plugin.TableOptions) (schema.Tables, error) {
	return c.tables.FilterDfs(o.Tables, o.SkipTables, o.SkipDependentTables)
}

func (c *client) Sync(ctx context.Context, o plugin.SyncOptions, res chan<- message.SyncMessage) error {
	if c.noConnection {
		return errors.New("fixture plugin was initialised without a connection")
	}
	tt, err := c.tables.FilterDfs(o.Tables, o.SkipTables, o.SkipDependentTables)
	if err != nil {
		return err
	}
	return c.sched.Sync(ctx, c, tt, res, scheduler.WithSyncDeterministicCQID(o.DeterministicCQID))
}

// fromMap resolves a column from the map[string]any item of its resource.
func fromMap(_ context.Context, _ schema.ClientMeta, r *schema.Resource, col schema.Column) error {
	return r.Set(col.Name, r.Item.(map[string]any)[col.Name])
}

func col(name string, typ arrow.DataType, pk bool, desc string) schema.Column {
	return schema.Column{Name: name, Type: typ, PrimaryKey: pk, NotNull: pk, Resolver: fromMap, Description: desc}
}

var categories = []string{"hardware", "software", "services"}

func itemRow(i int) map[string]any {
	created := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC).Add(time.Duration(i) * time.Hour)
	row := map[string]any{
		"id":            int64(i),
		"name":          fmt.Sprintf("item-%03d", i),
		"category":      categories[i%3],
		"price":         float64(i) * 1.5,
		"in_stock":      i%2 == 0,
		"created_at":    created,
		"item_uuid":     uuid.NewSHA1(uuid.NameSpaceOID, fmt.Appendf(nil, "item-%d", i)).String(),
		"attributes":    map[string]any{"rank": i, "color": []string{"red", "green", "blue"}[i%3]},
		"ip":            fmt.Sprintf("10.0.%d.%d/32", i/256, i%256),
		"mac":           fmt.Sprintf("02:00:00:00:%02x:%02x", i/256, i%256),
		"tags":          []string{"t" + fmt.Sprint(i%2), "all"},
		"related_uuids": []string{uuid.NewSHA1(uuid.NameSpaceOID, fmt.Appendf(nil, "item-%d", i+1)).String()},
	}
	if i%5 == 0 { // NULLs in every nullable extension column
		row["attributes"], row["ip"], row["mac"], row["related_uuids"] = nil, nil, nil, nil
	}
	return row
}

func (c *client) itemsTable() *schema.Table {
	return &schema.Table{
		Name:        "fixture_item",
		Title:       "Synthetic Inventory Items",
		Description: "Synthetic inventory items covering every CloudQuery column type the worker maps.",
		Columns: schema.ColumnList{
			col("id", arrow.PrimitiveTypes.Int64, true, "Item id."),
			col("name", arrow.BinaryTypes.String, false, "Item name."),
			col("category", arrow.BinaryTypes.String, false, "hardware, software or services."),
			col("price", arrow.PrimitiveTypes.Float64, false, "Price in dollars."),
			col("in_stock", arrow.FixedWidthTypes.Boolean, false, "Whether the item is in stock."),
			col("created_at", arrow.FixedWidthTypes.Timestamp_us, false, "Creation time."),
			col("item_uuid", types.ExtensionTypes.UUID, false, "Stable UUID."),
			col("attributes", types.ExtensionTypes.JSON, false, "Free-form attributes."),
			col("ip", types.ExtensionTypes.Inet, false, "Address."),
			col("mac", types.ExtensionTypes.MAC, false, "Hardware address."),
			col("tags", arrow.ListOf(arrow.BinaryTypes.String), false, "Tags."),
			col("related_uuids", arrow.ListOf(types.ExtensionTypes.UUID), false, "Related items."),
		},
		Resolver: func(_ context.Context, _ schema.ClientMeta, _ *schema.Resource, res chan<- any) error {
			n := c.spec.ItemCount
			for i := 1; i <= n; i++ {
				res <- itemRow(i)
			}
			return nil
		},
		Relations: schema.Tables{{
			Name:        "fixture_item_event",
			Title:       "Item Lifecycle Events",
			Description: "Two events per item: a child relation of fixture_item.",
			Columns: schema.ColumnList{
				{Name: "item_id", Type: arrow.PrimitiveTypes.Int64, PrimaryKey: true, NotNull: true, Resolver: schema.ParentColumnResolver("id"), Description: "Parent item id."},
				col("seq", arrow.PrimitiveTypes.Int64, true, "Event number within the item."),
				col("kind", arrow.BinaryTypes.String, false, "created or restocked."),
				col("at", arrow.FixedWidthTypes.Timestamp_us, false, "When it happened."),
			},
			Resolver: func(_ context.Context, _ schema.ClientMeta, parent *schema.Resource, res chan<- any) error {
				p := parent.Item.(map[string]any)
				at := p["created_at"].(time.Time)
				res <- map[string]any{"seq": int64(1), "kind": "created", "at": at}
				res <- map[string]any{"seq": int64(2), "kind": "restocked", "at": at.Add(24 * time.Hour)}
				return nil
			},
		}},
	}
}

func (c *client) whoamiTable() *schema.Table {
	return &schema.Table{
		Name:        "fixture_whoami",
		Description: "One row describing the spec the plugin was initialised with.",
		Columns: schema.ColumnList{
			col("greeting", arrow.BinaryTypes.String, false, "The spec's greeting."),
			col("has_token", arrow.FixedWidthTypes.Boolean, false, "Whether the spec carried a token."),
			col("item_count", arrow.PrimitiveTypes.Int64, false, "The spec's item_count."),
		},
		Resolver: func(_ context.Context, _ schema.ClientMeta, _ *schema.Resource, res chan<- any) error {
			res <- map[string]any{"greeting": c.spec.Greeting, "has_token": c.spec.Token != "", "item_count": int64(c.spec.ItemCount)}
			return nil
		},
	}
}

func flakyTable() *schema.Table {
	return &schema.Table{
		Name:        "fixture_flaky",
		Description: "Emits three rows, then its resolver fails.",
		Columns:     schema.ColumnList{col("n", arrow.PrimitiveTypes.Int64, false, "Row number.")},
		Resolver: func(_ context.Context, _ schema.ClientMeta, _ *schema.Resource, res chan<- any) error {
			for i := 1; i <= 3; i++ {
				res <- map[string]any{"n": int64(i)}
			}
			return errors.New("fixture_flaky: upstream API returned 503")
		},
	}
}

func newClient(_ context.Context, logger zerolog.Logger, specBytes []byte, opts plugin.NewClientOptions) (plugin.Client, error) {
	var spec Spec
	if len(specBytes) > 0 {
		dec := json.NewDecoder(bytes.NewReader(specBytes))
		dec.DisallowUnknownFields()
		if err := dec.Decode(&spec); err != nil {
			return nil, fmt.Errorf("invalid fixture spec: %w", err)
		}
	}
	if spec.ItemCount == 0 {
		spec.ItemCount = 25
	}
	if spec.Greeting == "" {
		spec.Greeting = "hello"
	}
	c := &client{spec: spec, noConnection: opts.NoConnection,
		sched: scheduler.NewScheduler(scheduler.WithLogger(logger), scheduler.WithConcurrency(4))}
	c.tables = schema.Tables{c.itemsTable(), c.whoamiTable(), flakyTable()}
	for _, t := range c.tables {
		schema.AddCqIDs(t)
	}
	if err := setParents(c.tables, nil); err != nil {
		return nil, err
	}
	return c, nil
}

func setParents(tables schema.Tables, parent *schema.Table) error {
	for _, t := range tables {
		t.Parent = parent
		if err := setParents(t.Relations, t); err != nil {
			return err
		}
	}
	return nil
}

func main() {
	p := plugin.NewPlugin("fixture", Version, newClient,
		plugin.WithKind("source"), plugin.WithTeam("query-farm"), plugin.WithJSONSchema(specSchema))
	if err := serve.Plugin(p).Serve(context.Background()); err != nil {
		log.Fatal(err)
	}
}
