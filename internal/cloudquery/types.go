// Mapping CloudQuery's Arrow schemas to the columns DuckDB sees.
//
// CloudQuery plugins already speak Arrow, so most columns pass through
// untouched (their buffers are handed to DuckDB as they arrived). Four
// CloudQuery extension types need a mapping:
//
//	CloudQuery type     storage               DuckDB
//	uuid                fixed_size_binary(16) UUID     (arrow.uuid; same buffers)
//	json                binary (JSON text)    JSON     (arrow.json over utf8; same buffers)
//	inet                binary (CIDR text)    VARCHAR  ('10.0.0.1/32'; same buffers)
//	mac                 binary (6..20 bytes)  VARCHAR  ('02:00:00:00:00:01'; converted)
//
// Lists, structs and maps are mapped element by element, so list<uuid>
// becomes UUID[]. timestamp[us, UTC] is TIMESTAMP WITH TIME ZONE. Any other
// extension type is passed as its storage type.

package cloudquery

import (
	"fmt"
	"net"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/extensions"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// CloudQuery metadata keys.
const (
	mdTableName        = "cq:table_name"
	mdTableDescription = "cq:table_description"
	mdTableTitle       = "cq:table_title"
	mdTableDependsOn   = "cq:table_depends_on"
	mdTableIncremental = "cq:extension:incremental"
	mdTablePaid        = "cq:table_paid"
	mdTablePermissions = "cq:table_permissions_needed"
	mdTableSensitive   = "cq:table_sensitive_columns"
	mdPrimaryKey       = "cq:extension:primary_key"
	mdUnique           = "cq:extension:unique"
	mdIncremental      = "cq:extension:incremental"
	mdPKComponent      = "cq:extension:primary_key_component"
	mdExtensionName    = "ARROW:extension:name"
)

var (
	uuidType = extensions.NewUUIDType()
	jsonType = mustJSONType()
)

func mustJSONType() arrow.DataType {
	t, err := extensions.NewJSONType(arrow.BinaryTypes.String)
	if err != nil {
		panic(err)
	}
	return t
}

func mdValue(md arrow.Metadata, key string) string {
	if i := md.FindKey(key); i >= 0 {
		return md.Values()[i]
	}
	return ""
}

func tableName(s *arrow.Schema) string { return mdValue(s.Metadata(), mdTableName) }

// cqExtension names the CloudQuery extension type of a value, or "".
func cqExtension(t arrow.DataType, md arrow.Metadata) string {
	if et, ok := t.(arrow.ExtensionType); ok {
		return et.ExtensionName()
	}
	return mdValue(md, mdExtensionName)
}

func storageOf(t arrow.DataType) arrow.DataType {
	if et, ok := t.(arrow.ExtensionType); ok {
		return et.StorageType()
	}
	return t
}

// mapType returns the DuckDB-facing type of a CloudQuery value type.
func mapType(t arrow.DataType, md arrow.Metadata) arrow.DataType {
	switch cqExtension(t, md) {
	case "uuid":
		return uuidType
	case "json":
		return jsonType
	case "inet", "mac":
		return arrow.BinaryTypes.String
	case "":
	default:
		return mapType(storageOf(t), arrow.Metadata{})
	}
	switch x := t.(type) {
	case *arrow.ListType:
		return arrow.ListOfField(mapField(x.ElemField()))
	case *arrow.LargeListType:
		return arrow.LargeListOfField(mapField(x.ElemField()))
	case *arrow.FixedSizeListType:
		return arrow.FixedSizeListOfField(x.Len(), mapField(x.ElemField()))
	case *arrow.StructType:
		fields := make([]arrow.Field, x.NumFields())
		for i, f := range x.Fields() {
			fields[i] = mapField(f)
		}
		return arrow.StructOf(fields...)
	case *arrow.MapType:
		m := arrow.MapOfFields(mapField(x.KeyField()), mapField(x.ItemField()))
		m.KeysSorted = x.KeysSorted
		return m
	}
	return t
}

func mapField(f arrow.Field) arrow.Field {
	return arrow.Field{Name: f.Name, Type: mapType(f.Type, f.Metadata), Nullable: f.Nullable}
}

// outputField is the DuckDB column for a plugin table field, with a comment.
func outputField(f arrow.Field) arrow.Field {
	out := mapField(f)
	out.Metadata = arrow.NewMetadata([]string{"comment"}, []string{columnComment(f)})
	return out
}

// outputSchema is the DuckDB-facing schema of a plugin table.
func outputSchema(s *arrow.Schema) *arrow.Schema {
	fields := make([]arrow.Field, s.NumFields())
	for i, f := range s.Fields() {
		fields[i] = outputField(f)
	}
	return arrow.NewSchema(fields, nil)
}

func columnComment(f arrow.Field) string {
	var flags []string
	if mdValue(f.Metadata, mdPrimaryKey) == "true" {
		flags = append(flags, "primary key")
	}
	if mdValue(f.Metadata, mdPKComponent) == "true" {
		flags = append(flags, "primary key component")
	}
	if mdValue(f.Metadata, mdUnique) == "true" {
		flags = append(flags, "unique")
	}
	if mdValue(f.Metadata, mdIncremental) == "true" {
		flags = append(flags, "incremental key")
	}
	desc := ""
	switch f.Name {
	case "_cq_id":
		desc = "CloudQuery row id (deterministic: derived from the primary key)."
	case "_cq_parent_id":
		desc = "The _cq_id of the parent row this row was fetched under (NULL for top-level tables)."
	case "_cq_client_id":
		desc = "The multiplexed plugin client (account, region, ...) that fetched the row."
	default:
		desc = "Plugin column " + f.Name + " (CloudQuery type " + cqTypeName(f.Type, f.Metadata) + ")."
	}
	if len(flags) > 0 {
		desc += " " + strings.ToUpper(flags[0][:1]) + strings.Join(flags, ", ")[1:] + "."
	}
	return desc
}

// cqTypeName is CloudQuery's spelling of a column type.
func cqTypeName(t arrow.DataType, md arrow.Metadata) string {
	if ext := cqExtension(t, md); ext != "" {
		return ext
	}
	switch x := t.(type) {
	case *arrow.ListType:
		return "list<" + cqTypeName(x.Elem(), x.ElemField().Metadata) + ">"
	case *arrow.LargeListType:
		return "large_list<" + cqTypeName(x.Elem(), x.ElemField().Metadata) + ">"
	}
	return t.String()
}

// duckdbTypeName is DuckDB's spelling of an output column type.
func duckdbTypeName(t arrow.DataType) string {
	if et, ok := t.(arrow.ExtensionType); ok {
		switch et.ExtensionName() {
		case "arrow.uuid":
			return "UUID"
		case "arrow.json":
			return "JSON"
		}
		return duckdbTypeName(et.StorageType())
	}
	switch x := t.(type) {
	case *arrow.BooleanType:
		return "BOOLEAN"
	case *arrow.Int8Type:
		return "TINYINT"
	case *arrow.Int16Type:
		return "SMALLINT"
	case *arrow.Int32Type:
		return "INTEGER"
	case *arrow.Int64Type:
		return "BIGINT"
	case *arrow.Uint8Type:
		return "UTINYINT"
	case *arrow.Uint16Type:
		return "USMALLINT"
	case *arrow.Uint32Type:
		return "UINTEGER"
	case *arrow.Uint64Type:
		return "UBIGINT"
	case *arrow.Float16Type, *arrow.Float32Type:
		return "FLOAT"
	case *arrow.Float64Type:
		return "DOUBLE"
	case *arrow.StringType, *arrow.LargeStringType, *arrow.StringViewType:
		return "VARCHAR"
	case *arrow.BinaryType, *arrow.LargeBinaryType, *arrow.FixedSizeBinaryType, *arrow.BinaryViewType:
		return "BLOB"
	case *arrow.TimestampType:
		if x.TimeZone != "" {
			return "TIMESTAMP WITH TIME ZONE"
		}
		return "TIMESTAMP"
	case *arrow.Date32Type, *arrow.Date64Type:
		return "DATE"
	case *arrow.Time32Type, *arrow.Time64Type:
		return "TIME"
	case *arrow.DurationType, *arrow.MonthDayNanoIntervalType, *arrow.MonthIntervalType, *arrow.DayTimeIntervalType:
		return "INTERVAL"
	case *arrow.Decimal128Type:
		return fmt.Sprintf("DECIMAL(%d,%d)", x.Precision, x.Scale)
	case *arrow.ListType:
		return duckdbTypeName(x.Elem()) + "[]"
	case *arrow.LargeListType:
		return duckdbTypeName(x.Elem()) + "[]"
	case *arrow.FixedSizeListType:
		return fmt.Sprintf("%s[%d]", duckdbTypeName(x.Elem()), x.Len())
	case *arrow.StructType:
		parts := make([]string, x.NumFields())
		for i, f := range x.Fields() {
			parts[i] = fmt.Sprintf("%q %s", f.Name, duckdbTypeName(f.Type))
		}
		return "STRUCT(" + strings.Join(parts, ", ") + ")"
	case *arrow.MapType:
		return "MAP(" + duckdbTypeName(x.KeyType()) + ", " + duckdbTypeName(x.ItemType()) + ")"
	case *arrow.NullType:
		return "NULL"
	}
	return strings.ToUpper(t.String())
}

// needsConversion reports whether a value type's buffers need any change.
func needsConversion(t arrow.DataType, md arrow.Metadata) bool {
	if cqExtension(t, md) != "" {
		return true
	}
	switch x := t.(type) {
	case arrow.ListLikeType:
		return needsConversion(x.Elem(), x.ElemField().Metadata)
	case *arrow.StructType:
		for _, f := range x.Fields() {
			if needsConversion(f.Type, f.Metadata) {
				return true
			}
		}
	}
	return false
}

// convertColumn returns the DuckDB-facing array for a plugin column. Columns
// that need no change are returned as is (retained); uuid, json and inet keep
// their buffers; only mac values are rewritten.
func convertColumn(mem memory.Allocator, arr arrow.Array, f arrow.Field) (arrow.Array, error) {
	if !needsConversion(f.Type, f.Metadata) {
		arr.Retain()
		return arr, nil
	}
	d, err := convertData(mem, arr.Data(), f.Type, f.Metadata)
	if err != nil {
		return nil, fmt.Errorf("column %s: %w", f.Name, err)
	}
	defer d.Release()
	return array.MakeFromData(d), nil
}

// convertData maps one ArrayData (and its children) to the output type. The
// result is a new reference the caller releases.
func convertData(mem memory.Allocator, d arrow.ArrayData, t arrow.DataType, md arrow.Metadata) (arrow.ArrayData, error) {
	out := mapType(t, md)
	switch ext := cqExtension(t, md); ext {
	case "uuid", "json", "inet":
		if ext == "uuid" {
			if fsb, ok := storageOf(t).(*arrow.FixedSizeBinaryType); !ok || fsb.ByteWidth != 16 {
				return nil, fmt.Errorf("uuid storage is %s, not fixed_size_binary(16)", storageOf(t))
			}
		} else if _, ok := storageOf(t).(*arrow.BinaryType); !ok {
			if _, ok := storageOf(t).(*arrow.StringType); !ok {
				return nil, fmt.Errorf("%s storage is %s, not binary", ext, storageOf(t))
			}
		}
		// Same buffers, new type: binary and utf8 share a layout.
		return array.NewData(out, d.Len(), d.Buffers(), nil, d.NullN(), d.Offset()), nil
	case "mac":
		sd := array.NewData(storageOf(t), d.Len(), d.Buffers(), nil, d.NullN(), d.Offset())
		src := array.MakeFromData(sd)
		sd.Release()
		defer src.Release()
		bin, ok := src.(*array.Binary)
		if !ok {
			return nil, fmt.Errorf("mac storage is %s, not binary", src.DataType())
		}
		b := array.NewStringBuilder(mem)
		defer b.Release()
		b.Reserve(bin.Len())
		for i := 0; i < bin.Len(); i++ {
			if bin.IsNull(i) {
				b.AppendNull()
				continue
			}
			b.Append(net.HardwareAddr(bin.Value(i)).String())
		}
		a := b.NewArray()
		defer a.Release()
		ad := a.Data()
		ad.Retain()
		return ad, nil
	case "":
	default:
		sd := array.NewData(storageOf(t), d.Len(), d.Buffers(), d.Children(), d.NullN(), d.Offset())
		defer sd.Release()
		return convertData(mem, sd, storageOf(t), arrow.Metadata{})
	}
	switch x := t.(type) {
	case arrow.ListLikeType:
		child, err := convertData(mem, d.Children()[0], x.Elem(), x.ElemField().Metadata)
		if err != nil {
			return nil, err
		}
		defer child.Release()
		return array.NewData(out, d.Len(), d.Buffers(), []arrow.ArrayData{child}, d.NullN(), d.Offset()), nil
	case *arrow.StructType:
		children := make([]arrow.ArrayData, x.NumFields())
		for i, f := range x.Fields() {
			c, err := convertData(mem, d.Children()[i], f.Type, f.Metadata)
			if err != nil {
				return nil, err
			}
			defer c.Release()
			children[i] = c
		}
		return array.NewData(out, d.Len(), d.Buffers(), children, d.NullN(), d.Offset()), nil
	}
	d.Retain()
	return d, nil
}
