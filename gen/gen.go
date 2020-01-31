package gen

import (
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"strings"
	"text/template"

	"github.com/Masterminds/sprig"
	"github.com/jackc/pgtype"
	"github.com/jackc/pgx/v4"
	_ "github.com/jackc/pgxrecord/gen/statik"
	"github.com/rakyll/statik/fs"
)

var tmpl *template.Template

var pgNullToGoTypeMap map[pgtype.OID]string
var pgNotNullToGoTypeMap map[pgtype.OID]string

func init() {
	statikFS, err := fs.New()
	if err != nil {
		panic(err)
	}

	file, err := statikFS.Open("/pgxrecord.tmpl")
	if err != nil {
		panic(err)
	}
	defer file.Close()

	tmplBytes, err := ioutil.ReadAll(file)
	if err != nil {
		panic(err)
	}

	tmpl = template.Must(template.New("pgxrecord").Funcs(sprig.TxtFuncMap()).Parse(string(tmplBytes)))

	pgNullToGoTypeMap = map[pgtype.OID]string{
		pgtype.ACLItemArrayOID:     "pgtype.ACLItemArray",
		pgtype.BoolArrayOID:        "pgtype.BoolArray",
		pgtype.BPCharArrayOID:      "pgtype.BPCharArray",
		pgtype.ByteaArrayOID:       "pgtype.ByteaArray",
		pgtype.CIDRArrayOID:        "pgtype.CIDRArray",
		pgtype.DateArrayOID:        "pgtype.DateArray",
		pgtype.Float4ArrayOID:      "pgtype.Float4Array",
		pgtype.Float8ArrayOID:      "pgtype.Float8Array",
		pgtype.InetArrayOID:        "pgtype.InetArray",
		pgtype.Int2ArrayOID:        "pgtype.Int2Array",
		pgtype.Int4ArrayOID:        "pgtype.Int4Array",
		pgtype.Int8ArrayOID:        "pgtype.Int8Array",
		pgtype.NumericArrayOID:     "pgtype.NumericArray",
		pgtype.TextArrayOID:        "pgtype.TextArray",
		pgtype.TimestampArrayOID:   "pgtype.TimestampArray",
		pgtype.TimestamptzArrayOID: "pgtype.TimestamptzArray",
		pgtype.UUIDArrayOID:        "pgtype.UUIDArray",
		pgtype.VarcharArrayOID:     "pgtype.VarcharArray",
		pgtype.ACLItemOID:          "pgtype.ACLItem",
		pgtype.BitOID:              "pgtype.Bit",
		pgtype.BoolOID:             "pgtype.Bool",
		pgtype.BoxOID:              "pgtype.Box",
		pgtype.BPCharOID:           "pgtype.BPChar",
		pgtype.ByteaOID:            "pgtype.Bytea",
		pgtype.QCharOID:            "pgtype.QChar",
		pgtype.CIDOID:              "pgtype.CID",
		pgtype.CIDROID:             "pgtype.CIDR",
		pgtype.CircleOID:           "pgtype.Circle",
		pgtype.DateOID:             "pgtype.Date",
		pgtype.DaterangeOID:        "pgtype.Daterange",
		pgtype.Float4OID:           "pgtype.Float4",
		pgtype.Float8OID:           "pgtype.Float8",
		pgtype.InetOID:             "pgtype.Inet",
		pgtype.Int2OID:             "pgtype.Int2",
		pgtype.Int4OID:             "pgtype.Int4",
		pgtype.Int4rangeOID:        "pgtype.Int4range",
		pgtype.Int8OID:             "pgtype.Int8",
		pgtype.Int8rangeOID:        "pgtype.Int8range",
		pgtype.IntervalOID:         "pgtype.Interval",
		pgtype.JSONOID:             "pgtype.JSON",
		pgtype.JSONBOID:            "pgtype.JSONB",
		pgtype.LineOID:             "pgtype.Line",
		pgtype.LsegOID:             "pgtype.Lseg",
		pgtype.MacaddrOID:          "pgtype.Macaddr",
		pgtype.NameOID:             "pgtype.Name",
		pgtype.NumericOID:          "shopspring.Numeric",
		pgtype.NumrangeOID:         "pgtype.Numrange",
		pgtype.OIDOID:              "pgtype.OIDValue",
		pgtype.PathOID:             "pgtype.Path",
		pgtype.PointOID:            "pgtype.Point",
		pgtype.PolygonOID:          "pgtype.Polygon",
		pgtype.RecordOID:           "pgtype.Record",
		pgtype.TextOID:             "pgtype.Text",
		pgtype.TIDOID:              "pgtype.TID",
		pgtype.TimeOID:             "pgtype.Time",
		pgtype.TimestampOID:        "pgtype.Timestamp",
		pgtype.TimestamptzOID:      "pgtype.Timestamptz",
		pgtype.TsrangeOID:          "pgtype.Tsrange",
		pgtype.TstzrangeOID:        "pgtype.Tstzrange",
		pgtype.UnknownOID:          "pgtype.Unknown",
		pgtype.UUIDOID:             "pgtypeuuid.UUID",
		pgtype.VarbitOID:           "pgtype.Varbit",
		pgtype.VarcharOID:          "pgtype.Varchar",
		pgtype.XIDOID:              "pgtype.XID",
	}

	pgNotNullToGoTypeMap = map[pgtype.OID]string{
		pgtype.BoolOID:        "bool",
		pgtype.ByteaOID:       "[]byte",
		pgtype.DateOID:        "time.Time",
		pgtype.Float4OID:      "float32",
		pgtype.Float8OID:      "float64",
		pgtype.Int2OID:        "int16",
		pgtype.Int4OID:        "int32",
		pgtype.Int8OID:        "int64",
		pgtype.MacaddrOID:     "net.HardwareAddr",
		pgtype.NumericOID:     "decimal.Decimal",
		pgtype.NumrangeOID:    "pgtype.Numrange",
		pgtype.OIDOID:         "pgtype.OID",
		pgtype.TextOID:        "string",
		pgtype.TimestampOID:   "time.Time",
		pgtype.TimestamptzOID: "time.Time",
		pgtype.UUIDOID:        "uuid.UUID",
		pgtype.VarcharOID:     "string",
	}
}

type Column struct {
	ColumnName string

	FieldName   string
	FieldType   string
	ConvertType string `json:",omitempty"`

	PrimaryKey      bool `json:",omitempty"`
	Select          bool `json:",omitempty"`
	Insert          bool `json:",omitempty"`
	InsertReturning bool `json:",omitempty"`
	Update          bool `json:",omitempty"`
}

type Table struct {
	SchemaName string
	TableName  string

	PackageName  string
	StructName   string
	ReceiverName string

	Columns []*Column
}

func filterColumns(input []*Column, f func(*Column) bool) []*Column {
	columns := []*Column{}
	for _, c := range input {
		if f(c) {
			columns = append(columns, c)
		}
	}

	return columns
}

func (t *Table) PrimaryKeyColumns() []*Column {
	return filterColumns(t.Columns, func(c *Column) bool { return c.PrimaryKey })
}

func (t *Table) SelectColumns() []*Column {
	return filterColumns(t.Columns, func(c *Column) bool { return c.Select })
}

func (t *Table) InsertColumns() []*Column {
	return filterColumns(t.Columns, func(c *Column) bool { return c.Insert })
}

func (t *Table) InsertReturningColumns() []*Column {
	return filterColumns(t.Columns, func(c *Column) bool { return c.InsertReturning })
}

func (t *Table) UpdateColumns() []*Column {
	return filterColumns(t.Columns, func(c *Column) bool { return c.Update })
}

func (t *Table) Generate(w io.Writer) error {
	return tmpl.Execute(w, t)
}

type Queryer interface {
	Query(ctx context.Context, sql string, optionsAndArgs ...interface{}) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, optionsAndArgs ...interface{}) pgx.Row
}

func NewTableFromPgCatalog(ctx context.Context, db Queryer, schemaName, tableName string) (*Table, error) {
	var tableOID pgtype.OID

	if schemaName == "" {
		err := db.QueryRow(ctx, `select c.oid
from pg_catalog.pg_class c
  join pg_catalog.pg_namespace n on n.oid=c.relnamespace
where c.relname=$1
  and pg_catalog.pg_table_is_visible(c.oid)
limit 1`, tableName).Scan(&tableOID)
		if err != nil {
			return nil, fmt.Errorf("failed to find table OID: %v", err)
		}
	} else {
		err := db.QueryRow(ctx, `select c.oid
from pg_catalog.pg_class c
	join pg_catalog.pg_namespace n on n.oid=c.relnamespace
where c.relname=$1
  and n.nspname=$2
  and pg_catalog.pg_table_is_visible(c.oid)
limit 1`, tableName, schemaName).Scan(&tableOID)
		if err != nil {
			return nil, fmt.Errorf("failed to find table OID: %v", err)
		}
	}

	t := &Table{
		SchemaName: schemaName,
		TableName:  tableName,
	}

	rows, err := db.Query(ctx, `select attname, atttypid, attnotnull,
  coalesce((
    select true
    from pg_catalog.pg_index
    where pg_index.indrelid=pg_attribute.attrelid
      and pg_index.indisprimary
      and pg_attribute.attnum = any(pg_index.indkey)
  ), false) as isprimary
from pg_catalog.pg_attribute
where attrelid=$1
	and attnum > 0
	and not attisdropped
order by attnum`, tableOID)
	if err != nil {
		return nil, fmt.Errorf("failed to find columns: %v", err)
	}

	for rows.Next() {
		var attname string
		var atttypid pgtype.OID
		var attnotnull bool
		var isprimary bool

		err := rows.Scan(&attname, &atttypid, &attnotnull, &isprimary)
		if err != nil {
			return nil, fmt.Errorf("failed to find columns: %v", err)
		}

		c := &Column{
			ColumnName:      attname,
			FieldName:       ToFieldName(attname),
			PrimaryKey:      isprimary,
			Select:          true,
			Insert:          !isprimary,
			InsertReturning: isprimary,
			Update:          !isprimary,
		}

		if t, ok := pgNotNullToGoTypeMap[atttypid]; attnotnull && ok {
			c.FieldType = t
		} else if t, ok := pgNullToGoTypeMap[atttypid]; ok {
			c.FieldType = t
		} else {
			c.FieldType = "string"
		}

		t.Columns = append(t.Columns, c)
	}

	if rows.Err() != nil {
		return nil, fmt.Errorf("failed to find columns: %v", err)
	}

	return t, nil

}

func ToUpperCamelCase(s string) string {
	var t string

	words := strings.Split(s, "_")
	for _, w := range words {
		t += strings.ToUpper(w[:1]) + w[1:]
	}

	return t
}

func ToFieldName(s string) string {
	s = ToUpperCamelCase(s)
	s = strings.ReplaceAll(s, "Id", "ID")
	return s
}
