// Package pgxrecord is a tiny library for CRUD operations.
package pgxrecord

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5"
)

// DB is the interface pgxrecord uses to access the database. It is satisfied by *pgx.Conn, pgx.Tx, *pgxpool.Pool, etc.
type DB interface {
	Query(ctx context.Context, sql string, optionsAndArgs ...interface{}) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, optionsAndArgs ...interface{}) pgx.Row
}

// Column represents a column in a table.
type Column struct {
	Name       string
	quotedName string
	OID        uint32
	NotNull    bool
	PrimaryKey bool
}

// Table represents a table in a database. It must not be mutated after Finalize is called.
type Table struct {
	Name    pgx.Identifier
	Columns []*Column

	finalized           bool
	quotedQualifiedName string
	quotedName          string
	selectQuery         string
	selectByPKQuery     string
	pkWhereClause       string
	pkIndexes           []int
	nameToColumnIndex   map[string]int
}

// Record represents a row from a table in the database.
type Record struct {
	table              *Table
	originalAttributes []any
	attributes         []any
	assigned           []bool
}

// LoadAllColumns queries the database for the table columns. It must not be called after Finalize.
func (t *Table) LoadAllColumns(ctx context.Context, db DB) error {
	if t.finalized {
		panic("cannot call after table finalized")
	}

	var tableOID uint32

	{
		var rows pgx.Rows

		if len(t.Name) == 1 {
			rows, _ = db.Query(ctx, `select c.oid
	from pg_catalog.pg_class c
	where c.relname=$1
		and pg_catalog.pg_table_is_visible(c.oid)
	limit 1`,
				t.Name[0],
			)
		} else if len(t.Name) == 2 {
			rows, _ = db.Query(ctx, `select c.oid
	from pg_catalog.pg_class c
		join pg_catalog.pg_namespace n on n.oid=c.relnamespace
	where c.relname=$1
		and n.nspname=$2
		and pg_catalog.pg_table_is_visible(c.oid)
	limit 1`,
				t.Name[1], t.Name[0],
			)
		}

		var err error
		tableOID, err = pgx.CollectOneRow(rows, pgx.RowTo[uint32])
		if err != nil {
			return fmt.Errorf("failed to find table OID for %v: %v", t.Name, err)
		}
	}

	rows, _ := db.Query(ctx, `select attname, atttypid, attnotnull,
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
	var err error
	t.Columns, err = pgx.CollectRows(rows, pgx.RowToAddrOfStructByPos[Column])
	if err != nil {
		return fmt.Errorf("failed to find columns: %v", err)
	}

	return nil
}

// Finalize finishes the table initialization.
func (t *Table) Finalize() {
	if t.finalized {
		panic("cannot call after table finalized")
	}

	t.finalized = true

	t.quotedQualifiedName = t.Name.Sanitize()
	t.quotedName = pgx.Identifier{t.Name[len(t.Name)-1]}.Sanitize()
	for i, c := range t.Columns {
		c.quotedName = pgx.Identifier{c.Name}.Sanitize()
		if c.PrimaryKey {
			t.pkIndexes = append(t.pkIndexes, i)
		}
	}

	t.pkWhereClause = t.buildPKWhereClause()
	t.selectQuery = t.buildSelectQuery()
	t.selectByPKQuery = t.selectQuery + " " + t.pkWhereClause
	t.nameToColumnIndex = buildNameToColumnIndex(t.Columns)
}

func (t *Table) buildSelectQuery() string {
	b := &strings.Builder{}
	b.WriteString("select ")
	for i := range t.Columns {
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteString(t.quotedName)
		b.WriteByte('.')
		b.WriteString(t.Columns[i].quotedName)
	}
	b.WriteString(" from ")
	b.WriteString(t.quotedQualifiedName)

	return b.String()
}

func (t *Table) buildPKWhereClause() string {
	b := &strings.Builder{}
	b.WriteString("where ")
	for i := range t.pkIndexes {
		if i > 0 {
			b.WriteString(" and ")
		}
		c := t.Columns[t.pkIndexes[i]]
		b.WriteString(c.quotedName)
		b.WriteString(" = $")
		b.WriteString(strconv.FormatInt(int64(i+1), 10))
	}

	return b.String()
}

func (t *Table) buildSelectByPKQuery() string {
	b := &strings.Builder{}
	b.WriteString(t.selectQuery)

	for i := range t.Columns {
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteString(t.quotedName)
		b.WriteByte('.')
		b.WriteString(t.Columns[i].quotedName)
	}
	b.WriteString(" from ")
	b.WriteString(t.quotedQualifiedName)

	return b.String()
}

func buildNameToColumnIndex(columns []*Column) map[string]int {
	m := make(map[string]int, len(columns))
	for i := range columns {
		m[columns[i].Name] = i
	}
	return m
}

// NewRecord creates an empty Record. It must be called after Finalize.
func (t *Table) NewRecord() *Record {
	if !t.finalized {
		panic("cannot call until table finalized")
	}

	record := &Record{
		table:      t,
		attributes: make([]any, len(t.Columns)),
		assigned:   make([]bool, len(t.Columns)),
	}

	return record
}

// SelectQuery returns the SQL query to select all rows from the table. It must be called after Finalize.
func (t *Table) SelectQuery() string {
	if !t.finalized {
		panic("cannot call until table finalized")
	}

	return t.selectQuery
}

// FindByPK finds a record by primary key. It must be called after Finalize.
func (t *Table) FindByPK(ctx context.Context, db DB, pk ...any) (*Record, error) {
	if !t.finalized {
		panic("cannot call until table finalized")
	}

	rows, _ := db.Query(ctx, t.selectByPKQuery, pk...)
	record, err := pgx.CollectOneRow(rows, t.RowToRecord)
	if err != nil {
		return nil, err
	}

	return record, nil
}

// RowToRecord is a pgx.RowToFunc that returns a *Record. It must be called after Finalize.
func (t *Table) RowToRecord(row pgx.CollectableRow) (*Record, error) {
	if !t.finalized {
		panic("cannot call until table finalized")
	}

	record := t.NewRecord()

	ptrsToAttributes := make([]any, len(record.attributes))
	for i := range record.attributes {
		ptrsToAttributes[i] = &record.attributes[i]
	}

	err := row.Scan(ptrsToAttributes...)
	if err != nil {
		return nil, err
	}

	record.originalAttributes = make([]any, len(record.attributes))
	copy(record.originalAttributes, record.attributes)

	return record, nil
}

// Set sets a attribute to a value.
func (r *Record) Set(attribute string, value any) error {
	idx, ok := r.table.nameToColumnIndex[attribute]
	if !ok {
		return fmt.Errorf("attribute %q is not found", attribute)
	}

	r.attributes[idx] = value
	r.assigned[idx] = true

	return nil
}

// MustSet sets a attribute to a value. It panics on failure.
func (r *Record) MustSet(attribute string, value any) {
	err := r.Set(attribute, value)
	if err != nil {
		panic(err.Error())
	}
}

// Get returns the value of attribute.
func (r *Record) Get(attribute string) (any, error) {
	idx, ok := r.table.nameToColumnIndex[attribute]
	if !ok {
		return nil, fmt.Errorf("attribute %q is not found", attribute)
	}

	return r.attributes[idx], nil
}

// MustGet returns the value of attribute. It panics on failure.
func (r *Record) MustGet(attribute string) any {
	value, err := r.Get(attribute)
	if err != nil {
		panic(err.Error())
	}
	return value
}

// SetAttributes sets attributes.
func (r *Record) SetAttributes(attributes map[string]any) error {
	for k, v := range attributes {
		idx, ok := r.table.nameToColumnIndex[k]
		if !ok {
			return fmt.Errorf("attribute %q is not found", k)
		}

		r.attributes[idx] = v
		r.assigned[idx] = true
	}

	return nil
}

// Attributes returns all attributes.
func (r *Record) Attributes() map[string]any {
	m := make(map[string]any, len(r.attributes))
	for i := range r.table.Columns {
		m[r.table.Columns[i].Name] = r.attributes[i]
	}

	return m
}

// Save saves the record using db.
func (r *Record) Save(ctx context.Context, db DB) error {
	if r.originalAttributes == nil {
		return r.insert(ctx, db)
	} else {
		return r.update(ctx, db)
	}
}

func (r *Record) insert(ctx context.Context, db DB) error {
	b := &strings.Builder{}
	b.WriteString("insert into ")
	b.WriteString(r.table.quotedQualifiedName)
	b.WriteString(" (")

	assignedCount := 0
	for i := range r.assigned {
		if r.assigned[i] {
			if assignedCount > 0 {
				b.WriteString(", ")
			}
			assignedCount++
			b.WriteString(r.table.Columns[i].quotedName)
		}
	}

	b.WriteString(") values (")
	args := make([]any, assignedCount)
	assignedCount = 0
	for i := range r.assigned {
		if r.assigned[i] {
			if assignedCount > 0 {
				b.WriteString(", ")
			}
			args[assignedCount] = r.attributes[i]
			assignedCount++
			b.WriteByte('$')
			b.WriteString(strconv.FormatInt(int64(assignedCount), 10))
		}
	}

	b.WriteString(") returning ")
	for i, c := range r.table.Columns {
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteString(c.quotedName)
	}

	ptrsToAttributes := make([]any, len(r.attributes))
	for i := range r.attributes {
		ptrsToAttributes[i] = &r.attributes[i]
	}

	err := db.QueryRow(ctx, b.String(), args...).Scan(ptrsToAttributes...)
	if err != nil {
		return err
	}

	r.originalAttributes = make([]any, len(r.attributes))
	copy(r.originalAttributes, r.attributes)
	for i := range r.assigned {
		r.assigned[i] = false
	}

	return nil
}

func (r *Record) update(ctx context.Context, db DB) error {
	b := &strings.Builder{}
	b.WriteString("update ")
	b.WriteString(r.table.quotedQualifiedName)
	b.WriteString(" set ")

	args := make([]any, 0, len(r.attributes))
	for _, pkIdx := range r.table.pkIndexes {
		args = append(args, r.attributes[pkIdx])
	}

	assignedCount := 0
	for i := range r.assigned {
		if r.assigned[i] {
			if assignedCount > 0 {
				b.WriteString(", ")
			}
			args = append(args, r.attributes[i])
			assignedCount++
			b.WriteString(r.table.Columns[i].quotedName)
			b.WriteString(" = $")
			b.WriteString(strconv.FormatInt(int64(len(args)), 10))
		}
	}

	b.WriteByte(' ')
	b.WriteString(r.table.pkWhereClause)

	b.WriteString(" returning ")
	for i, c := range r.table.Columns {
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteString(c.quotedName)
	}

	ptrsToAttributes := make([]any, len(r.attributes))
	for i := range r.attributes {
		ptrsToAttributes[i] = &r.attributes[i]
	}

	err := db.QueryRow(ctx, b.String(), args...).Scan(ptrsToAttributes...)
	if err != nil {
		return err
	}

	r.originalAttributes = make([]any, len(r.attributes))
	copy(r.originalAttributes, r.attributes)
	for i := range r.assigned {
		r.assigned[i] = false
	}

	return nil
}
