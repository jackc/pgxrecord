// Package pgxrecord is a tiny library for CRUD operations.
package pgxrecord

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5"
)

var errTooManyRows = fmt.Errorf("too many rows")

// DB is the interface pgxrecord uses to access the database. It is satisfied by *pgx.Conn, pgx.Tx, *pgxpool.Pool, etc.
type DB interface {
	Query(ctx context.Context, sql string, optionsAndArgs ...interface{}) (pgx.Rows, error)
}

// Column represents a column in a table.
type Column struct {
	Name       string
	quotedName string
	OID        uint32
	NotNull    bool
	PrimaryKey bool
}

// Table represents a table in a database. It must not be mutated after any method other than LoadAllColumns is called.
type Table struct {
	Name    pgx.Identifier
	Columns []*Column

	// Normalize is called before a record is saved. It is useful for normalizing data before it is saved. For example,
	// it can be used to trim strings. If Normalize returns an error then the save is aborted.
	Normalize func(ctx context.Context, db DB, table *Table, record *Record) error

	// Validate is called before a record is saved. If Validate returns an error then the save is aborted. A
	// *ValidationErrors should be returned if validation fails. Any other error indicates an error occurred while
	// validating. For example, a database query for a uniqueness check failed because of a broken database connection.
	Validate func(ctx context.Context, db DB, table *Table, record *Record) error

	finalized           bool
	quotedQualifiedName string
	quotedName          string
	selectQuery         string
	selectByPKQuery     string
	pkWhereClause       string
	returningClause     string
	pkIndexes           []int
	nameToColumnIndex   map[string]int
	validationErrors    *ValidationErrors
}

// Record represents a row from a table in the database.
type Record struct {
	table              *Table
	originalAttributes []any
	attributes         []any
	assigned           []bool
}

// LoadAllColumns queries the database for the table columns. It must not be called after any other method has been
// called.
func (t *Table) LoadAllColumns(ctx context.Context, db DB) error {
	if t.finalized {
		return fmt.Errorf("cannot call after table finalized")
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
			return fmt.Errorf("pgxrecord.Table (%s): LoadAllColumns: failed to find table OID: %v", t.Name.Sanitize(), err)
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
		return fmt.Errorf("pgxrecord.Table (%s): LoadAllColumns: failed to find columns: %v", t.Name.Sanitize(), err)
	}

	return nil
}

// finalize finishes the table initialization.
func (t *Table) finalize() {
	if t.finalized {
		panic("BUG: cannot call after table finalized")
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
	t.returningClause = t.buildReturningClause()
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

func (t *Table) buildReturningClause() string {
	b := &strings.Builder{}
	b.WriteString("returning ")
	for i, c := range t.Columns {
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteString(c.quotedName)
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

// NewRecord creates an empty Record.
func (t *Table) NewRecord() *Record {
	if !t.finalized {
		t.finalize()
	}

	record := &Record{
		table:      t,
		attributes: make([]any, len(t.Columns)),
		assigned:   make([]bool, len(t.Columns)),
	}

	return record
}

// SelectQuery returns the SQL query to select all rows from the table.
func (t *Table) SelectQuery() string {
	if !t.finalized {
		t.finalize()
	}

	return t.selectQuery
}

// FindByPK finds a record by primary key.
func (t *Table) FindByPK(ctx context.Context, db DB, pk ...any) (*Record, error) {
	if !t.finalized {
		t.finalize()
	}

	rows, _ := db.Query(ctx, t.selectByPKQuery, pk...)
	record, err := pgx.CollectOneRow(rows, t.RowToRecord)
	if err != nil {
		return nil, fmt.Errorf("pgxrecord.Table (%s): FindByPK (%v): %w", t.quotedQualifiedName, pk, err)
	}

	return record, nil
}

// RowToRecord is a pgx.RowToFunc that returns a *Record.
func (t *Table) RowToRecord(row pgx.CollectableRow) (*Record, error) {
	if !t.finalized {
		t.finalize()
	}

	record := t.NewRecord()

	ptrsToAttributes := make([]any, len(record.attributes))
	for i := range record.attributes {
		ptrsToAttributes[i] = &record.attributes[i]
	}

	err := row.Scan(ptrsToAttributes...)
	if err != nil {
		return nil, fmt.Errorf("pgxrecord.Table (%s): RowToRecord: %w", t.quotedQualifiedName, err)
	}

	record.originalAttributes = make([]any, len(record.attributes))
	copy(record.originalAttributes, record.attributes)

	return record, nil
}

// Set sets an attribute to a value. It panics if attribute does not exist.
func (r *Record) Set(attribute string, value any) {
	idx, ok := r.table.nameToColumnIndex[attribute]
	if !ok {
		panic(fmt.Sprintf("pgxrecord.Record (%s): Set: attribute %q is not found", r.table.quotedQualifiedName, attribute))
	}

	r.attributes[idx] = value
	r.assigned[idx] = true
}

// Get returns the value of attribute. It panics if attribute does not exist.
func (r *Record) Get(attribute string) any {
	idx, ok := r.table.nameToColumnIndex[attribute]
	if !ok {
		panic(fmt.Sprintf("pgxrecord.Record (%s): Get: attribute %q is not found", r.table.quotedQualifiedName, attribute))
	}

	return r.attributes[idx]
}

// SetAttributes sets attributes. Ignores attributes that do not exist.
func (r *Record) SetAttributes(attributes map[string]any) {
	for k, v := range attributes {
		idx, ok := r.table.nameToColumnIndex[k]
		if ok {
			r.attributes[idx] = v
			r.assigned[idx] = true
		}
	}
}

// SetAttributesStrict sets attributes. Returns an error if any attributes do not exist.
func (r *Record) SetAttributesStrict(attributes map[string]any) error {
	for k, v := range attributes {
		idx, ok := r.table.nameToColumnIndex[k]
		if !ok {
			return fmt.Errorf("pgxrecord.Record (%s): Set: attribute %q is not found", r.table.quotedQualifiedName, k)
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
	r.table.validationErrors = nil

	if fn := r.table.Normalize; fn != nil {
		err := fn(ctx, db, r.table, r)
		if err != nil {
			return fmt.Errorf("pgxrecord.Record (%s): Save: %w", r.table.quotedQualifiedName, err)
		}
	}

	if fn := r.table.Validate; fn != nil {
		err := fn(ctx, db, r.table, r)
		if err != nil {
			var ve *ValidationErrors
			if errors.As(err, &ve) {
				r.table.validationErrors = ve
			}
			return fmt.Errorf("pgxrecord.Record (%s): Save: %w", r.table.quotedQualifiedName, err)
		}
	}

	var sql string
	var args []any

	if r.originalAttributes == nil {
		sql, args = r.insert(ctx, db)
	} else {
		sql, args = r.update(ctx, db)
	}

	ptrsToAttributes := make([]any, len(r.attributes))
	for i := range r.attributes {
		ptrsToAttributes[i] = &r.attributes[i]
	}

	err := queryRow(ctx, db, sql, args, ptrsToAttributes)
	if err != nil {
		return fmt.Errorf("pgxrecord.Record (%s): Save: %w", r.table.quotedQualifiedName, err)
	}

	r.originalAttributes = make([]any, len(r.attributes))
	copy(r.originalAttributes, r.attributes)
	for i := range r.assigned {
		r.assigned[i] = false
	}

	return nil
}

func (r *Record) insert(ctx context.Context, db DB) (string, []any) {
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

	b.WriteString(") ")
	b.WriteString(r.table.returningClause)

	return b.String(), args
}

func (r *Record) update(ctx context.Context, db DB) (string, []any) {
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

	b.WriteByte(' ')
	b.WriteString(r.table.returningClause)

	return b.String(), args
}

func (r *Record) Errors() *ValidationErrors {
	return r.table.validationErrors
}

// queryRow builds QueryRow-like functionality on top of DB. This allows pgxutil to have the convenience of QueryRow
// without needing it as part of the DB interface.
func queryRow(ctx context.Context, db DB, sql string, args []any, scanTargets []any) error {
	rows, err := db.Query(ctx, sql, args...)
	if err != nil {
		return err
	}
	defer rows.Close()

	if rows.Next() {
		rows.Scan(scanTargets...)
	} else {
		return pgx.ErrNoRows
	}

	if rows.Next() {
		return errTooManyRows
	}

	err = rows.Err()
	if err != nil {
		return err
	}

	return nil
}
