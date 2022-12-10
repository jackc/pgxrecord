// Package pgxrecord is a tiny library for CRUD operations.
package pgxrecord

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

var errTooManyRows = fmt.Errorf("too many rows")

// DB is the interface pgxrecord uses to access the database. It is satisfied by *pgx.Conn, pgx.Tx, *pgxpool.Pool, etc.
type DB interface {
	Query(ctx context.Context, sql string, optionsAndArgs ...interface{}) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, optionsAndArgs ...interface{}) pgx.Row
	Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error)
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
	returningClause     string
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

	err := db.QueryRow(ctx, sql, args...).Scan(ptrsToAttributes...)
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

// Select executes sql with args on db and returns the []T produced by scanFn.
func Select[T any](ctx context.Context, db DB, sql string, args []any, scanFn pgx.RowToFunc[T]) ([]T, error) {
	rows, _ := db.Query(ctx, sql, args...)
	collectedRows, err := pgx.CollectRows(rows, scanFn)
	if err != nil {
		return nil, err
	}

	return collectedRows, nil
}

// SelectRow executes sql with args on db and returns the T produced by scanFn. The query should return one row. If no
// rows are found returns an error where errors.Is(pgx.ErrNoRows) is true. Returns an error if more than one row is
// returned.
func SelectRow[T any](ctx context.Context, db DB, sql string, args []any, scanFn pgx.RowToFunc[T]) (T, error) {
	rows, _ := db.Query(ctx, sql, args...)
	collectedRow, err := pgx.CollectOneRow(rows, scanFn)
	if err != nil {
		var zero T
		return zero, err
	}

	if rows.CommandTag().RowsAffected() > 1 {
		return collectedRow, errTooManyRows
	}

	return collectedRow, nil
}

// Insert inserts rows into tableName.
func Insert(ctx context.Context, db DB, tableName pgx.Identifier, rows []map[string]any) error {
	if len(rows) == 0 {
		return nil
	}

	sql, args := insertSQL(tableName, rows, "")
	_, err := db.Exec(ctx, sql, args...)
	return err
}

// InsertReturning inserts rows into tableName with returningClause and returns the []T produced by scanFn.
func InsertReturning[T any](ctx context.Context, db DB, tableName pgx.Identifier, rows []map[string]any, returningClause string, scanFn pgx.RowToFunc[T]) ([]T, error) {
	if len(rows) == 0 {
		return nil, nil
	}

	sql, args := insertSQL(tableName, rows, returningClause)
	return Select(ctx, db, sql, args, scanFn)
}

// insertSQL builds an insert statement that inserts rows into tableName with returningClause. len(rows) must be > 0.
func insertSQL(tableName pgx.Identifier, rows []map[string]any, returningClause string) (sql string, args []any) {
	b := &strings.Builder{}
	b.WriteString("insert into ")
	if len(tableName) == 1 {
		b.WriteString(sanitizeIdentifier(tableName[0]))
	} else {
		b.WriteString(tableName.Sanitize())
	}
	b.WriteString(" (")

	// Go maps are iterated in random order. The generated SQL should be stable so sort the keys.
	keys := make([]string, 0, len(rows[0]))
	for k := range rows[0] {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	for i, k := range keys {
		if i > 0 {
			b.WriteString(", ")
		}
		sanitizedKey := sanitizeIdentifier(k)
		b.WriteString(sanitizedKey)
	}

	args = make([]any, 0, len(keys))
	placeholder := int64(1)
	for i, values := range rows {
		if i == 0 {
			b.WriteString(") values (")
		} else {
			b.WriteString("), (")
		}

		for j, key := range keys {
			if j > 0 {
				b.WriteString(", ")
			}
			args = append(args, values[key])
			b.WriteByte('$')
			b.WriteString(strconv.FormatInt(placeholder, 10))
			placeholder++
		}
	}

	b.WriteString(")")

	if returningClause != "" {
		b.WriteString(" returning ")
		b.WriteString(returningClause)
	}

	return b.String(), args
}

// InsertRow inserts values into tableName.
func InsertRow(ctx context.Context, db DB, tableName pgx.Identifier, values map[string]any) error {
	sql, args := insertRowSQL(tableName, values, "")
	_, err := db.Exec(ctx, sql, args...)
	return err
}

// InsertRowReturning inserts values into tableName with returningClause and returns the T produced by scanFn.
func InsertRowReturning[T any](ctx context.Context, db DB, tableName pgx.Identifier, values map[string]any, returningClause string, scanFn pgx.RowToFunc[T]) (T, error) {
	sql, args := insertRowSQL(tableName, values, returningClause)
	return SelectRow(ctx, db, sql, args, scanFn)
}

func sanitizeIdentifier(s string) string {
	for _, r := range s {
		if !((r >= '0' && r <= '9') || (r >= 'a' && r <= 'z') || r == '_') {
			return pgx.Identifier{s}.Sanitize()
		}
	}

	return s
}

func insertRowSQL(tableName pgx.Identifier, values map[string]any, returningClause string) (sql string, args []any) {
	b := &strings.Builder{}
	b.WriteString("insert into ")
	if len(tableName) == 1 {
		b.WriteString(sanitizeIdentifier(tableName[0]))
	} else {
		b.WriteString(tableName.Sanitize())
	}
	b.WriteString(" (")

	// Go maps are iterated in random order. The generated SQL should be stable so sort the keys.
	keys := make([]string, 0, len(values))
	for k := range values {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	for i, k := range keys {
		if i > 0 {
			b.WriteString(", ")
		}
		sanitizedKey := sanitizeIdentifier(k)
		b.WriteString(sanitizedKey)
	}

	b.WriteString(") values (")
	args = make([]any, len(keys))
	for i, k := range keys {
		if i > 0 {
			b.WriteString(", ")
		}
		args[i] = values[k]
		b.WriteByte('$')
		b.WriteString(strconv.FormatInt(int64(i+1), 10))
	}

	b.WriteString(")")

	if returningClause != "" {
		b.WriteString(" returning ")
		b.WriteString(returningClause)
	}

	return b.String(), args
}

// ExecRow executes SQL with args on db. It returns an error unless exactly one row is affected.
func ExecRow(ctx context.Context, db DB, sql string, args ...any) (pgconn.CommandTag, error) {
	ct, err := db.Exec(ctx, sql, args...)
	if err != nil {
		return ct, err
	}
	rowsAffected := ct.RowsAffected()
	if rowsAffected == 0 {
		return ct, pgx.ErrNoRows
	} else if rowsAffected > 1 {
		return ct, errTooManyRows
	}

	return ct, nil
}

// Update updates rows matching whereValues in tableName with setValues. It includes returningClause and returns the []T
// produced by scanFn.
func Update(ctx context.Context, db DB, tableName pgx.Identifier, setValues, whereValues map[string]any) (pgconn.CommandTag, error) {
	sql, args := updateSQL(tableName, setValues, whereValues, "")
	return db.Exec(ctx, sql, args...)
}

// UpdateReturning updates rows matching whereValues in tableName with setValues. It includes returningClause and returns the []T
// produced by scanFn.
func UpdateReturning[T any](ctx context.Context, db DB, tableName pgx.Identifier, setValues, whereValues map[string]any, returningClause string, scanFn pgx.RowToFunc[T]) ([]T, error) {
	sql, args := updateSQL(tableName, setValues, whereValues, returningClause)
	return Select(ctx, db, sql, args, scanFn)
}

// UpdateRowReturning updates a row matching whereValues in tableName with setValues. It includes returningClause and returns the
// T produced by scanFn. Returns an error unless exactly one row is updated.
func UpdateRowReturning[T any](ctx context.Context, db DB, tableName pgx.Identifier, setValues, whereValues map[string]any, returningClause string, scanFn pgx.RowToFunc[T]) (T, error) {
	sql, args := updateSQL(tableName, setValues, whereValues, returningClause)
	return SelectRow(ctx, db, sql, args, scanFn)
}

func updateSQL(tableName pgx.Identifier, setValues, whereValues map[string]any, returningClause string) (sql string, args []any) {
	b := &strings.Builder{}
	b.WriteString("update ")
	if len(tableName) == 1 {
		b.WriteString(sanitizeIdentifier(tableName[0]))
	} else {
		b.WriteString(tableName.Sanitize())
	}
	b.WriteString(" set ")

	args = make([]any, 0, len(setValues)+len(whereValues))

	// Go maps are iterated in random order. The generated SQL should be stable so sort the setValueKeys.
	setValueKeys := make([]string, 0, len(setValues))
	for k := range setValues {
		setValueKeys = append(setValueKeys, k)
	}
	sort.Strings(setValueKeys)

	for i, k := range setValueKeys {
		if i > 0 {
			b.WriteString(", ")
		}
		sanitizedKey := sanitizeIdentifier(k)
		b.WriteString(sanitizedKey)
		b.WriteString(" = $")
		args = append(args, setValues[k])
		b.WriteString(strconv.FormatInt(int64(len(args)), 10))
	}

	if len(whereValues) > 0 {
		b.WriteString(" where ")

		whereValueKeys := make([]string, 0, len(whereValues))
		for k := range whereValues {
			whereValueKeys = append(whereValueKeys, k)
		}
		sort.Strings(whereValueKeys)

		for i, k := range whereValueKeys {
			if i > 0 {
				b.WriteString(" and ")
			}
			sanitizedKey := sanitizeIdentifier(k)
			b.WriteString(sanitizedKey)
			b.WriteString(" = $")
			args = append(args, whereValues[k])
			b.WriteString(strconv.FormatInt(int64(len(args)), 10))
		}
	}

	if returningClause != "" {
		b.WriteString(" returning ")
		b.WriteString(returningClause)
	}

	return b.String(), args
}
