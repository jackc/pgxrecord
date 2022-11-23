// Package pgxrecord is a tiny library for CRUD operations.
package pgxrecord

import (
	"context"

	"github.com/jackc/pgx/v5"
)

type databaseValue int64

const DatabaseDefault = databaseValue(1)

type Queryer interface {
	Query(ctx context.Context, sql string, optionsAndArgs ...interface{}) (pgx.Rows, error)
}

type Column struct {
	Name       string
	PrimaryKey bool
}

type Table struct {
	Name       pgx.Identifier
	columns    []*Column
	primaryKey []*Column
}

type Record struct {
	table              *Table
	originalAttributes []any
	attributes         []any
}

func (t *Table) LoadAllColumns(ctx context.Context, db Queryer) error {
	db.Query(
		ctx,
		`select *
from pg_catalog.pg_attribute
where attrelid = $1::regclass
	and attnum > 0
	and not attisdropped
order by attnum;`,
		t.Name,
	)
}
