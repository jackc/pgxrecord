// Package pgxrecord is a tiny framework for CRUD operations and data mapping.
package pgxrecord

import (
	"context"
	"fmt"

	"github.com/jackc/pgconn"
	"github.com/jackc/pgsql"
	"github.com/jackc/pgx/v4"
)

type DB interface {
	Exec(ctx context.Context, sql string, arguments ...interface{}) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, optionsAndArgs ...interface{}) (pgx.Rows, error)
}

type Normalizer interface {
	// Normalize normalizes the data in a record. e.g. Strip spaces from strings.
	Normalize(context.Context)
}

type Validator interface {
	// Validate returns an error if the record is not valid.
	Validate(context.Context) error
}

type Inserter interface {
	// InsertQuery returns the sql, query arguments, and optionally arguments to scan (for a returning clause) used to insert the record.
	InsertQuery(context.Context) (sql string, queryArgs []interface{}, scanArgs []interface{})
}

type Updater interface {
	// UpdateQuery returns the sql, query arguments, and optionally arguments to scan (for a returning clause) used to update the record.
	UpdateQuery(context.Context) (sql string, queryArgs []interface{}, scanArgs []interface{})
}

type Deleter interface {
	// UpdateQuery returns the sql and query arguments used to delete the record.
	DeleteQuery(context.Context) (sql string, queryArgs []interface{})
}

type Selector interface {
	// SelectStatementOptions returns statement options to build a query that selects a record or records.
	SelectStatementOptions(context.Context) []pgsql.StatementOption

	// SelectScanArgs returns the arguments to scan from the record.
	SelectScanArgs(context.Context) []interface{}
}

type SelectCollection interface {
	// Add adds a Selector to the collection and returns it.
	Add() Selector
}

type PgErrorMapper interface {
	// MapPgError converts a pgconn.PgError to another type of error. For example, a unique constraint violation may be
	// converted to an application specific validation error.
	MapPgError(context.Context, *pgconn.PgError) error
}

func tryMapPgError(ctx context.Context, record interface{}, err error) error {
	if mapper, ok := record.(PgErrorMapper); ok {
		if pgErr, ok := err.(*pgconn.PgError); ok {
			return mapper.MapPgError(ctx, pgErr)
		}
	}

	return err
}

type multipleRowsError struct {
	rowCount int64
}

func (e *multipleRowsError) Error() string {
	return fmt.Sprintf("expected 1 row got %d", e.rowCount)
}

type notFoundError struct{}

func (e *notFoundError) Error() string {
	return "not found"
}

type badStatementTypeError struct {
	expected string
	actual   string
}

func (e *badStatementTypeError) Error() string {
	return fmt.Sprintf("expected %s statement got %s", e.expected, e.actual)
}

// NotFound returns true if err is a not found error.
func NotFound(err error) bool {
	_, ok := err.(*notFoundError)
	return ok
}

// Insert inserts record into db. If record implements Normalizer then Normalize will be called. Next, if the record
// implements Validator then Validate will be called. If an error is returned the Insert is aborted.
func Insert(ctx context.Context, db DB, record Inserter) error {
	return insertOrUpdate(
		ctx,
		db,
		record,
		func() (string, []interface{}, []interface{}) { return record.InsertQuery(ctx) },
		func(ct pgconn.CommandTag) error {
			if !ct.Insert() {
				return &badStatementTypeError{expected: "INSERT", actual: ct.String()}
			}
			return nil
		},
	)
}

// Update updates record in db. If record implements Normalizer then Normalize will be called. Next, if the record
// implements Validator then Validate will be called. If an error is returned the Update is aborted. If the update
// query does not update exactly one record an error will be returned.
func Update(ctx context.Context, db DB, record Updater) error {
	return insertOrUpdate(
		ctx,
		db,
		record,
		func() (string, []interface{}, []interface{}) { return record.UpdateQuery(ctx) },
		func(ct pgconn.CommandTag) error {
			if !ct.Update() {
				return &badStatementTypeError{expected: "UPDATE", actual: ct.String()}
			}
			return nil
		},
	)
}

type buildInsertOrUpdateQueryFunc func() (sql string, queryArgs []interface{}, scanArgs []interface{})
type checkCommandTagTypeFunc func(pgconn.CommandTag) error

func insertOrUpdate(
	ctx context.Context,
	db DB,
	record interface{},
	buildQuery buildInsertOrUpdateQueryFunc,
	checkCommandTagType checkCommandTagTypeFunc,
) error {
	if n, ok := record.(Normalizer); ok {
		n.Normalize(ctx)
	}

	if v, ok := record.(Validator); ok {
		err := v.Validate(ctx)
		if err != nil {
			return err
		}
	}

	sql, queryArgs, scanArgs := buildQuery()

	var ct pgconn.CommandTag
	var err error
	if len(scanArgs) > 0 {
		ct, err = insertOrUpdateWithScan(ctx, db, sql, queryArgs, scanArgs)
	} else {
		ct, err = db.Exec(ctx, sql, queryArgs...)
	}
	if err != nil {
		return tryMapPgError(ctx, record, err)
	}

	err = checkCommandTagType(ct)
	if err != nil {
		return err
	}

	rowsAffected := ct.RowsAffected()
	if rowsAffected == 0 {
		return &notFoundError{}
	}
	if rowsAffected > 1 {
		return &multipleRowsError{rowCount: rowsAffected}
	}

	return nil
}

func insertOrUpdateWithScan(ctx context.Context, db DB, sql string, queryArgs []interface{}, scanArgs []interface{}) (pgconn.CommandTag, error) {
	rows, err := db.Query(ctx, sql, queryArgs...)
	if err != nil {
		return pgconn.CommandTag(""), err
	}

	rowCount := int64(0)
	for rows.Next() {
		if rowCount == 0 {
			rows.Scan(scanArgs...)
		}

		rowCount++
	}

	return rows.CommandTag(), rows.Err()
}

// Delete deletes record in db. If the delete query does not delete exactly one record an error will be returned.
func Delete(ctx context.Context, db DB, record Deleter) error {
	sql, queryArgs := record.DeleteQuery(ctx)

	ct, err := db.Exec(ctx, sql, queryArgs...)
	if err != nil {
		return tryMapPgError(ctx, record, err)
	}

	if !ct.Delete() {
		return &badStatementTypeError{expected: "DELETE", actual: ct.String()}
	}

	rowsAffected := ct.RowsAffected()
	if rowsAffected == 0 {
		return &notFoundError{}
	}
	if rowsAffected > 1 {
		return &multipleRowsError{rowCount: rowsAffected}
	}

	return nil
}

// SelectOne selects a single record from db into record. It applies options to the SQL statement. An error will be
// returned if no rows are found. Check for this case with the NotFound function. If multiple rows are selected an
// error will be returned.
func SelectOne(ctx context.Context, db DB, record Selector, options ...pgsql.StatementOption) error {
	stmt := pgsql.NewStatement()

	recordOptions := record.SelectStatementOptions(ctx)
	err := stmt.Apply(recordOptions...)
	if err != nil {
		return err
	}

	err = stmt.Apply(options...)
	if err != nil {
		return err
	}

	rows, err := db.Query(ctx, stmt.String(), stmt.Args.Values()...)
	if err != nil {
		return tryMapPgError(ctx, record, err)
	}

	rowCount := int64(0)
	for rows.Next() {
		if rowCount == 0 {
			scanArgs := record.SelectScanArgs(ctx)
			err := rows.Scan(scanArgs...)
			if err != nil {
				return err
			}
		}

		rowCount++
	}
	if rows.Err() != nil {
		return tryMapPgError(ctx, record, rows.Err())
	}

	if rowCount == 0 {
		return &notFoundError{}
	}

	if rowCount > 1 {
		return &multipleRowsError{rowCount: rowCount}
	}

	return nil
}

// SelectAll selects records from db into collection. It applies options to the SQL statement.
func SelectAll(ctx context.Context, db DB, collection SelectCollection, options ...pgsql.StatementOption) error {
	stmt := pgsql.NewStatement()

	record := collection.Add()
	recordOptions := record.SelectStatementOptions(ctx)
	err := stmt.Apply(recordOptions...)
	if err != nil {
		return err
	}

	err = stmt.Apply(options...)
	if err != nil {
		return err
	}

	rows, err := db.Query(ctx, stmt.String(), stmt.Args.Values()...)
	if err != nil {
		return tryMapPgError(ctx, record, err)
	}

	rowCount := 0
	for rows.Next() {
		if rowCount > 0 {
			record = collection.Add()
		}
		scanArgs := record.SelectScanArgs(ctx)
		err := rows.Scan(scanArgs...)
		if err != nil {
			return err
		}

		rowCount++
	}
	if rows.Err() != nil {
		return tryMapPgError(ctx, record, rows.Err())
	}

	return nil
}
