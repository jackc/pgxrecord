// Package pgxrecord is a tiny framework for CRUD operations and data mapping.
package pgxrecord

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgconn"
	"github.com/jackc/pgsql"
	"github.com/jackc/pgx/v4"
)

type Queryer interface {
	Query(ctx context.Context, sql string, optionsAndArgs ...interface{}) (pgx.Rows, error)
}

type Field interface {
	Get() interface{}
	Set(interface{}) error
}

type FieldByColumnNamer interface {
	FieldByColumnName(string) (Field, error)
}

type Inserter interface {
	InsertStatement() (*pgsql.InsertStatement, error)
}

type Updater interface {
	UpdateStatement() (*pgsql.UpdateStatement, error)
}

type Deleter interface {
	DeleteStatement() (*pgsql.DeleteStatement, error)
}

type Selector interface {
	// SelectStatement returns a select statement that selects a record.
	SelectStatement() (*pgsql.SelectStatement, error)
	FieldByColumnNamer
}

type SelectCollection interface {
	// NewRecord allocates and returns a new record that can be appended to this collection.
	NewRecord() Selector

	// Append appends record to the collection.
	Append(record Selector)
}

type PgErrorMapper interface {
	// MapPgError converts a *pgconn.PgError to another type of error. For example, a unique constraint violation may be
	// converted to an application specific validation error.
	MapPgError(*pgconn.PgError) error
}

func tryMapPgError(record interface{}, err error) error {
	if mapper, ok := record.(PgErrorMapper); ok {
		if pgErr, ok := err.(*pgconn.PgError); ok {
			return mapper.MapPgError(pgErr)
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

// NotFound returns true if err is a not found error.
func NotFound(err error) bool {
	return errors.Is(err, &notFoundError{})
}

// Insert inserts record into db. If the insert query does not affect exactly one record an error will be returned.
func InsertOne(ctx context.Context, db Queryer, record Inserter) error {
	stmt, err := record.InsertStatement()
	if err != nil {
		return err
	}
	sql, args := pgsql.Build(stmt)
	return queryOne(ctx, db, record, sql, args)
}

// Update updates record in db. If the update query does not affect exactly one record an error will be returned.
func UpdateOne(ctx context.Context, db Queryer, record Updater) error {
	stmt, err := record.UpdateStatement()
	if err != nil {
		return err
	}
	sql, args := pgsql.Build(stmt)
	return queryOne(ctx, db, record, sql, args)
}

// Delete deletes record in db. If the delete query does affect exactly one record an error will be returned.
func DeleteOne(ctx context.Context, db Queryer, record Deleter) error {
	stmt, err := record.DeleteStatement()
	if err != nil {
		return err
	}
	sql, args := pgsql.Build(stmt)
	return queryOne(ctx, db, record, sql, args)
}

// SelectOne selects a single record from db into record. It applies scopes to the SQL statement. An error will be
// returned if no rows are found. Check for this case with the NotFound function. If multiple rows are selected an
// error will be returned.
func SelectOne(ctx context.Context, db Queryer, record Selector, scopes ...*pgsql.SelectStatement) error {
	stmt, err := record.SelectStatement()
	if err != nil {
		return err
	}

	stmt.Apply(scopes...)

	sql, args := pgsql.Build(stmt)
	return queryOne(ctx, db, record, sql, args)
}

func scanByNames(rows pgx.Rows, fielder FieldByColumnNamer) error {
	dest := make([]interface{}, len(rows.FieldDescriptions()))
	for i, fd := range rows.FieldDescriptions() {
		var err error
		dest[i], err = fielder.FieldByColumnName(string(fd.Name))
		if err != nil {
			return err
		}
	}
	return rows.Scan(dest...)
}

func queryOne(ctx context.Context, db Queryer, record interface{}, sql string, queryArgs []interface{}) error {
	rows, err := db.Query(ctx, sql, queryArgs...)
	if err != nil {
		return err
	}

	fielder, _ := record.(FieldByColumnNamer)

	if rows.Next() {
		if fielder != nil && len(rows.FieldDescriptions()) > 0 {
			err = scanByNames(rows, fielder)
			if err != nil {
				rows.Close()
				return tryMapPgError(record, err)
			}
		}
	}
	rows.Close()
	if rows.Err() != nil {
		return tryMapPgError(record, rows.Err())
	}

	rowsAffected := rows.CommandTag().RowsAffected()
	if rowsAffected == 0 {
		return &notFoundError{}
	}
	if rowsAffected > 1 {
		return &multipleRowsError{rowCount: rowsAffected}
	}

	return nil
}

// SelectAll selects records from db into collection. It applies scopes to the SQL statement.
func SelectAll(ctx context.Context, db Queryer, collection SelectCollection, scopes ...*pgsql.SelectStatement) error {
	record := collection.NewRecord()
	stmt, err := record.SelectStatement()
	if err != nil {
		return err
	}

	stmt.Apply(scopes...)

	sql, args := pgsql.Build(stmt)

	rows, err := db.Query(ctx, sql, args...)
	if err != nil {
		return tryMapPgError(record, err)
	}

	rowCount := 0
	for rows.Next() {
		if rowCount > 0 {
			record = collection.NewRecord()
		}
		err = scanByNames(rows, record)
		if err != nil {
			return err
		}

		collection.Append(record)
		rowCount++
	}
	if rows.Err() != nil {
		return tryMapPgError(record, rows.Err())
	}

	return nil
}

type AttrMapper interface {
	AttrMap() AttrMap
}

type AttrMap map[string]interface{}

func (m AttrMap) AttrMap() AttrMap {
	return m
}

type AttrsErrors map[string]error

func (errs AttrsErrors) Error() string {
	sb := &strings.Builder{}
	i := 0
	for k, v := range errs {
		if i > 0 {
			sb.WriteString(", ")
		}
		fmt.Fprintf(sb, "%s: %v", k, v)
		i++
	}

	return sb.String()
}

func AssignAttrs(record FieldByColumnNamer, attrMapper AttrMapper) error {
	attrMap := attrMapper.AttrMap()
	var attrsErrs AttrsErrors
	for k, v := range attrMap {
		field, err := record.FieldByColumnName(k)
		if err != nil {
			if attrsErrs == nil {
				attrsErrs = make(AttrsErrors)
			}
			attrsErrs[k] = err
			continue
		}

		err = field.Set(v)
		if err != nil {
			if attrsErrs == nil {
				attrsErrs = make(AttrsErrors)
			}
			attrsErrs[k] = err
		}
	}

	if len(attrsErrs) > 0 {
		return attrsErrs
	}

	return nil
}
