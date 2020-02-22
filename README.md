[![](https://godoc.org/github.com/jackc/pgxrecord?status.svg)](https://godoc.org/github.com/jackc/pgxrecord)
[![Build Status](https://travis-ci.org/jackc/pgxrecord.svg)](https://travis-ci.org/jackc/pgxrecord)

# pgxrecord

Package pgxrecord is a tiny framework for CRUD operations and data mapping. It is both a library and a code generation tool.

It does not and will not have traditional ORM features such as associations, validation, and hooks. It's sole purpose is a simple way to read and write rows from the database.

## Package Status

pgxrecord is functional and tested but highly experimental. The API may change at any time or the package may be abandoned.

## Example Usage

Select all records matching conditions.

```go
var widgets WidgetCollection
err = pgxrecord.SelectAll(ctx, tx, &widgets, pgsql.Where("name like ?", "%green%"))
```

Select one record matching conditions.

```go
widget := &Widget{}
err = pgxrecord.SelectOne(ctx, tx, widget, pgsql.Where("id=?", 42))
```

Insert a record.

```go
widget := &Widget{Name: "sprocket"}
err := pgxrecord.Insert(ctx, db, widget)
```

Update a record.

```go
widget := &Widget{}
err = pgxrecord.SelectOne(ctx, tx, widget, pgsql.Where("id=?", 42))

widget.Name = "New and Improved"
err = pgxrecord.Update(ctx, tx, widget)
```

Delete a record.

```go
widget := &Widget{}
err = pgxrecord.SelectOne(ctx, tx, widget, pgsql.Where("id=?", 42))

err = pgxrecord.Delete(ctx, tx, widget)
```

For the above code to work, the `Widget` and `WidgetCollection` types need to be defined. These can be created automatically by the `pgxrecord` CLI tool.

Install:

```
go get github.com/jackc/pgxrecord/cmd/pgxrecord
```

Connect to database and introspect widgets table:

```
pgxrecord new widgets > widgets.json
```

Compile `widgets.json` into Go code:

```
pgxrecord compile widgets.json | goimports > widgets.go
```

`pgxrecord new` will use the standard `PG*` environment variables to determine what database to connect to or a URL can be specified as an argument.

The produced JSON can be modified to change the mapping between Go and PostgreSQL names and types.

## Testing

The pgxrecord tests require a PostgreSQL database. It will use the standard PG* environment variables (PGHOST, PGDATABASE, etc.) for its connection settings. Each test is run inside of a transaction which is rolled back at the end of the test. No permanent changes will be made to the test database.
