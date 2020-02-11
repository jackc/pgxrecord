[![](https://godoc.org/github.com/jackc/pgxrecord?status.svg)](https://godoc.org/github.com/jackc/pgxrecord)
[![Build Status](https://travis-ci.org/jackc/pgxrecord.svg)](https://travis-ci.org/jackc/pgxrecord)

# pgxrecord

Package pgxrecord is a tiny framework for CRUD operations and data mapping.

It is not an ORM. It does not use reflection and it does not require code generation.

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

However, the price for strong typing, no reflection, and no code generation is writing SQL and data mapping code manually.

```go
type Widget struct {
	ID   int32
	Name string
}

func (widget *Widget) SelectStatement() *pgsql.SelectStatement {
	return pgsql.Select("widgets.id, widgets.name").From("widgets")
}

func (widget *Widget) SelectScan(rows pgx.Rows) error {
	return rows.Scan(&widget.ID, &widget.Name)
}

func (widget *Widget) InsertQuery() (sql string, queryArgs []interface{}) {
	sql = "insert into widgets(name) values ($1) returning id"
	queryArgs = []interface{}{widget.Name}
	return sql, queryArgs
}

func (widget *Widget) InsertScan(rows pgx.Rows) error {
	return rows.Scan(&widget.ID)
}

func (widget *Widget) UpdateQuery() (sql string, queryArgs []interface{}) {
	sql = `update widgets set name=$1 where id=$2`
	queryArgs = []interface{}{widget.Name, widget.ID}
	return sql, queryArgs
}

func (widget *Widget) DeleteQuery() (sql string, queryArgs []interface{}) {
	sql = `delete from widgets where id=$1`
	queryArgs = []interface{}{widget.ID}
	return sql, queryArgs
}

type WidgetCollection []*Widget

func (c *WidgetCollection) NewRecord() pgxrecord.Selector {
	return &Widget{}
}

func (c *WidgetCollection) Append(s pgxrecord.Selector) {
	*c = append(*c, s.(*Widget))
}
```

## Testing

The pgxrecord tests require a PostgreSQL database. It will use the standard PG* environment variables (PGHOST, PGDATABASE, etc.) for its connection settings. Each test is run inside of a transaction which is rolled back at the end of the test. No permanent changes will be made to the test database.
