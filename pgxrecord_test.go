package pgxrecord_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jackc/imperator/normalize"
	"github.com/jackc/imperator/validate"
	"github.com/jackc/pgconn"
	"github.com/jackc/pgsql"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgxrecord"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func withTx(t testing.TB, f func(ctx context.Context, tx pgx.Tx)) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()

	conn, err := pgx.Connect(ctx, "")
	require.NoError(t, err)
	defer closeConn(t, conn)

	tx, err := conn.Begin(ctx)
	require.NoError(t, err)
	defer tx.Rollback(ctx)

	conn.Exec(ctx, `create temporary table widgets (
	id serial primary key,
	name text not null unique
);`)

	f(ctx, tx)
}

func closeConn(t testing.TB, conn *pgx.Conn) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, conn.Close(ctx))
}

type Widget struct {
	ID   int32
	Name string
}

func (widget *Widget) BeforeSave(op pgxrecord.Op) error {
	widget.Name = normalize.TextField(widget.Name)

	v := &validate.Validator{}
	v.Presence("name", widget.Name)
	return v.Errors()
}

func (widget *Widget) SelectStatementOptions() []pgsql.StatementOption {
	return []pgsql.StatementOption{
		pgsql.Select("widgets.id, widgets.name"),
		pgsql.From("widgets"),
	}
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

func (widget *Widget) MapPgError(*pgconn.PgError) error {
	return errors.New("mapped error")
}

type WidgetCollection []*Widget

func (c *WidgetCollection) Add() pgxrecord.Selector {
	widget := &Widget{}
	*c = append(*c, widget)
	return widget
}

func TestInsertInserts(t *testing.T) {
	withTx(t, func(ctx context.Context, tx pgx.Tx) {
		widget := &Widget{Name: "sprocket"}
		err := pgxrecord.Insert(ctx, tx, widget)
		require.NoError(t, err)

		readBack := &Widget{}
		err = pgxrecord.SelectOne(ctx, tx, readBack, pgsql.Where("name=?", widget.Name))
		require.NoError(t, err)

		assert.Equal(t, widget.ID, readBack.ID)
		assert.Equal(t, widget.Name, readBack.Name)

		var n int
		err = tx.QueryRow(ctx, "select count(*) from widgets").Scan(&n)
		require.NoError(t, err)
		assert.Equal(t, 1, n)
	})
}

func TestInsertCallsBeforeSave(t *testing.T) {
	withTx(t, func(ctx context.Context, tx pgx.Tx) {
		widget := &Widget{}
		err := pgxrecord.Insert(ctx, tx, widget)
		assert.Error(t, err)

		var n int
		err = tx.QueryRow(ctx, "select count(*) from widgets").Scan(&n)
		require.NoError(t, err)
		assert.Equal(t, 0, n)
	})
}

type widgetWithoutInsertReturning Widget

func (widget *widgetWithoutInsertReturning) InsertQuery() (sql string, queryArgs []interface{}) {
	return (*Widget)(widget).InsertQuery()
}

func TestInsertWithoutReturningScan(t *testing.T) {
	withTx(t, func(ctx context.Context, tx pgx.Tx) {
		widget := &widgetWithoutInsertReturning{Name: "sprocket"}
		err := pgxrecord.Insert(ctx, tx, widget)
		assert.NoError(t, err)

		assert.EqualValues(t, 0, widget.ID)

		var n int
		err = tx.QueryRow(ctx, "select count(*) from widgets").Scan(&n)
		require.NoError(t, err)
		assert.Equal(t, 1, n)
	})
}

func TestUpdateUpdates(t *testing.T) {
	withTx(t, func(ctx context.Context, tx pgx.Tx) {
		widget := &Widget{Name: "sprocket"}
		err := pgxrecord.Insert(ctx, tx, widget)
		require.NoError(t, err)

		widget.Name = "device"
		err = pgxrecord.Update(ctx, tx, widget)
		require.NoError(t, err)

		readBack := &Widget{}
		err = pgxrecord.SelectOne(ctx, tx, readBack, pgsql.Where("name=?", widget.Name))
		require.NoError(t, err)

		assert.Equal(t, widget.ID, readBack.ID)
		assert.Equal(t, widget.Name, readBack.Name)

		var n int
		err = tx.QueryRow(ctx, "select count(*) from widgets").Scan(&n)
		require.NoError(t, err)
		assert.Equal(t, 1, n)
	})
}

func TestUpdateCallsBeforeSave(t *testing.T) {
	withTx(t, func(ctx context.Context, tx pgx.Tx) {
		widget := &Widget{Name: "sprocket"}
		err := pgxrecord.Insert(ctx, tx, widget)
		require.NoError(t, err)

		widget.Name = ""
		err = pgxrecord.Update(ctx, tx, widget)
		assert.Error(t, err)

		readBack := &Widget{}
		err = pgxrecord.SelectOne(ctx, tx, readBack, pgsql.Where("id=?", widget.ID))
		require.NoError(t, err)

		assert.Equal(t, "sprocket", readBack.Name)
	})
}

func TestUpdateNotFound(t *testing.T) {
	withTx(t, func(ctx context.Context, tx pgx.Tx) {
		widget := &Widget{ID: 42, Name: "sprocket"}
		err := pgxrecord.Update(ctx, tx, widget)
		require.Error(t, err)
		require.True(t, pgxrecord.NotFound(err))
	})
}

type widgetUpdatesTooMany Widget

func (widget *widgetUpdatesTooMany) UpdateQuery() (sql string, queryArgs []interface{}) {
	sql = `update widgets set name=name`
	queryArgs = nil
	return sql, queryArgs
}

func TestUpdateTooMany(t *testing.T) {
	withTx(t, func(ctx context.Context, tx pgx.Tx) {
		err := pgxrecord.Insert(ctx, tx, &Widget{Name: "sprocket"})
		require.NoError(t, err)
		err = pgxrecord.Insert(ctx, tx, &Widget{Name: "device"})
		require.NoError(t, err)

		widget := &widgetUpdatesTooMany{}
		err = pgxrecord.Update(ctx, tx, widget)
		require.Error(t, err)
		require.Equal(t, "expected 1 row got 2", err.Error())
	})
}

type widgetUpdateWithReturningScan Widget

func (widget *widgetUpdateWithReturningScan) UpdateQuery() (sql string, queryArgs []interface{}) {
	sql = `update widgets set name=$1||$1 where id=$2 returning name`
	queryArgs = []interface{}{widget.Name, widget.ID}
	return sql, queryArgs
}

func (widget *widgetUpdateWithReturningScan) UpdateScan(rows pgx.Rows) error {
	return rows.Scan(&widget.Name)
}

func TestUpdateWithReturningScan(t *testing.T) {
	withTx(t, func(ctx context.Context, tx pgx.Tx) {
		widget := &Widget{Name: "sprocket"}
		err := pgxrecord.Insert(ctx, tx, widget)
		require.NoError(t, err)

		w2 := (*widgetUpdateWithReturningScan)(widget)

		err = pgxrecord.Update(ctx, tx, w2)
		require.NoError(t, err)
		assert.Equal(t, "sprocketsprocket", w2.Name)
	})
}

func TestDeleteDeletes(t *testing.T) {
	withTx(t, func(ctx context.Context, tx pgx.Tx) {
		widget := &Widget{Name: "sprocket"}
		err := pgxrecord.Insert(ctx, tx, widget)
		require.NoError(t, err)

		err = pgxrecord.Delete(ctx, tx, widget)
		assert.NoError(t, err)

		var n int
		err = tx.QueryRow(ctx, "select count(*) from widgets").Scan(&n)
		require.NoError(t, err)
		assert.Equal(t, 0, n)
	})
}

func TestDeleteNotFound(t *testing.T) {
	withTx(t, func(ctx context.Context, tx pgx.Tx) {
		widget := &Widget{ID: 42, Name: "sprocket"}
		err := pgxrecord.Delete(ctx, tx, widget)
		require.Error(t, err)
		require.True(t, pgxrecord.NotFound(err))
	})
}

type widgetDeletesTooMany Widget

func (widget *widgetDeletesTooMany) DeleteQuery() (sql string, queryArgs []interface{}) {
	sql = `delete from widgets`
	queryArgs = nil
	return sql, queryArgs
}

func TestDeleteTooMany(t *testing.T) {
	withTx(t, func(ctx context.Context, tx pgx.Tx) {
		err := pgxrecord.Insert(ctx, tx, &Widget{Name: "sprocket"})
		require.NoError(t, err)
		err = pgxrecord.Insert(ctx, tx, &Widget{Name: "device"})
		require.NoError(t, err)

		widget := &widgetDeletesTooMany{}
		err = pgxrecord.Delete(ctx, tx, widget)
		require.Error(t, err)
		require.Equal(t, "expected 1 row got 2", err.Error())
	})
}

type widgetDeleteWithReturningScan Widget

func (widget *widgetDeleteWithReturningScan) DeleteQuery() (sql string, queryArgs []interface{}) {
	sql = `delete from widgets where id=$1 returning name`
	queryArgs = []interface{}{widget.ID}
	return sql, queryArgs
}

func (widget *widgetDeleteWithReturningScan) DeleteScan(rows pgx.Rows) error {
	return rows.Scan(&widget.Name)
}

func TestDeleteWithReturningScan(t *testing.T) {
	withTx(t, func(ctx context.Context, tx pgx.Tx) {
		widget := &Widget{Name: "sprocket"}
		err := pgxrecord.Insert(ctx, tx, widget)
		require.NoError(t, err)

		toDelete := &widgetDeleteWithReturningScan{ID: widget.ID}
		err = pgxrecord.Delete(ctx, tx, toDelete)
		require.NoError(t, err)
		assert.Equal(t, widget.Name, toDelete.Name)
	})
}

func TestSelectOneSelects(t *testing.T) {
	withTx(t, func(ctx context.Context, tx pgx.Tx) {
		dbWidget := &Widget{Name: "sprocket"}
		err := pgxrecord.Insert(ctx, tx, dbWidget)
		require.NoError(t, err)

		selectedWidget := &Widget{}
		err = pgxrecord.SelectOne(ctx, tx, selectedWidget, pgsql.Where("name=?", dbWidget.Name))
		require.NoError(t, err)

		assert.Equal(t, dbWidget.ID, selectedWidget.ID)
		assert.Equal(t, dbWidget.Name, selectedWidget.Name)
	})
}

func TestSelectOneErrorWhenNotFound(t *testing.T) {
	withTx(t, func(ctx context.Context, tx pgx.Tx) {
		widget := &Widget{}
		err := pgxrecord.SelectOne(ctx, tx, widget)
		require.Error(t, err)
		require.True(t, pgxrecord.NotFound(err))
	})
}

func TestSelectOneErrorWhenTooManyRows(t *testing.T) {
	withTx(t, func(ctx context.Context, tx pgx.Tx) {
		err := pgxrecord.Insert(ctx, tx, &Widget{Name: "sprocket"})
		require.NoError(t, err)
		err = pgxrecord.Insert(ctx, tx, &Widget{Name: "device"})
		require.NoError(t, err)

		widget := &Widget{}
		err = pgxrecord.SelectOne(ctx, tx, widget)
		require.Error(t, err)
		require.Equal(t, "expected 1 row got 2", err.Error())
	})
}

func TestSelectAllSelects(t *testing.T) {
	withTx(t, func(ctx context.Context, tx pgx.Tx) {
		err := pgxrecord.Insert(ctx, tx, &Widget{Name: "sprocket"})
		require.NoError(t, err)
		err = pgxrecord.Insert(ctx, tx, &Widget{Name: "device"})
		require.NoError(t, err)

		var widgets WidgetCollection

		err = pgxrecord.SelectAll(ctx, tx, &widgets)
		require.NoError(t, err)

		assert.Len(t, widgets, 2)
	})
}

func TestSelectAllOptions(t *testing.T) {
	withTx(t, func(ctx context.Context, tx pgx.Tx) {
		err := pgxrecord.Insert(ctx, tx, &Widget{Name: "sprocket"})
		require.NoError(t, err)
		err = pgxrecord.Insert(ctx, tx, &Widget{Name: "device"})
		require.NoError(t, err)

		var widgets WidgetCollection

		err = pgxrecord.SelectAll(ctx, tx, &widgets, pgsql.Where("name=?", "sprocket"))
		require.NoError(t, err)

		assert.Len(t, widgets, 1)
	})
}

func TestPgErrorMapper(t *testing.T) {
	withTx(t, func(ctx context.Context, tx pgx.Tx) {
		err := pgxrecord.Insert(ctx, tx, &Widget{Name: "sprocket"})
		require.NoError(t, err)
		err = pgxrecord.Insert(ctx, tx, &Widget{Name: "sprocket"})
		require.Error(t, err)
		assert.Equal(t, "mapped error", err.Error())
	})
}

func BenchmarkSelectOne(b *testing.B) {
	withTx(b, func(ctx context.Context, tx pgx.Tx) {
		widget := &Widget{Name: "sprocket"}
		err := pgxrecord.Insert(ctx, tx, widget)
		require.NoError(b, err)

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			w := &Widget{}
			err := pgxrecord.SelectOne(ctx, tx, w, pgsql.Where("id=?", widget.ID))
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkSelectOnePgx(b *testing.B) {
	withTx(b, func(ctx context.Context, tx pgx.Tx) {
		widget := &Widget{Name: "sprocket"}
		err := pgxrecord.Insert(ctx, tx, widget)
		require.NoError(b, err)

		stmt := pgsql.NewStatement()
		err = stmt.Apply(pgsql.Select("id, widgets"), pgsql.From("widgets"), pgsql.Where("id=?", widget.ID))
		if err != nil {
			b.Fatal(err)
		}

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			w := &Widget{}
			err = tx.QueryRow(ctx, stmt.String(), stmt.Args.Values()...).Scan(&w.ID, &w.Name)
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}
