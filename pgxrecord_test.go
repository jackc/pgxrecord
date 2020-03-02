package pgxrecord_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgconn"
	"github.com/jackc/pgsql"
	"github.com/jackc/pgtype"
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
  name text not null unique,
  aaaaaaaaaa text,
  bbbbbbbbbb text,
  cccccccccc text,
  dddddddddd timestamptz,
  eeeeeeeeee timestamptz,
  ffffffffff date,
  gggggggggg bigint,
  hhhhhhhhhh int
);`)

	f(ctx, tx)
}

func closeConn(t testing.TB, conn *pgx.Conn) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, conn.Close(ctx))
}

func TestInsertInserts(t *testing.T) {
	withTx(t, func(ctx context.Context, tx pgx.Tx) {
		widget := &Widget{}
		widget.Name.Set("sprocket")
		err := pgxrecord.InsertOne(ctx, tx, widget)
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

type widgetWithoutInsertReturning Widget

func (widget *widgetWithoutInsertReturning) InsertStatement() (*pgsql.InsertStatement, error) {
	columns := make([]string, 0, 2)
	values := make([]interface{}, 0, 2)

	if widget.ID.Status != pgtype.Undefined {
		columns = append(columns, "id")
		values = append(values, widget.ID)
	}

	if widget.Name.Status != pgtype.Undefined {
		columns = append(columns, "name")
		values = append(values, widget.Name)
	}

	vs := pgsql.Values().Row(values...)
	return pgsql.Insert("widgets").Columns(columns...).Values(vs), nil
}

func TestInsertWithoutReturningScan(t *testing.T) {
	withTx(t, func(ctx context.Context, tx pgx.Tx) {
		widget := &widgetWithoutInsertReturning{}
		widget.Name.Set("sprocket")
		err := pgxrecord.InsertOne(ctx, tx, widget)
		assert.NoError(t, err)

		assert.EqualValues(t, 0, widget.ID.Get())

		var n int
		err = tx.QueryRow(ctx, "select count(*) from widgets").Scan(&n)
		require.NoError(t, err)
		assert.Equal(t, 1, n)
	})
}

func TestUpdateUpdates(t *testing.T) {
	withTx(t, func(ctx context.Context, tx pgx.Tx) {
		widget := &Widget{}
		widget.Name.Set("sprocket")
		err := pgxrecord.InsertOne(ctx, tx, widget)
		require.NoError(t, err)

		widget.Name.Set("device")
		err = pgxrecord.UpdateOne(ctx, tx, widget)
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

func TestUpdateNotFound(t *testing.T) {
	withTx(t, func(ctx context.Context, tx pgx.Tx) {
		widget := &Widget{}
		err := pgxrecord.AssignAttrs(widget, pgxrecord.AttrMap{"id": 42, "name": "sprocket"})
		require.NoError(t, err)
		err = pgxrecord.UpdateOne(ctx, tx, widget)
		require.Error(t, err)
		require.True(t, pgxrecord.NotFound(err))
	})
}

func TestUpdateTooMany(t *testing.T) {
	withTx(t, func(ctx context.Context, tx pgx.Tx) {
		err := pgxrecord.InsertOne(ctx, tx, &Widget{Name: pgtype.Text{String: "sprocket", Status: pgtype.Present}})
		require.NoError(t, err)
		err = pgxrecord.InsertOne(ctx, tx, &Widget{Name: pgtype.Text{String: "device", Status: pgtype.Present}})
		require.NoError(t, err)

		err = pgxrecord.UpdateOne(ctx, tx, pgsql.Update("widgets").Setf("name=name"))
		require.Error(t, err)
		require.Equal(t, "expected 1 row got 2", err.Error())
	})
}

func TestDeleteDeletes(t *testing.T) {
	withTx(t, func(ctx context.Context, tx pgx.Tx) {
		widget := &Widget{}
		widget.Name.Set("sprocket")
		err := pgxrecord.InsertOne(ctx, tx, widget)
		require.NoError(t, err)

		err = pgxrecord.DeleteOne(ctx, tx, widget)
		assert.NoError(t, err)

		var n int
		err = tx.QueryRow(ctx, "select count(*) from widgets").Scan(&n)
		require.NoError(t, err)
		assert.Equal(t, 0, n)
	})
}

func TestDeleteNotFound(t *testing.T) {
	withTx(t, func(ctx context.Context, tx pgx.Tx) {
		widget := &Widget{}
		widget.ID.Set(42)
		widget.Name.Set("sprocket")
		err := pgxrecord.DeleteOne(ctx, tx, widget)
		require.Error(t, err)
		require.True(t, pgxrecord.NotFound(err))
	})
}

func TestDeleteTooMany(t *testing.T) {
	withTx(t, func(ctx context.Context, tx pgx.Tx) {
		err := pgxrecord.InsertOne(ctx, tx, &Widget{Name: pgtype.Text{String: "sprocket", Status: pgtype.Present}})
		require.NoError(t, err)
		err = pgxrecord.InsertOne(ctx, tx, &Widget{Name: pgtype.Text{String: "device", Status: pgtype.Present}})
		require.NoError(t, err)

		err = pgxrecord.DeleteOne(ctx, tx, pgsql.Delete("widgets"))
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

func TestSelectOneSelects(t *testing.T) {
	withTx(t, func(ctx context.Context, tx pgx.Tx) {
		dbWidget := &Widget{}
		dbWidget.Name.Set("sprocket")
		err := pgxrecord.InsertOne(ctx, tx, dbWidget)
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

func TestNotFoundUnwrapsError(t *testing.T) {
	withTx(t, func(ctx context.Context, tx pgx.Tx) {
		widget := &Widget{}
		err := pgxrecord.SelectOne(ctx, tx, widget)
		require.Error(t, err)
		err = fmt.Errorf("some wrapping err: %w", err)
		require.True(t, pgxrecord.NotFound(err))
	})
}

func TestSelectOneErrorWhenTooManyRows(t *testing.T) {
	withTx(t, func(ctx context.Context, tx pgx.Tx) {
		err := pgxrecord.InsertOne(ctx, tx, &Widget{Name: pgtype.Text{String: "sprocket", Status: pgtype.Present}})
		require.NoError(t, err)
		err = pgxrecord.InsertOne(ctx, tx, &Widget{Name: pgtype.Text{String: "device", Status: pgtype.Present}})
		require.NoError(t, err)

		widget := &Widget{}
		err = pgxrecord.SelectOne(ctx, tx, widget)
		require.Error(t, err)
		require.Equal(t, "expected 1 row got 2", err.Error())
	})
}

func TestSelectAllSelects(t *testing.T) {
	withTx(t, func(ctx context.Context, tx pgx.Tx) {
		err := pgxrecord.InsertOne(ctx, tx, &Widget{Name: pgtype.Text{String: "sprocket", Status: pgtype.Present}})
		require.NoError(t, err)
		err = pgxrecord.InsertOne(ctx, tx, &Widget{Name: pgtype.Text{String: "device", Status: pgtype.Present}})
		require.NoError(t, err)

		var widgets WidgetCollection

		err = pgxrecord.SelectAll(ctx, tx, &widgets)
		require.NoError(t, err)

		assert.Len(t, widgets, 2)
	})
}

func TestSelectAllOptions(t *testing.T) {
	withTx(t, func(ctx context.Context, tx pgx.Tx) {
		err := pgxrecord.InsertOne(ctx, tx, &Widget{Name: pgtype.Text{String: "sprocket", Status: pgtype.Present}})
		require.NoError(t, err)
		err = pgxrecord.InsertOne(ctx, tx, &Widget{Name: pgtype.Text{String: "device", Status: pgtype.Present}})
		require.NoError(t, err)

		var widgets WidgetCollection

		err = pgxrecord.SelectAll(ctx, tx, &widgets, pgsql.Where("name=?", "sprocket"))
		require.NoError(t, err)

		assert.Len(t, widgets, 1)
	})
}

func TestSelectAllWhenNoResults(t *testing.T) {
	withTx(t, func(ctx context.Context, tx pgx.Tx) {
		err := pgxrecord.InsertOne(ctx, tx, &Widget{Name: pgtype.Text{String: "sprocket", Status: pgtype.Present}})
		require.NoError(t, err)
		err = pgxrecord.InsertOne(ctx, tx, &Widget{Name: pgtype.Text{String: "device", Status: pgtype.Present}})
		require.NoError(t, err)

		var widgets WidgetCollection

		err = pgxrecord.SelectAll(ctx, tx, &widgets, pgsql.Where("name=?", "invalid"))
		require.NoError(t, err)

		assert.Len(t, widgets, 0)
	})
}

type widgetMappedError Widget

func (row *widgetMappedError) InsertStatement() (*pgsql.InsertStatement, error) {
	return (*Widget)(row).InsertStatement()
}

func (row *widgetMappedError) MapPgError(*pgconn.PgError) error {
	return errors.New("mapped error")
}

func TestPgErrorMapper(t *testing.T) {
	withTx(t, func(ctx context.Context, tx pgx.Tx) {
		err := pgxrecord.InsertOne(ctx, tx, &widgetMappedError{Name: pgtype.Text{String: "sprocket", Status: pgtype.Present}})
		require.NoError(t, err)
		err = pgxrecord.InsertOne(ctx, tx, &widgetMappedError{Name: pgtype.Text{String: "sprocket", Status: pgtype.Present}})
		require.Error(t, err)
		assert.Equal(t, "mapped error", err.Error())
	})
}

func TestAssignAttrs(t *testing.T) {
	widget := &Widget{}
	err := pgxrecord.AssignAttrs(widget, pgxrecord.AttrMap{"id": 42, "name": "sprocket"})
	assert.NoError(t, err)
	assert.Equal(t, pgtype.Int4{Int: 42, Status: pgtype.Present}, widget.ID)
	assert.Equal(t, pgtype.Text{String: "sprocket", Status: pgtype.Present}, widget.Name)
}

func benchmarkInsertOne(b *testing.B, ctx context.Context, tx pgx.Tx) *Widget {
	widget := &Widget{}
	pgxrecord.AssignAttrs(widget, pgxrecord.AttrMap{
		"name":       "sprocket",
		"aaaaaaaaaa": "fajkfawjk",
		"bbbbbbbbbb": "fasdjklsaf",
		"cccccccccc": "rwehakjlsdaf",
		"dddddddddd": time.Now(),
		"eeeeeeeeee": time.Now(),
		"ffffffffff": time.Now(),
		"gggggggggg": 123456789,
		"hhhhhhhhhh": 987,
	})
	err := pgxrecord.InsertOne(ctx, tx, widget)
	require.NoError(b, err)
	return widget
}

func BenchmarkSelectOne(b *testing.B) {
	withTx(b, func(ctx context.Context, tx pgx.Tx) {
		widget := benchmarkInsertOne(b, ctx, tx)

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
		widget := benchmarkInsertOne(b, ctx, tx)

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			w := &Widget{}
			err := tx.QueryRow(
				ctx,
				"select id, name, aaaaaaaaaa, bbbbbbbbbb, cccccccccc, dddddddddd, eeeeeeeeee, ffffffffff, gggggggggg, hhhhhhhhhh from widgets where id=$1",
				widget.ID,
			).Scan(
				&w.ID,
				&w.Name,
				&w.Aaaaaaaaaa,
				&w.Bbbbbbbbbb,
				&w.Cccccccccc,
				&w.Dddddddddd,
				&w.Eeeeeeeeee,
				&w.Ffffffffff,
				&w.Gggggggggg,
				&w.Hhhhhhhhhh,
			)
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}
