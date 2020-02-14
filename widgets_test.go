package pgxrecord_test

import (
	"encoding/json"
	"errors"

	"github.com/jackc/pgsql"
	"github.com/jackc/pgtype"
	"github.com/jackc/pgxrecord"
)

type Widget struct {
	ID         pgtype.Int4
	Name       pgtype.Text
	Aaaaaaaaaa pgtype.Text
	Bbbbbbbbbb pgtype.Text
	Cccccccccc pgtype.Text
	Dddddddddd pgtype.Timestamptz
	Eeeeeeeeee pgtype.Timestamptz
	Ffffffffff pgtype.Date
	Gggggggggg pgtype.Int8
	Hhhhhhhhhh pgtype.Int4
}

func (row *Widget) FieldByColumnName(name string) (pgxrecord.Field, error) {
	switch name {
	case `id`:
		return &row.ID, nil
	case `name`:
		return &row.Name, nil
	case `aaaaaaaaaa`:
		return &row.Aaaaaaaaaa, nil
	case `bbbbbbbbbb`:
		return &row.Bbbbbbbbbb, nil
	case `cccccccccc`:
		return &row.Cccccccccc, nil
	case `dddddddddd`:
		return &row.Dddddddddd, nil
	case `eeeeeeeeee`:
		return &row.Eeeeeeeeee, nil
	case `ffffffffff`:
		return &row.Ffffffffff, nil
	case `gggggggggg`:
		return &row.Gggggggggg, nil
	case `hhhhhhhhhh`:
		return &row.Hhhhhhhhhh, nil
	default:
		return nil, errors.New("unknown attribute")
	}
}

func (row *Widget) AttrMap() pgxrecord.AttrMap {
	m := make(pgxrecord.AttrMap, 10)

	var value interface{}

	value = row.ID.Get()
	if value != pgtype.Undefined {
		m[`id`] = value
	}

	value = row.Name.Get()
	if value != pgtype.Undefined {
		m[`name`] = value
	}

	value = row.Aaaaaaaaaa.Get()
	if value != pgtype.Undefined {
		m[`aaaaaaaaaa`] = value
	}

	value = row.Bbbbbbbbbb.Get()
	if value != pgtype.Undefined {
		m[`bbbbbbbbbb`] = value
	}

	value = row.Cccccccccc.Get()
	if value != pgtype.Undefined {
		m[`cccccccccc`] = value
	}

	value = row.Dddddddddd.Get()
	if value != pgtype.Undefined {
		m[`dddddddddd`] = value
	}

	value = row.Eeeeeeeeee.Get()
	if value != pgtype.Undefined {
		m[`eeeeeeeeee`] = value
	}

	value = row.Ffffffffff.Get()
	if value != pgtype.Undefined {
		m[`ffffffffff`] = value
	}

	value = row.Gggggggggg.Get()
	if value != pgtype.Undefined {
		m[`gggggggggg`] = value
	}

	value = row.Hhhhhhhhhh.Get()
	if value != pgtype.Undefined {
		m[`hhhhhhhhhh`] = value
	}

	return m
}

func (row *Widget) MarshalJSON() ([]byte, error) {
	return json.Marshal(row.AttrMap())
}

func (row *Widget) InsertStatement() (*pgsql.InsertStatement, error) {
	columns := make([]string, 0, 10)
	values := make([]interface{}, 0, 10)

	if row.ID.Status != pgtype.Undefined {
		columns = append(columns, `id`)
		values = append(values, row.ID)
	}

	if row.Name.Status != pgtype.Undefined {
		columns = append(columns, `name`)
		values = append(values, row.Name)
	}

	if row.Aaaaaaaaaa.Status != pgtype.Undefined {
		columns = append(columns, `aaaaaaaaaa`)
		values = append(values, row.Aaaaaaaaaa)
	}

	if row.Bbbbbbbbbb.Status != pgtype.Undefined {
		columns = append(columns, `bbbbbbbbbb`)
		values = append(values, row.Bbbbbbbbbb)
	}

	if row.Cccccccccc.Status != pgtype.Undefined {
		columns = append(columns, `cccccccccc`)
		values = append(values, row.Cccccccccc)
	}

	if row.Dddddddddd.Status != pgtype.Undefined {
		columns = append(columns, `dddddddddd`)
		values = append(values, row.Dddddddddd)
	}

	if row.Eeeeeeeeee.Status != pgtype.Undefined {
		columns = append(columns, `eeeeeeeeee`)
		values = append(values, row.Eeeeeeeeee)
	}

	if row.Ffffffffff.Status != pgtype.Undefined {
		columns = append(columns, `ffffffffff`)
		values = append(values, row.Ffffffffff)
	}

	if row.Gggggggggg.Status != pgtype.Undefined {
		columns = append(columns, `gggggggggg`)
		values = append(values, row.Gggggggggg)
	}

	if row.Hhhhhhhhhh.Status != pgtype.Undefined {
		columns = append(columns, `hhhhhhhhhh`)
		values = append(values, row.Hhhhhhhhhh)
	}

	if len(columns) == 0 {
		return nil, errors.New("no attributes to insert")
	}

	vs := pgsql.Values().Row(values...)
	stmt := pgsql.Insert(`widgets`).Columns(columns...).Values(vs)

	stmt.Returning(`id`)

	return stmt, nil
}

func (row *Widget) UpdateStatement() (*pgsql.UpdateStatement, error) {
	assignments := make(pgsql.Assignments, 0, 10)

	if row.Name.Status != pgtype.Undefined {
		assignments = append(assignments, &pgsql.Assignment{Left: pgsql.Ident{`name`}, Right: pgsql.Param{Value: row.Name}})
	}

	if row.Aaaaaaaaaa.Status != pgtype.Undefined {
		assignments = append(assignments, &pgsql.Assignment{Left: pgsql.Ident{`aaaaaaaaaa`}, Right: pgsql.Param{Value: row.Aaaaaaaaaa}})
	}

	if row.Bbbbbbbbbb.Status != pgtype.Undefined {
		assignments = append(assignments, &pgsql.Assignment{Left: pgsql.Ident{`bbbbbbbbbb`}, Right: pgsql.Param{Value: row.Bbbbbbbbbb}})
	}

	if row.Cccccccccc.Status != pgtype.Undefined {
		assignments = append(assignments, &pgsql.Assignment{Left: pgsql.Ident{`cccccccccc`}, Right: pgsql.Param{Value: row.Cccccccccc}})
	}

	if row.Dddddddddd.Status != pgtype.Undefined {
		assignments = append(assignments, &pgsql.Assignment{Left: pgsql.Ident{`dddddddddd`}, Right: pgsql.Param{Value: row.Dddddddddd}})
	}

	if row.Eeeeeeeeee.Status != pgtype.Undefined {
		assignments = append(assignments, &pgsql.Assignment{Left: pgsql.Ident{`eeeeeeeeee`}, Right: pgsql.Param{Value: row.Eeeeeeeeee}})
	}

	if row.Ffffffffff.Status != pgtype.Undefined {
		assignments = append(assignments, &pgsql.Assignment{Left: pgsql.Ident{`ffffffffff`}, Right: pgsql.Param{Value: row.Ffffffffff}})
	}

	if row.Gggggggggg.Status != pgtype.Undefined {
		assignments = append(assignments, &pgsql.Assignment{Left: pgsql.Ident{`gggggggggg`}, Right: pgsql.Param{Value: row.Gggggggggg}})
	}

	if row.Hhhhhhhhhh.Status != pgtype.Undefined {
		assignments = append(assignments, &pgsql.Assignment{Left: pgsql.Ident{`hhhhhhhhhh`}, Right: pgsql.Param{Value: row.Hhhhhhhhhh}})
	}

	if len(assignments) == 0 {
		return nil, errors.New("no attributes to update")
	}

	stmt := pgsql.Update(`widgets`).Set(assignments)
	if row.ID.Status != pgtype.Present {
		return nil, errors.New("primary key not set")
	}
	stmt.Where("id=?", row.ID)
	return stmt, nil
}

func (row *Widget) DeleteStatement() (*pgsql.DeleteStatement, error) {
	stmt := pgsql.Delete(`widgets`)
	if row.ID.Status != pgtype.Present {
		return nil, errors.New("primary key not set")
	}
	stmt.Where("id=?", row.ID)
	return stmt, nil
}

func (row *Widget) SelectStatement() (*pgsql.SelectStatement, error) {
	return pgsql.Select(`widgets.id, widgets.name, widgets.aaaaaaaaaa, widgets.bbbbbbbbbb, widgets.cccccccccc, widgets.dddddddddd, widgets.eeeeeeeeee, widgets.ffffffffff, widgets.gggggggggg, widgets.hhhhhhhhhh`).From(`widgets`), nil
}

type WidgetCollection []*Widget

func (c *WidgetCollection) NewRecord() pgxrecord.Selector {
	return &Widget{}
}

func (c *WidgetCollection) Append(s pgxrecord.Selector) {
	*c = append(*c, s.(*Widget))
}
