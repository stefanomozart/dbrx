package dbrx

import (
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/gocraft/dbr"
	dbrdialect "github.com/gocraft/dbr/dialect"
	_ "github.com/mattn/go-sqlite3"
)

func TestRunInTransaction(t *testing.T) {
	cases := []struct {
		name   string
		input  func(tx TX) error
		assert func(dml DML) bool
		err    error
	}{
		{
			"insert a row, must commit",
			func(tx TX) error {
				tx.InsertInto("t").Columns("s").Values("a").Exec()
				return nil
			},
			func(dml DML) bool {
				ss, err := dml.Select("s").From("t").ReturnStrings()
				return err == nil && len(ss) == 1 && ss[0] == "a"
			},
			nil,
		},
		{
			"insert a row and return an error, must rollback",
			func(tx TX) error {
				tx.InsertInto("t").Columns("s").Values("a").Exec()
				return fmt.Errorf("err")
			},
			func(dml DML) bool {
				ss, err := dml.Select("s").From("t").ReturnStrings()
				return err == nil && len(ss) == 0
			},
			fmt.Errorf("err"),
		},
		{
			"insert rows, two of them in inner transactions, must commit",
			func(tx TX) error {
				tx.InsertInto("t").Columns("s").Values("a").Exec()
				innertx, _ := tx.Begin()
				innertx.InsertInto("t").Columns("s").Values("b").Exec()
				innerinnertx, _ := innertx.Begin()
				innertx.InsertInto("t").Columns("s").Values("c").Exec()
				innerinnertx.Commit()
				innertx.Commit()
				return nil
			},
			func(dml DML) bool {
				ss, err := dml.Select("s").From("t").ReturnStrings()
				return err == nil && reflect.DeepEqual([]string{"a", "b", "c"}, ss)
			},
			nil,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			conn, err := dbr.Open("sqlite3", ":memory:", nil)
			if err != nil {
				t.Fatal(err)
			}
			sess := conn.NewSession(nil)
			dml := Wrap(sess)
			sess.Exec("create table t(id integer primary key, s varchar);")
			err = RunInTransaction(dml, c.input)
			if !c.assert(dml) {
				t.Error("not pass")
			}
			if !reflect.DeepEqual(c.err, err) {
				t.Errorf("expected %v, got %v", c.err, err)
			}
		})
	}
}

func TestReturning(t *testing.T) {
	cases := []struct {
		name  string
		input func(dml DML) *InsertStmt
		err   error
	}{
		{
			"Insert with Returning",
			func(dml DML) *InsertStmt {
				return dml.InsertInto("t").
					Columns("s").
					Values("v").
					Returning("id")
			},
			nil,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			conn, err := dbr.Open("sqlite3", ":memory:", nil)
			if err != nil {
				t.Fatal(err)
			}
			sess := conn.NewSession(nil)
			dml := Wrap(sess)
			sess.Exec("create table t(id integer primary key, s varchar);")
			stmt := c.input(dml)
			result, err := stmt.Exec()
			if !reflect.DeepEqual(c.err, err) {
				t.Errorf("expected %v, got %v", c.err, err)
			}
			id, err := result.LastInsertId()
			if err != nil {
				t.Error(err)
			}
			if id != 1 {
				t.Errorf("expected 1 got %v", id)
			}
		})
	}

}

func TestWith(t *testing.T) {
	type builder interface {
		Build(d dbr.Dialect, buf dbr.Buffer) error
	}
	cases := []struct {
		name   string
		script string
		input  func(DML) builder
		output string
		assert func(DML) (interface{}, error)
		data   interface{}
	}{
		{
			"With before Update",
			`create table t (id integer primary key, value varchar);
			 insert into t(value) values ('v1'),('v2');`,
			func(dml DML) builder {
				return dml.
					With("v(id,value)", Values(1, "v_1").Values(2, "v_2")).
					Update("t").
					Set(
						"value",
						Parens(dml.Select("value").
							From("v").
							Where("v.id = t.id")),
					).
					Where("t.id in ?", dml.Select("id").From("v"))
			},
			`WITH v(id,value) AS (VALUES (1,'v_1'),(2,'v_2'))
			 UPDATE "t" SET "value" = (SELECT value FROM v WHERE (v.id = t.id))
			 WHERE (t.id in (SELECT id FROM v))`,
			func(dml DML) (interface{}, error) {
				m := make(map[int]string)
				_, err := dml.Select("id", "value").From("t").Load(&m)
				return m, err
			},
			map[int]string{1: "v_1", 2: "v_2"},
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			conn, err := dbr.Open("sqlite3", ":memory:", nil)
			if err != nil {
				t.Fatal(err)
			}
			sess := conn.NewSession(nil)
			dml := Wrap(sess)
			builder := c.input(dml)
			buf := dbr.NewBuffer()
			err = builder.Build(dbrdialect.SQLite3, buf)
			if err != nil {
				t.Error(err)
			}
			str, err := dbr.InterpolateForDialect(
				buf.String(),
				buf.Value(),
				dbrdialect.SQLite3,
			)
			if err != nil {
				t.Error(err)
			}
			output := c.output
			output = strings.Replace(output, "\n", "", -1)
			output = strings.Replace(output, "\t", "", -1)
			if output != str {
				t.Errorf("expected\n%v,\ngot\n%v.", output, str)
			}
			_, err = sess.Exec(c.script)
			if err != nil {
				t.Error(err)
			}
			_, err = builder.(*UpdateStmt).Exec()
			if err != nil {
				t.Error(err)
			}
			data, err := c.assert(dml)
			if err != nil {
				t.Error(err)
			}
			if !reflect.DeepEqual(c.data, data) {
				t.Errorf("expected\n%v,\ngot\n%v.", c.data, data)
			}
		})
	}
}
