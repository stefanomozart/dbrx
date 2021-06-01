package dbrx

import (
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/gocraft/dbr/v2"
	dbrdialect "github.com/gocraft/dbr/v2/dialect"
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
			if (c.err == nil) != (err == nil) {
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
			if (c.err == nil) != (err == nil) {
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
		{
			"With before Select",
			`create table t (id integer primary key, value varchar);
			 insert into t(value) values ('v1'),('v2');`,
			func(dml DML) builder {
				return dml.
					With("v(id,value)", Values(1, "v_1").Values(2, "v_2")).
					Select("v.value a", "t.value b").
					From("v").
					Join("t", "v.id = t.id")
			},
			`WITH v(id,value) AS (VALUES (1,'v_1'),(2,'v_2'))
			 SELECT v.value a, t.value b
			 FROM v JOIN "t" ON v.id = t.id`,
			func(dml DML) (interface{}, error) {
				return nil, nil
			},
			map[interface{}][]interface{}{
				"v_1": {"v1"},
				"v_2": {"v2"},
			},
		},
		{
			"Two withs",
			"",
			func(dml DML) builder {
				return dml.
					With("v1(id,value)", Values(1, "v1").Values(2, "v2")).
					With("v2(id,value)", Values(1, "v3").Values(2, "v4")).
					Select("v1.value", "v2.value").
					From("v1").
					Join("v2", "v1.id = v2.id")
			},
			`WITH v1(id,value) AS (VALUES (1,'v1'),(2,'v2')) ,
			 	v2(id,value) AS (VALUES (1,'v3'),(2,'v4'))
			 SELECT v1.value, v2.value
			 FROM v1 JOIN "v2" ON v1.id = v2.id`,
			func(dml DML) (interface{}, error) {
				return nil, nil
			},
			map[interface{}][]interface{}{
				"v1": {"v3"},
				"v2": {"v4"},
			},
		},
		{
			"Subselect",
			"",
			func(dml DML) builder {
				return dml.
					With("v(id,value)", dbr.Select("id", "value").From("t")).
					Select("v.value").
					From("v")
			},
			`WITH v(id,value) AS (SELECT id, value FROM t)
			 SELECT v.value
			 FROM v`,
			nil,
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
			switch stmt := builder.(type) {
			case *UpdateStmt:
				if c.data != nil && c.assert != nil {
					_, err = stmt.Exec()
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
				}
			case *SelectStmt:
				if c.data != nil {
					data := make(map[interface{}][]interface{})
					_, err := stmt.Load(&data)
					if err != nil {
						t.Error(err)
					}
					if !reflect.DeepEqual(c.data, data) {
						t.Errorf("expected\n%v,\ngot\n%v.", c.data, data)
					}
				}
			}
		})
	}
}

func TestBuild(t *testing.T) {
	conn, err := dbr.Open("sqlite3", ":memory:", nil)
	if err != nil {
		t.Fatal(err)
	}
	dml := Wrap(conn.NewSession(nil))
	cases := []struct {
		name   string
		input  dbr.Builder
		output string
	}{
		{
			"upsert",
			dml.
				InsertInto("t").
				Columns("c").
				Values("v").
				OnConflict("c", dml.Update("t").Set("t", "v")),
			`INSERT INTO "t" ("c") VALUES (?) ON CONFLICT ("c") DO UPDATE "t" SET "t" = ?`,
		},
		{
			"on conflict do nothing",
			dml.
				InsertInto("t").
				Columns("c").
				Values("v").
				OnConflict("", dbr.Expr("nothing")),
			`INSERT INTO "t" ("c") VALUES (?) ON CONFLICT DO nothing`,
		},
	}
	for _, c := range cases {
		buf := dbr.NewBuffer()
		err := c.input.Build(dbrdialect.SQLite3, buf)
		if err != nil {
			t.Error(err)
		}
		if c.output != buf.String() {
			t.Errorf("expected\n%v,\ngot\n%v.", c.output, buf.String())
		}
	}
}

func TestGreatest(t *testing.T) {
	conn, err := dbr.Open("sqlite3", ":memory:", nil)
	if err != nil {
		t.Fatal(err)
	}
	dml := Wrap(conn.NewSession(nil))
	cases := []struct {
		name   string
		input  dbr.Builder
		output string
	}{
		{
			"two values",
			dml.Greatest(1, 2),
			`max(?,?)`,
		},
	}
	for _, c := range cases {
		buf := dbr.NewBuffer()
		err := c.input.Build(dbrdialect.SQLite3, buf)
		if err != nil {
			t.Error(err)
		}
		if c.output != buf.String() {
			t.Errorf("expected\n%v,\ngot\n%v.", c.output, buf.String())
		}
	}
}

func TestUnion(t *testing.T) {
	conn, err := dbr.Open("sqlite3", ":memory:", nil)
	if err != nil {
		t.Fatal(err)
	}
	sess := conn.NewSession(nil)
	_, err = sess.Exec(`
        create table t1 (id int);
        create table t2 (id int);
        insert into t1 values(1);
        insert into t2 values(2);
    `)
	if err != nil {
		panic(err)
	}
	dml := Wrap(sess)
	cases := []struct {
		name  string
		input *UnionStmt
	}{
		{
			"two selects",
			dml.Union(
				dml.Select("*").From("t1").Where("id = ?", 1),
				dml.Select("*").From("t2").Where("id = ?", 2)),
		},
	}
	for _, c := range cases {
		var r []struct{ ID int }
		_, err := c.input.Load(&r)
		if err != nil {
			t.Error(err)
		}
		if len(r) != 2 {
			t.Errorf("expected\ntwo rows,\ngot\n%v.", r)
		}
	}
}

func TestRunAfterCommit(t *testing.T) {
	conn, err := dbr.Open("sqlite3", ":memory:", nil)
	if err != nil {
		t.Fatal(err)
	}
	sess := conn.NewSession(&AfterCommitEventReceiver{})
	dml := Wrap(sess)
	var ok, ok2 bool
	dml.RunAfterCommit(func() { ok = true })
	err = RunInTransaction(dml, func(tx TX) error {
		tx.RunAfterCommit(func() { ok2 = true })
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if !ok || !ok2 {
		t.Errorf("not ok")
	}
}
