package dbrx

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/gocraft/dbr"
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
