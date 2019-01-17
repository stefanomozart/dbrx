package dbrx

import (
	"database/sql"
	"time"

	"github.com/gocraft/dbr"
	dbrdialect "github.com/gocraft/dbr/dialect"
)

// DML is the data manipulation language interface for dbr
type DML interface {
	Select(...string) *dbr.SelectStmt
	InsertInto(string) *InsertStmt
	Update(string) *dbr.UpdateStmt
	DeleteFrom(string) *dbr.DeleteStmt
	Begin() (TX, error)
}

// TX represents a db transaction
type TX interface {
	DML
	Commit() error
	Rollback() error
	RollbackUnlessCommitted()
}

type wrapper struct{ *dbr.Session }

// Wrap a *dbr.Session
func Wrap(s *dbr.Session) DML {
	if _, ok := s.Dialect.(dialect); !ok {
		s.Dialect = dialect{s.Dialect}
	}
	return &wrapper{s}
}

func (w *wrapper) Begin() (TX, error) {
	tx, err := w.Session.Begin()
	return outerTransaction{tx}, err
}

type outerTransaction struct{ *dbr.Tx }

func (t outerTransaction) Begin() (TX, error) {
	return innerTransaction{t.Tx}, nil
}

func (t outerTransaction) InsertInto(table string) *InsertStmt {
	return &InsertStmt{t.Tx.InsertInto(table)}
}

type innerTransaction struct{ *dbr.Tx }

func (t innerTransaction) Begin() (TX, error)     { return t, nil }
func (innerTransaction) Commit() error            { return nil }
func (innerTransaction) Rollback() error          { return nil }
func (innerTransaction) RollbackUnlessCommitted() {}

func (t innerTransaction) InsertInto(table string) *InsertStmt {
	return &InsertStmt{t.Tx.InsertInto(table)}
}

// RunInTransaction calls f inside a transaction and rollbacks if it returns an error
func RunInTransaction(dml DML, f func(tx TX) error) error {
	tx, err := dml.Begin()
	if err != nil {
		return err
	}
	defer tx.RollbackUnlessCommitted()
	if err := f(tx); err != nil {
		return err
	}
	tx.Commit()
	return nil
}

func (w *wrapper) InsertInto(table string) *InsertStmt {
	return &InsertStmt{w.Session.InsertInto(table)}
}

// InsertStmt overcomes dbr.InsertStmt limitations
type InsertStmt struct {
	*dbr.InsertStmt
}

// Columns specifies the columns names
func (b *InsertStmt) Columns(column ...string) *InsertStmt {
	b.InsertStmt.Columns(column...)
	return b
}

// Values adds a tuple to be inserted.
// The order of the tuple should match Columns.
func (b *InsertStmt) Values(value ...interface{}) *InsertStmt {
	b.InsertStmt.Values(value...)
	return b
}

// Returning specifies the returning columns for postgres.
func (b *InsertStmt) Returning(column ...string) *InsertStmt {
	if isPostgres(b.Dialect) {
		b.InsertStmt.Returning(column...)
	}
	return b
}

// Exec runs the insert statement
func (b *InsertStmt) Exec() (sql.Result, error) {
	if isPostgres(b.Dialect) && len(b.InsertStmt.ReturnColumn) == 1 {
		var id int64
		err := b.InsertStmt.Load(&id)
		if err != nil {
			return nil, err
		}
		return sqlResult(id), nil
	}
	return b.InsertStmt.Exec()
}

func isPostgres(d dbr.Dialect) bool {
	if dbrxDialect, ok := d.(dialect); ok {
		return dbrxDialect.Dialect == dbrdialect.PostgreSQL
	}
	return d == dbrdialect.PostgreSQL
}

type sqlResult int64

func (r sqlResult) LastInsertId() (int64, error) {
	return int64(r), nil
}

func (r sqlResult) RowsAffected() (int64, error) {
	return 0, nil
}

const (
	timeFormat = "2006-01-02 15:04:05.000000 -07:00"
)

type dialect struct{ dbr.Dialect }

func (d dialect) QuoteIdent(s string) string {
	return d.Dialect.QuoteIdent(s)
}

func (d dialect) EncodeString(s string) string {
	return d.Dialect.EncodeString(s)
}

func (d dialect) EncodeBool(b bool) string {
	return d.Dialect.EncodeBool(b)
}

func (d dialect) EncodeTime(t time.Time) string {
	return `'` + t.Format(timeFormat) + `'`
}

func (d dialect) EncodeBytes(b []byte) string {
	return d.Dialect.EncodeBytes(b)
}

func (d dialect) Placeholder(i int) string {
	return d.Dialect.Placeholder(i)
}
