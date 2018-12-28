package dbrx

import (
	"time"

	"github.com/gocraft/dbr"
)

// DML is the data manipulation language interface for dbr
type DML interface {
	Select(...string) *dbr.SelectStmt
	InsertInto(string) *dbr.InsertStmt
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
	s.Dialect = dialect{s.Dialect}
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

type innerTransaction struct{ *dbr.Tx }

func (t innerTransaction) Begin() (TX, error)     { return t, nil }
func (innerTransaction) Commit() error            { return nil }
func (innerTransaction) Rollback() error          { return nil }
func (innerTransaction) RollbackUnlessCommitted() {}

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
