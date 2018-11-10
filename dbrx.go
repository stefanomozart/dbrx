package dbrx

import "github.com/gocraft/dbr"

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
