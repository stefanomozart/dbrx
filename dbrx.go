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

type session struct{ *dbr.Session }

// NewSession wraps a *dbr.Session
func NewSession(s *dbr.Session) DML {
	return &session{s}
}

func (s *session) Begin() (TX, error) {
	tx, err := s.Session.Begin()
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
