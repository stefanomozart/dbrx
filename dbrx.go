package dbrx

import (
	"database/sql"
	"time"

	"github.com/gocraft/dbr"
	dbrdialect "github.com/gocraft/dbr/dialect"
)

const (
	placeholder = "?"
)

// DML is the data manipulation language interface for dbr
type DML interface {
	Select(...string) *dbr.SelectStmt
	InsertInto(string) *InsertStmt
	Update(string) *UpdateStmt
	DeleteFrom(string) *dbr.DeleteStmt
	Begin() (TX, error)
	With(name string, builder dbr.Builder) DML
	updateBySql(sql string) *dbr.UpdateBuilder
}

// TX represents a db transaction
type TX interface {
	DML
	Commit() error
	Rollback() error
	RollbackUnlessCommitted()
}

type wrapper struct {
	*dbr.Session
	withClause withClause
}

type withClause struct {
	name    string
	builder dbr.Builder
}

// Wrap a *dbr.Session
func Wrap(s *dbr.Session) DML {
	if _, ok := s.Dialect.(dialect); !ok {
		s.Dialect = dialect{s.Dialect}
	}
	return &wrapper{Session: s}
}

func (w *wrapper) Begin() (TX, error) {
	tx, err := w.Session.Begin()
	return outerTransaction{Tx: tx}, err
}

func (w *wrapper) With(name string, builder dbr.Builder) DML {
	w.withClause.name = name
	w.withClause.builder = builder
	return w
}

func (w *wrapper) InsertInto(table string) *InsertStmt {
	return &InsertStmt{w.Session.InsertInto(table)}
}

func (w *wrapper) Update(table string) *UpdateStmt {
	return &UpdateStmt{w.Session.Update(table), w.withClause, w}
}

func (w *wrapper) updateBySql(sql string) *dbr.UpdateBuilder {
	return w.Session.UpdateBySql(sql)
}

type outerTransaction struct {
	*dbr.Tx
	withClause withClause
}

func (t outerTransaction) Begin() (TX, error) {
	return innerTransaction{Tx: t.Tx}, nil
}

func (t outerTransaction) InsertInto(table string) *InsertStmt {
	return &InsertStmt{t.Tx.InsertInto(table)}
}

func (t outerTransaction) Update(table string) *UpdateStmt {
	return &UpdateStmt{t.Tx.Update(table), t.withClause, t}
}

func (t outerTransaction) With(name string, builder dbr.Builder) DML {
	t.withClause.name = name
	t.withClause.builder = builder
	return t
}

func (t outerTransaction) updateBySql(sql string) *dbr.UpdateBuilder {
	return t.Tx.UpdateBySql(sql)
}

type innerTransaction struct {
	*dbr.Tx
	withClause withClause
}

func (t innerTransaction) Begin() (TX, error)     { return t, nil }
func (innerTransaction) Commit() error            { return nil }
func (innerTransaction) Rollback() error          { return nil }
func (innerTransaction) RollbackUnlessCommitted() {}

func (t innerTransaction) InsertInto(table string) *InsertStmt {
	return &InsertStmt{t.Tx.InsertInto(table)}
}

func (t innerTransaction) Update(table string) *UpdateStmt {
	return &UpdateStmt{t.Tx.Update(table), t.withClause, t}
}

func (t innerTransaction) With(name string, builder dbr.Builder) DML {
	t.withClause.name = name
	t.withClause.builder = builder
	return t
}

func (t innerTransaction) updateBySql(sql string) *dbr.UpdateBuilder {
	return t.Tx.UpdateBySql(sql)
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

// UpdateStmt overcomes dbr.UpdateStmt limitations
type UpdateStmt struct {
	*dbr.UpdateStmt
	withClause withClause
	dml        DML
}

// Build calls itself to build SQL.
func (b *UpdateStmt) Build(d dbr.Dialect, buf dbr.Buffer) error {
	if b.withClause.name != "" {
		buf.WriteString("WITH ")
		buf.WriteString(b.withClause.name)
		buf.WriteString(" AS (")
		err := b.withClause.builder.Build(d, buf)
		if err != nil {
			return err
		}
		buf.WriteString(") ")
	}
	return b.UpdateStmt.Build(d, buf)
}

// Set updates column with value.
func (b *UpdateStmt) Set(column string, value interface{}) *UpdateStmt {
	b.UpdateStmt.Set(column, value)
	return b
}

// Where adds a where condition.
// query can be Builder or string. value is used only if query type is string.
func (b *UpdateStmt) Where(query interface{}, value ...interface{}) *UpdateStmt {
	b.UpdateStmt.Where(query, value)
	return b
}

// Exec runs the update statement
func (b *UpdateStmt) Exec() (sql.Result, error) {
	buf := dbr.NewBuffer()
	err := b.Build(b.Dialect, buf)
	if err != nil {
		return nil, err
	}
	str, err := dbr.InterpolateForDialect(
		buf.String(),
		buf.Value(),
		b.Dialect,
	)
	if err != nil {
		return nil, err
	}
	return b.dml.updateBySql(str).Exec()
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

func Parens(b dbr.Builder) dbr.Builder {
	return parensBuilder{b}
}

type parensBuilder struct {
	dbr.Builder
}

func (b parensBuilder) Build(d dbr.Dialect, buf dbr.Buffer) error {
	buf.WriteString("(")
	err := b.Builder.Build(d, buf)
	if err != nil {
		return err
	}
	buf.WriteString(")")
	return nil
}

func Values(v ...interface{}) *ValuesExpr {
	return &ValuesExpr{[][]interface{}{v}}
}

type ValuesExpr struct {
	values [][]interface{}
}

func (e *ValuesExpr) Values(v ...interface{}) *ValuesExpr {
	if e == nil {
		return Values(v...)
	}
	e.values = append(e.values, v)
	return e
}

func (e *ValuesExpr) Build(d dbr.Dialect, buf dbr.Buffer) error {
	buf.WriteString("VALUES ")
	for i, values := range e.values {
		if len(values) == 0 {
			continue
		}
		if i > 0 {
			buf.WriteString(",")
		}
		buf.WriteString("(")
		for j, value := range values {
			if j > 0 {
				buf.WriteString(",")
			}
			buf.WriteString(placeholder)
			buf.WriteValue(value)
		}
		buf.WriteString(")")
	}
	return nil
}
