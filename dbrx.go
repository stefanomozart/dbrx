package dbrx

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/gocraft/dbr/v2"
	dbrdialect "github.com/gocraft/dbr/v2/dialect"
)

const (
	placeholder = "?"
)

// SetupConn abre conexão de banco
func SetupConn(dsn string) DML {
	if len(dsn) == 0 {
		dsn = fmt.Sprintf(
			"host=%s port=%s user=%s password=%s dbname=%s sslmode=disable",
			GetEnv("DB_HOST", "localhost"),
			GetEnv("DB_PORT", "5432"),
			GetEnv("DB_USER", "postgres"),
			GetEnv("DB_PASSWD", ""),
			GetEnv("DB_DBNAME", "postgres"),
		)
	}

	conn, err := dbr.Open("pgx", dsn, nil)
	if err != nil {
		panic(err)
	}

	idleConns, _ := strconv.Atoi(GetEnv("MAX_IDLE_CONNS", "3"))
	conn.SetMaxIdleConns(idleConns)

	openConns, _ := strconv.Atoi(GetEnv("MAX_OPEN_CONNS", "30"))
	conn.SetMaxOpenConns(openConns)

	return Wrap(conn.NewSession(nil))
}

// DML is the data manipulation language interface for dbr
type DML interface {
	Select(...string) *SelectStmt
	InsertInto(string) *InsertStmt
	Update(string) *UpdateStmt
	DeleteFrom(string) *dbr.DeleteStmt
	Begin() (TX, error)
	Exec(sql string, args ...interface{}) (sql.Result, error)
	With(name string, builder dbr.Builder) DML
	Greatest(value ...interface{}) dbr.Builder
	Union(builders ...dbr.Builder) *UnionStmt
	RunAfterCommit(func()) error
	UpdateBySql(sql string) *dbr.UpdateBuilder
	SelectBySql(sql string, value ...interface{}) *dbr.SelectBuilder
	InsertBySql(sql string, value ...interface{}) *dbr.InsertStmt
	TranslateString(text, regex, replace string) string
	Translate(text, regex, replace string) dbr.Builder
}

// TX represents a db transaction
type TX interface {
	DML
	Commit() error
	Rollback() error
	RollbackUnlessCommitted()
}

type AfterCommitEventReceiver struct {
	*dbr.NullEventReceiver
	funcs []func()
}

func (er *AfterCommitEventReceiver) Add(f func()) {
	er.funcs = append(er.funcs, f)
}

func (er *AfterCommitEventReceiver) Event(eventName string) {
	if eventName != "dbr.commit" {
		return
	}
	for _, f := range er.funcs {
		f()
	}
}

type withClauses []withClause

type wrapper struct {
	Session     *dbr.Session
	withClauses withClauses
}

type withClause struct {
	name    string
	builder dbr.Builder
}

func (ws withClauses) write(d dbr.Dialect, buf dbr.Buffer) error {
	if len(ws) == 0 {
		return nil
	}
	buf.WriteString("WITH ")
	for i, w := range ws {
		if i > 0 {
			buf.WriteString(", ")
		}
		buf.WriteString(w.name)
		buf.WriteString(" AS (")
		err := w.builder.Build(d, buf)
		if err != nil {
			return err
		}
		buf.WriteString(") ")
	}
	return nil
}

func newWrapper(s *dbr.Session) *wrapper {
	if _, ok := s.Dialect.(dialect); !ok {
		s.Dialect = dialect{s.Dialect}
	}
	return &wrapper{Session: s}
}

// Wrap a *dbr.Session
func Wrap(s *dbr.Session) DML {
	return newWrapper(s)
}

func (w *wrapper) Exec(sql string, args ...interface{}) (sql.Result, error) {
	return w.Session.Exec(sql, args...)
}

func (w *wrapper) Begin() (TX, error) {
	tx, err := w.Session.Begin()
	return outerTransaction{Tx: tx, w: w}, err
}

func (w *wrapper) With(name string, builder dbr.Builder) DML {
	w.withClauses = append(w.withClauses, withClause{name, builder})
	return w
}

func (w *wrapper) Select(column ...string) *SelectStmt {
	stmt := &SelectStmt{w.Session.Select(column...), w.withClauses, w}
	w.withClauses = nil
	return stmt
}

func (w *wrapper) InsertInto(table string) *InsertStmt {
	return &InsertStmt{InsertStmt: w.Session.InsertInto(table), dml: w}
}

func (w *wrapper) Update(table string) *UpdateStmt {
	stmt := &UpdateStmt{w.Session.Update(table), w.withClauses, w}
	w.withClauses = nil
	return stmt
}

func (w *wrapper) DeleteFrom(sql string) *dbr.DeleteStmt {
	return w.Session.DeleteFrom(sql)
}

func (w *wrapper) UpdateBySql(sql string) *dbr.UpdateBuilder {
	return w.Session.UpdateBySql(sql)
}

func (w *wrapper) SelectBySql(sql string, value ...interface{}) *dbr.SelectBuilder {
	return w.Session.SelectBySql(sql, value...)
}

func (w *wrapper) InsertBySql(sql string, value ...interface{}) *dbr.InsertStmt {
	return w.Session.InsertBySql(sql, value...)
}

func (w *wrapper) Union(builders ...dbr.Builder) *UnionStmt {
	return &UnionStmt{builders, w, false, w.Session.Dialect}
}

func (w *wrapper) RunAfterCommit(f func()) error {
	type funcAdder interface{ Add(func()) }
	if fa, ok := w.Session.EventReceiver.(funcAdder); !ok {
		return errors.New("session does not have a AfterCommitEventReceiver")
	} else {
		fa.Add(f)
	}
	return nil
}

type outerTransaction struct {
	*dbr.Tx
	withClauses withClauses
	w           *wrapper
}

func (t outerTransaction) Begin() (TX, error) {
	return innerTransaction{Tx: t.Tx}, nil
}

func (t outerTransaction) Select(columns ...string) *SelectStmt {
	return &SelectStmt{t.Tx.Select(columns...), t.withClauses, t}
}

func (t outerTransaction) InsertInto(table string) *InsertStmt {
	return &InsertStmt{InsertStmt: t.Tx.InsertInto(table), dml: t}
}

func (t outerTransaction) Update(table string) *UpdateStmt {
	return &UpdateStmt{t.Tx.Update(table), t.withClauses, t}
}

func (t outerTransaction) With(name string, builder dbr.Builder) DML {
	t.withClauses = append(t.withClauses, withClause{name, builder})
	return t
}

func (t outerTransaction) Greatest(value ...interface{}) dbr.Builder {
	return Greatest(t.Tx.Dialect, value...)
}

func (t outerTransaction) Union(builders ...dbr.Builder) *UnionStmt {
	return &UnionStmt{builders, t, false, t.Tx.Dialect}
}

func (t outerTransaction) RunAfterCommit(f func()) error {
	return t.w.RunAfterCommit(f)
}

func (t outerTransaction) SelectBySql(sql string, value ...interface{}) *dbr.SelectBuilder {
	return t.Tx.SelectBySql(sql, value...)
}

func (t outerTransaction) UpdateBySql(sql string) *dbr.UpdateBuilder {
	return t.Tx.UpdateBySql(sql)
}

func (t outerTransaction) InsertBySql(sql string, value ...interface{}) *dbr.InsertStmt {
	return t.Tx.InsertBySql(sql, value...)
}

func (t outerTransaction) TranslateString(text, regex, replace string) string {
	return TranslateString(t.Tx.Dialect, text, regex, replace)
}

func (t outerTransaction) Translate(text, regex, replace string) dbr.Builder {
	return Translate(t.Tx.Dialect, text, regex, replace)
}

type innerTransaction struct {
	*dbr.Tx
	withClauses withClauses
	w           *wrapper
}

func (t innerTransaction) Begin() (TX, error)     { return t, nil }
func (innerTransaction) Commit() error            { return nil }
func (innerTransaction) Rollback() error          { return nil }
func (innerTransaction) RollbackUnlessCommitted() {}

func (t innerTransaction) Select(columns ...string) *SelectStmt {
	return &SelectStmt{t.Tx.Select(columns...), t.withClauses, t}
}

func (t innerTransaction) InsertInto(table string) *InsertStmt {
	return &InsertStmt{InsertStmt: t.Tx.InsertInto(table), dml: t}
}

func (t innerTransaction) Update(table string) *UpdateStmt {
	return &UpdateStmt{t.Tx.Update(table), t.withClauses, t}
}

func (t innerTransaction) With(name string, builder dbr.Builder) DML {
	t.withClauses = append(t.withClauses, withClause{name, builder})
	return t
}

func (t innerTransaction) Greatest(value ...interface{}) dbr.Builder {
	return Greatest(t.Tx.Dialect, value...)
}

func (t innerTransaction) Union(builders ...dbr.Builder) *UnionStmt {
	return &UnionStmt{builders, t, false, t.Tx.Dialect}
}

func (t innerTransaction) RunAfterCommit(f func()) error {
	return t.w.RunAfterCommit(f)
}

func (t innerTransaction) SelectBySql(sql string, value ...interface{}) *dbr.SelectBuilder {
	return t.Tx.SelectBySql(sql)
}

func (t innerTransaction) UpdateBySql(sql string) *dbr.UpdateBuilder {
	return t.Tx.UpdateBySql(sql)
}

func (t innerTransaction) InsertBySql(sql string, value ...interface{}) *dbr.InsertStmt {
	return t.Tx.InsertBySql(sql, value...)
}

func (t innerTransaction) TranslateString(text, regex, replace string) string {
	return TranslateString(t.Tx.Dialect, text, regex, replace)
}

func (t innerTransaction) Translate(text, regex, replace string) dbr.Builder {
	return Translate(t.Tx.Dialect, text, regex, replace)
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

// SelectStmt overcomes dbr.SelectStmt limitations
type SelectStmt struct {
	*dbr.SelectStmt
	withClauses withClauses
	dml         DML
}

// Build calls itself to build SQL.
func (b *SelectStmt) Build(d dbr.Dialect, buf dbr.Buffer) error {
	err := b.withClauses.write(d, buf)
	if err != nil {
		return err
	}
	return b.SelectStmt.Build(d, buf)
}

func (b *SelectStmt) From(table interface{}) *SelectStmt {
	b.SelectStmt.From(table)
	return b
}

func (b *SelectStmt) Join(table, on interface{}) *SelectStmt {
	b.SelectStmt.Join(table, on)
	return b
}

func (b *SelectStmt) LeftJoin(table, on interface{}) *SelectStmt {
	b.SelectStmt.LeftJoin(table, on)
	return b
}

func (b *SelectStmt) Where(query interface{}, value ...interface{}) *SelectStmt {
	b.SelectStmt.Where(query, value...)
	return b
}

func (b *SelectStmt) OrderBy(col string) *SelectStmt {
	b.SelectStmt.OrderBy(col)
	return b
}

func (b *SelectStmt) OrderAsc(col string) *SelectStmt {
	b.SelectStmt.OrderAsc(col)
	return b
}

func (b *SelectStmt) OrderDesc(col string) *SelectStmt {
	b.SelectStmt.OrderDesc(col)
	return b
}

func (b *SelectStmt) Load(value interface{}) (int, error) {
	if len(b.withClauses) == 0 {
		return b.SelectStmt.Load(value)
	}
	buf := dbr.NewBuffer()
	err := b.Build(b.Dialect, buf)
	if err != nil {
		return 0, err
	}
	str, err := dbr.InterpolateForDialect(
		buf.String(),
		buf.Value(),
		b.Dialect,
	)
	if err != nil {
		return 0, err
	}
	return b.dml.SelectBySql(str).Load(value)
}

// InsertStmt overcomes dbr.InsertStmt limitations
type InsertStmt struct {
	*dbr.InsertStmt
	onConflict bool
	name       interface{}
	do         dbr.Builder
	dml        DML
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
	if !b.onConflict {
		return b.InsertStmt.Exec()
	}
	sql, err := b.interpolate()
	if err != nil {
		return nil, err
	}
	return b.dml.InsertBySql(sql).Exec()
}

// ExecContext runs the insert statement
func (b *InsertStmt) ExecContext(ctx context.Context) (sql.Result, error) {
	if isPostgres(b.Dialect) && len(b.InsertStmt.ReturnColumn) == 1 {
		var id int64
		err := b.InsertStmt.LoadContext(ctx, &id)
		if err != nil {
			return nil, err
		}
		return sqlResult(id), nil
	}
	if !b.onConflict {
		return b.InsertStmt.ExecContext(ctx)
	}
	sql, err := b.interpolate()
	if err != nil {
		return nil, err
	}
	return b.dml.InsertBySql(sql).ExecContext(ctx)
}

// OnConflict implements the ON CONFLICT clause
func (b *InsertStmt) OnConflict(name interface{}, do dbr.Builder) *InsertStmt {
	b.onConflict = true
	b.name = name
	b.do = do
	return b
}

// Build calls itself to build SQL.
func (b *InsertStmt) Build(d dbr.Dialect, buf dbr.Buffer) error {
	err := b.InsertStmt.Build(d, buf)
	if err != nil {
		return err
	}
	if b.onConflict {
		buf.WriteString(" ON CONFLICT")
		if b.name != "" && b.name != nil {
			var names []string
			name, ok := b.name.(string)
			if ok {
				names = []string{name}
			} else {
				names = b.name.([]string)
			}
			buf.WriteString(" (")
			for i, n := range names {
				if i > 0 {
					buf.WriteString(",")
				}
				buf.WriteString(d.QuoteIdent(n))
			}
			buf.WriteString(")")
		}
		buf.WriteString(" DO ")
		b.do.Build(d, buf)
	}
	return nil
}

func (b *InsertStmt) interpolate() (str string, err error) {
	buf := dbr.NewBuffer()
	err = b.Build(b.Dialect, buf)
	if err != nil {
		return "", err
	}
	return dbr.InterpolateForDialect(
		buf.String(),
		buf.Value(),
		b.Dialect,
	)
}

// UpdateStmt overcomes dbr.UpdateStmt limitations
type UpdateStmt struct {
	*dbr.UpdateStmt
	withClauses withClauses
	dml         DML
}

// Build calls itself to build SQL.
func (b *UpdateStmt) Build(d dbr.Dialect, buf dbr.Buffer) error {
	err := b.withClauses.write(d, buf)
	if err != nil {
		return err
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
	if len(b.withClauses) == 0 {
		return b.UpdateStmt.Exec()
	}
	str, err := b.interpolateWithClause()
	if err != nil {
		return nil, err
	}
	return b.dml.UpdateBySql(str).Exec()
}

// ExecContext runs the update statement
func (b *UpdateStmt) ExecContext(ctx context.Context) (sql.Result, error) {
	if len(b.withClauses) == 0 {
		return b.UpdateStmt.ExecContext(ctx)
	}
	str, err := b.interpolateWithClause()
	if err != nil {
		return nil, err
	}
	return b.dml.UpdateBySql(str).ExecContext(ctx)
}

func (b *UpdateStmt) interpolateWithClause() (str string, err error) {
	buf := dbr.NewBuffer()
	err = b.Build(b.Dialect, buf)
	if err != nil {
		return "", err
	}
	return dbr.InterpolateForDialect(
		buf.String(),
		buf.Value(),
		b.Dialect,
	)
}

// Returning specifies the returning columns for postgres.
func (b *UpdateStmt) Returning(column ...string) *UpdateStmt {
	if isPostgres(b.Dialect) {
		b.UpdateStmt.Returning(column...)
	}
	return b
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

func (w *wrapper) TranslateString(text, regex, replace string) string {
	return TranslateString(w.Session.Dialect, text, regex, replace)
}

func (w *wrapper) Translate(text, regex, replace string) dbr.Builder {
	return Translate(w.Session.Dialect, text, regex, replace)
}

func TranslateString(d dbr.Dialect, text, regex, replace string) string {
	if isPostgres(d) {
		return fmt.Sprintf("translate(%s, '%s', '%s')", text, regex, replace)
	}
	final := text
	for i, c := range regex {
		final = fmt.Sprintf("replace(%s, '%c', '%c')", final, c, replace[i/2])
	}
	return final
}

func Translate(d dbr.Dialect, text, regex, replace string) dbr.Builder {
	return dbr.Expr(TranslateString(d, text, regex, replace))
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

func DoUpdate() *DoUpdateBuilder {
	return &DoUpdateBuilder{
		Value: make(map[string]interface{}),
	}
}

type DoUpdateBuilder struct {
	Value     map[string]interface{}
	WhereCond []dbr.Builder
}

func (b *DoUpdateBuilder) Build(d dbr.Dialect, buf dbr.Buffer) error {
	buf.WriteString("UPDATE ")
	buf.WriteString(" SET ")

	i := 0
	for col, v := range b.Value {
		if i > 0 {
			buf.WriteString(", ")
		}
		buf.WriteString(d.QuoteIdent(col))
		buf.WriteString(" = ")
		buf.WriteString(placeholder)

		buf.WriteValue(v)
		i++
	}

	if len(b.WhereCond) > 0 {
		buf.WriteString(" WHERE ")
		err := dbr.And(b.WhereCond...).Build(d, buf)
		if err != nil {
			return err
		}
	}

	return nil
}

func (b *DoUpdateBuilder) Set(column string, expr interface{}) *DoUpdateBuilder {
	b.Value[column] = expr
	return b
}

func (b *DoUpdateBuilder) Where(query interface{}, value ...interface{}) *DoUpdateBuilder {
	switch query := query.(type) {
	case string:
		b.WhereCond = append(b.WhereCond, dbr.Expr(query, value...))
	case dbr.Builder:
		b.WhereCond = append(b.WhereCond, query)
	}
	return b
}

func (w *wrapper) Greatest(value ...interface{}) dbr.Builder {
	return Greatest(w.Session.Dialect, value...)
}

func Greatest(d dbr.Dialect, value ...interface{}) dbr.Builder {
	placeholders := strings.Repeat("?,", len(value))
	placeholders = placeholders[0 : len(placeholders)-1]
	if isPostgres(d) {
		return dbr.Expr(fmt.Sprintf("greatest(%v)", placeholders), value...)
	}
	return dbr.Expr(fmt.Sprintf("max(%v)", placeholders), value...)
}

type UnionStmt struct {
	builders []dbr.Builder
	dml      DML
	all      bool
	dialect  dbr.Dialect
}

func (us *UnionStmt) Load(value interface{}) (int, error) {
	return us.LoadContext(context.Background(), value)
}

func (us *UnionStmt) LoadContext(ctx context.Context, value interface{}) (int, error) {
	var buf = dbr.NewBuffer()
	for i, b := range us.builders {
		if i > 0 {
			buf.WriteString(" UNION ")
			if us.all {
				buf.WriteString("ALL ")
			}
		}
		err := b.Build(us.dialect, buf)
		if err != nil {
			return 0, err
		}
	}
	str, err := dbr.InterpolateForDialect(
		buf.String(),
		buf.Value(),
		us.dialect,
	)
	if err != nil {
		return 0, err
	}
	return us.dml.SelectBySql(str).LoadContext(ctx, value)
}

type MultipleEventReceiver []dbr.EventReceiver

func (ers MultipleEventReceiver) Event(eventName string) {
	for _, er := range ers {
		if er == nil {
			continue
		}
		er.Event(eventName)
	}
}

func (ers MultipleEventReceiver) EventKv(eventName string, kvs map[string]string) {
	for _, er := range ers {
		if er == nil {
			continue
		}
		er.EventKv(eventName, kvs)
	}
}

func (ers MultipleEventReceiver) EventErr(eventName string, err error) error {
	for _, er := range ers {
		if er == nil {
			continue
		}
		er.EventErr(eventName, err)
	}
	return err
}

func (ers MultipleEventReceiver) EventErrKv(eventName string, err error, kvs map[string]string) error {
	for _, er := range ers {
		if er == nil {
			continue
		}
		er.EventErrKv(eventName, err, kvs)
	}
	return err
}

func (ers MultipleEventReceiver) Timing(eventName string, nanoseconds int64) {
	for _, er := range ers {
		if er == nil {
			continue
		}
		er.Timing(eventName, nanoseconds)
	}
}

func (ers MultipleEventReceiver) TimingKv(eventName string, nanoseconds int64, kvs map[string]string) {
	for _, er := range ers {
		if er == nil {
			continue
		}
		er.TimingKv(eventName, nanoseconds, kvs)
	}
}

func (ers MultipleEventReceiver) Add(fn func()) {
	type funcAdder interface{ Add(func()) }
	var added bool
	for _, er := range ers {
		if er == nil {
			continue
		}
		if fa, ok := er.(funcAdder); ok {
			fa.Add(fn)
			added = true
		}
	}
	if !added {
		panic("session does not have a AfterCommitEventReceiver between its event receivers")
	}
}

// GetEnv returns the environment variable, if it is set, or the provided
// default value
func GetEnv(varName, defaultVal string) string {
	val := os.Getenv(varName)
	if len(val) == 0 {
		return defaultVal
	}
	return val
}
