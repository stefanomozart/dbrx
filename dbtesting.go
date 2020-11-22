package dbrx

import (
	"io"
	"io/ioutil"
	"os"
	"strings"

	"github.com/gocraft/dbr"
	dbrdialect "github.com/gocraft/dbr/dialect"
	_ "github.com/mattn/go-sqlite3" // import necessário para executar os scripts
	sqlmock "gopkg.in/DATA-DOG/go-sqlmock.v1"
)

// Setup Configura banco de dados em memoria com o schema definido
// e o script passado
func Setup(script string) *dbr.Session {
	sess, _ := SetupConn(script)
	return sess
}

// SetupConn Configura banco de dados em memoria com o schema definido
// e o script passado
func SetupConn(script string) (*dbr.Session, *dbr.Connection) {
	schema, err := os.Open("schema.sql")
	if err != nil {
		panic(err)
	}
	return ExecScripts(schema, strings.NewReader(script))
}

// ExecScripts abre conexão de banco, executa comandos de DDL e retorna sessão
func ExecScripts(readers ...io.Reader) (*dbr.Session, *dbr.Connection) {
	conn, err := dbr.Open("sqlite3", ":memory:?parseTime=true", nil)

	if err != nil {
		panic(err)
	}
	sess := conn.NewSession(nil)

	tx, err := sess.Begin()
	defer tx.RollbackUnlessCommitted()
	for _, r := range readers {
		readed, err := ioutil.ReadAll(r)
		if err != nil {
			panic(err)
		}
		_, err = tx.Exec(string(readed))
		if err != nil {
			panic(err)
		}
	}
	err = tx.Commit()
	if err != nil {
		panic(err)
	}
	return sess, conn
}

// SetupMock abre uma conexão de bd mock que permite capturar os comandos
// enviados ao banco
func SetupMock() (*dbr.Session, sqlmock.Sqlmock) {
	db, mock, err := sqlmock.New()
	if err != nil {
		panic(err)
	}
	conn := &dbr.Connection{
		DB:            db,
		EventReceiver: &dbr.NullEventReceiver{},
		Dialect:       dbrdialect.SQLite3,
	}
	return conn.NewSession(nil), mock
}
