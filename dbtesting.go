package dbrx

import (
	"fmt"
	"io/ioutil"
	"os"

	dbrdialect "github.com/gocraft/dbr/dialect"
	"github.com/gocraft/dbr/v2"
	_ "github.com/jackc/pgx/v4/stdlib" // driver postgres
	sqlmock "gopkg.in/DATA-DOG/go-sqlmock.v1"
)

// Setup abre conexão de banco, cria o schema definido e
// executa o script passado
func Setup(schema, script string) DML {
	dml := setupTestConn()

	fs, err := os.Open(schema)
	if err != nil {
		panic(err)
	}

	readed, err := ioutil.ReadAll(fs)
	if err != nil {
		panic(err)
	}

	if err := ExecScripts(dml, []string{string(readed), script}); err != nil {
		panic(err)
	}

	return dml
}

func setupTestConn() DML {
	return SetupConn(
		fmt.Sprintf(
			"host=%s port=%s user=%s password=%s dbname=%s sslmode=disable",
			GetEnv("DBTESTING_HOST", "localhost"),
			GetEnv("DBTESTING_PORT", "5432"),
			GetEnv("DBTESTING_USER", "postgres"),
			GetEnv("DBTESTING_PASSWD", ""),
			GetEnv("DBTESTING_DBNAME", "postgres"),
		),
	)
}

// ExecScripts executa comandos de DDL
func ExecScripts(sess DML, scripts []string) error {
	tx, err := sess.Begin()
	defer tx.RollbackUnlessCommitted()

	for _, s := range scripts {
		_, err = tx.UpdateBySql(s).Exec()
		if err != nil {
			return err
		}
	}

	return tx.Commit()
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
