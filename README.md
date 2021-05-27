# dbrx - Transaction wrapped dbr

This is a little utility package used to wrap [dbr](github.com/gocrapft/dbr) sessions
with a global transaction. The idea is to simplify the development of microservices, 
releasing the developer from the need of repeating the code necessary to create transactions,
and deal with commit or rollback, for all requests.

## Instalation
Use `go get`

```
go get github.com/stefanomozart/dbrx
```

## Usage

You just need to use `dbr` connect to the database and create a regular session , then use 
the `Wrap(sess *dbr.Session)` function to wrap the session with a global transaction.

```{go}
import (
    "github.com/gocraft/dbr"
    "github.com/stefanomozart/dbrx"
)

func handleRequest() {
    // connect to the database
    conn, err := dbr.Open("pgx", dsn, nil)
    if err != nil {
        panic(err)
    }

    // wrap the session 
    wrappedSession :=  dbrx.Wrap(conn.NewSession(nil))

    // then pass on the wrapped session to your service endpoint

}
```

The package also provides the utility function `SetupConn(dsn string)` that will receive a 
dns string and perform the coneection, create the session and return the wrapped connection:

```
import (
    "github.com/gocraft/dbr"
    "github.com/stefanomozart/dbrx"
)

func handleRequest() {
    dsn := "string with host, port, user, password and dbname"
    // Use dbrx to connect, start the session and wrap it on a transaction
    wrappedSession :=  dbrx.SetupConn(dsn)

    // then pass on the wrapped session to your service endpoint
}
```

If you provide and empty string as the parameter to the `SetupConn` function, it will atempt 
to create a dsn using envoronment variables, as follows:

```
dsn = fmt.Sprintf(
    "host=%s port=%s user=%s password=%s dbname=%s sslmode=disable",
    GetEnv("DB_HOST", "localhost"),
    GetEnv("DB_PORT", "5432"),
    GetEnv("DB_USER", "postgres"),
    GetEnv("DB_PASSWD", ""),
    GetEnv("DB_DBNAME", "postgres"),
)
```

## Supported database divres
This package was written especifically to be used with the `postgres` or the
`pgx` database drivers. It can was be used with the `SQLite3` driver for 
testing.