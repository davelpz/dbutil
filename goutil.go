package goutil

import (
	"database/sql"

	"github.com/davelpz/goutil/db"
)

// DataBaseProvider - Interface for all database providers
type DataBaseProvider interface {
	Open(driverName, dbURL string) error
	Close() error
	Begin() error
	Rollback() error
	Commit() error
	Prepare(name, query string) error
	Exec(name string, args ...interface{}) (sql.Result, error)
	Query(name string, args ...interface{}) (*sql.Rows, error)
	CloseStmt(name string) error
	ExecSQL(query string, args ...interface{}) (sql.Result, error)
	QuerySQL(query string, args ...interface{}) (*sql.Rows, error)
}

// OpenDatabase - open a database, returns a DataBaseProvider
func OpenDatabase(driverName, dbURL string) (DataBaseProvider, error) {
	var d DataBaseProvider = &db.DataBase{}
	err := d.Open(driverName, dbURL)
	if err != nil {
		return nil, err
	}

	return d, nil
}
