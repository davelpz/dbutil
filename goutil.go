package goutil

import (
	"database/sql"

	"github.com/davelpz/goutil/mysql"
)

// DataBaseProvider - Interface for all database providers
type DataBaseProvider interface {
	Open(dbURL string) error
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
func OpenDatabase(dbURL string) (DataBaseProvider, error) {
	var d DataBaseProvider = &mysql.DataBase{}
	err := d.Open(dbURL)
	if err != nil {
		return nil, err
	}

	return d, nil
}
