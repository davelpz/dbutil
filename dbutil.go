package dbutil

import (
	"database/sql"

	"strings"

	"errors"

	"github.com/davelpz/dbutil/db"
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

// RowsToMap - Convert a sql.Rows to a generic slice of maps
func RowsToMap(rows *sql.Rows, typeString string) ([]map[string]interface{}, error) {
	arr := make([]map[string]interface{}, 0)

	cols, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	//Set up valuePointers slice using types from typeString
	types := strings.Split(typeString, ",")
	valuePointers := make([]interface{}, len(types))
	for i, t := range types {
		if t == "int" {
			valuePointers[i] = new(int)
		} else if t == "string" {
			valuePointers[i] = new(string)
		} else {
			return nil, errors.New("Unknown type in typeString")
		}
	}

	for rows.Next() {
		// Scan the result into the value pointers...
		if err := rows.Scan(valuePointers...); err != nil {
			return nil, err
		}

		m := make(map[string]interface{})
		for i, colName := range cols {
			m[colName] = valuePointers[i]
		}

		arr = append(arr, m)
	}

	return arr, nil
}
