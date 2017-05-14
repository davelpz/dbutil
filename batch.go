package dbutil

import (
	"database/sql"
	"errors"
)

// Batch - holdes batches of two dimensional arrays
type Batch struct {
	rows       []interface{}
	batchSize  int
	rowSize    int
	callback   func(rows []interface{}) error
	dbProvider DataBaseProvider
}

// New - initialize a Batch struct
func New(dbProvider DataBaseProvider, batchSize int, rowSize int, callback func(rows []interface{}) error) *Batch {
	b := Batch{}
	b.dbProvider = dbProvider
	b.batchSize = batchSize
	b.rowSize = rowSize
	b.rows = make([]interface{}, 0, batchSize*rowSize)
	b.callback = callback

	return &b
}

// Add - add a row to the Batch struct
func (b *Batch) Add(row []interface{}) error {
	if len(row) != b.rowSize {
		return errors.New("length of row not valid")
	}

	b.rows = append(b.rows, row...)

	if b.batchSize == b.Size() {
		rtn := b.callback(b.rows)
		if rtn != nil {
			return rtn
		}
		b.rows = make([]interface{}, 0, b.batchSize*b.rowSize)
	}

	return nil
}

// Size - return how many rows have been added to this Batch so far
func (b *Batch) Size() int {
	return len(b.rows) / b.rowSize
}

// Clear - reset rows back to zero size
func (b *Batch) Clear() {
	b.rows = make([]interface{}, 0, b.batchSize*b.rowSize)
}

// Call - call the callback function, passing Batch.rows as the argument
func (b *Batch) Call() error {
	return b.callback(b.rows)
}

func (b *Batch) Open(driverName, dbURL string) error {
	return b.dbProvider.Open(driverName, dbURL)
}
func (b *Batch) Close() error {
	return b.dbProvider.Close()
}
func (b *Batch) Begin() error {
	return b.dbProvider.Begin()
}
func (b *Batch) Rollback() error {
	return b.dbProvider.Rollback()
}
func (b *Batch) Commit() error {
	return b.dbProvider.Commit()
}
func (b *Batch) Prepare(name, query string) error {
	return b.dbProvider.Prepare(name, query)
}
func (b *Batch) Exec(name string, args ...interface{}) (sql.Result, error) {
	return b.dbProvider.Exec(name, args...)
}
func (b *Batch) Query(name string, args ...interface{}) (*sql.Rows, error) {
	return b.dbProvider.Query(name, args...)
}
func (b *Batch) CloseStmt(name string) error {
	return b.dbProvider.CloseStmt(name)
}
func (b *Batch) ExecSQL(query string, args ...interface{}) (sql.Result, error) {
	return b.dbProvider.ExecSQL(query, args...)
}
func (b *Batch) QuerySQL(query string, args ...interface{}) (*sql.Rows, error) {
	return b.dbProvider.QuerySQL(query, args...)
}
