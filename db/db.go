package db

import (
	"database/sql"
	"errors"

	"log"

	_ "github.com/go-sql-driver/mysql" //needed for mysql drivers
	_ "github.com/mattn/go-sqlite3"    //needed for sqlite3 drivers
)

// DataBase - holds all info on a database connection
type DataBase struct {
	url     string
	db      *sql.DB
	tx      *sql.Tx
	stmtMap map[string]*sql.Stmt
}

// Open - open database connection to db specified by URL
func (d *DataBase) Open(driverName, dbURL string) error {
	d.url = dbURL

	db, err := sql.Open(driverName, d.url)
	if err != nil {
		return err
	}

	//test connection
	err = db.Ping()
	if err != nil {
		return err
	}

	d.db = db

	d.stmtMap = make(map[string]*sql.Stmt)

	return nil
}

// Close - close connection
func (d *DataBase) Close() error {

	// loop and close all prepared statements
	for key := range d.stmtMap {
		err := d.CloseStmt(key)
		if err != nil {
			log.Print(err)
			//return err
		}
	}

	return d.db.Close()
}

// Begin - begin a transaction
func (d *DataBase) Begin() error {
	tx, err := d.db.Begin()
	if err != nil {
		return err
	}
	d.tx = tx

	return nil
}

// ExecSQL - execute a previously prepared sql statement
func (d *DataBase) ExecSQL(query string, args ...interface{}) (sql.Result, error) {
	//log.Printf("Starting ExecSQL\n")
	//defer log.Printf("Done ExecSQL\n")

	return d.tx.Exec(query, args...)
}

// QuerySQL - executes a query that returns rows, typically a SELECT.
func (d *DataBase) QuerySQL(query string, args ...interface{}) (*sql.Rows, error) {

	return d.tx.Query(query, args...)
}

// Rollback - rollback a transaction
func (d *DataBase) Rollback() error {
	//log.Println("Rollback")
	return d.tx.Rollback()
}

// Commit - commit a transaction
func (d *DataBase) Commit() error {
	//log.Println("Commit")
	return d.tx.Commit()
}

// Prepare - prepare a sql statement
func (d *DataBase) Prepare(name, query string) error {
	//log.Printf("Preparing %v:%v\n", name, query)
	//Lets prepart our delete and insert sql statements
	stmt, err := d.tx.Prepare(query)
	if err != nil {
		return err
	}

	d.stmtMap[name] = stmt

	return nil
}

// Exec - execute a previously prepared sql statement
func (d *DataBase) Exec(name string, args ...interface{}) (sql.Result, error) {
	//log.Printf("Starting Exec %v\n", name)
	//defer log.Printf("Done Exec %v\n", name)

	stmt, ok := d.stmtMap[name]

	if !ok {
		return nil, errors.New("statement " + name + " not found")
	}

	return stmt.Exec(args...)
}

// Query - executes a query that returns rows, typically a SELECT.
func (d *DataBase) Query(name string, args ...interface{}) (*sql.Rows, error) {
	//log.Printf("Query'ing %v\n", name)
	stmt, ok := d.stmtMap[name]

	if !ok {
		return nil, errors.New("statement " + name + " not found")
	}

	return stmt.Query(args...)
}

// CloseStmt - close a prepared statement
func (d *DataBase) CloseStmt(name string) error {
	//log.Printf("Closing Stmt %v\n", name)
	stmt, ok := d.stmtMap[name]

	if !ok {
		return errors.New("statement " + name + " not found")
	}

	//delete the statment from the map
	delete(d.stmtMap, name)

	return stmt.Close()
}
