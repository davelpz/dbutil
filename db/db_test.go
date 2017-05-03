package db

import (
	"database/sql"
	"fmt"
	"testing"

	_ "github.com/go-sql-driver/mysql"
)

func TestDataBase_Open(t *testing.T) {
	type fields struct {
		url     string
		db      *sql.DB
		tx      *sql.Tx
		stmtMap map[string]*sql.Stmt
	}
	type args struct {
		dbURL string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		// TODO: Add test cases.
		{"open-good", fields{}, args{"root:Baxter5537@/davelpz"}, false},
		{"open-bad", fields{}, args{"dlopez:xxxxxx@/davelpz"}, true},
	}
	for _, tt := range tests {
		fmt.Println(tt.name)
		_ = t.Run(tt.name, func(t *testing.T) {
			d := &DataBase{
				url:     tt.fields.url,
				db:      tt.fields.db,
				tx:      tt.fields.tx,
				stmtMap: tt.fields.stmtMap,
			}
			if err := d.Open("mysql", tt.args.dbURL); (err != nil) != tt.wantErr {
				t.Errorf("DataBase.Open() error = %v, wantErr %v", err, tt.wantErr)
			} else {
				//fmt.Println(err)
			}
		})
	}
}

func TestDataBase_ExecSQL(t *testing.T) {
	d := &DataBase{}

	err := d.Open("mysql", "root:Baxter5537@/unittest")
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()

	err = d.Begin()
	if err != nil {
		t.Fatal(err)
	}

	_, err = d.ExecSQL("CREATE TABLE `nv` (`name` varchar(64) NOT NULL DEFAULT '',`value` varchar(64) NOT NULL DEFAULT '')")
	if err != nil {
		t.Fatal(err)
	}

	err = d.Commit()
	if err != nil {
		t.Fatal(err)
	}

	err = d.Begin()
	if err != nil {
		t.Fatal(err)
	}

	_, err = d.ExecSQL("DROP TABLE `nv`")
	if err != nil {
		t.Fatal(err)
	}

	err = d.Commit()
	if err != nil {
		t.Fatal(err)
	}

}

func TestInserts(t *testing.T) {
	d := &DataBase{}
	err := d.Open("mysql", "root:Baxter5537@/mysql")

	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()

	err = d.Begin()
	if err != nil {
		t.Fatal(err)
	}

	_, err = d.ExecSQL("CREATE DATABASE `unittest`")
	if err != nil {
		t.Fatal(err)
	}

	_, err = d.ExecSQL("CREATE TABLE `unittest.nv` (`name` varchar(64) NOT NULL DEFAULT '',`value` varchar(64) NOT NULL DEFAULT '')")
	if err != nil {
		t.Fatal(err)
	}

	_, err = d.ExecSQL("DROP TABLE `unittest.nv`")
	if err != nil {
		t.Fatal(err)
	}

	_, err = d.ExecSQL("DROP DATABASE `unittest`")
	if err != nil {
		t.Fatal(err)
	}

	err = d.Commit()
	if err != nil {
		t.Fatal(err)
	}
}
