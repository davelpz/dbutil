package dbutil

import (
	"encoding/json"
	"fmt"
	"strconv"
	"testing"
)

func TestOpenDatabase(t *testing.T) {
	d, err := OpenDatabase("mysql", "root:Baxter5537@/mysql")

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

	err = d.Commit()
	if err != nil {
		t.Fatal(err)
	}

	err = d.Begin()
	if err != nil {
		t.Fatal(err)
	}

	_, err = d.ExecSQL("CREATE TABLE `unittest.nv` (`name` varchar(64) NOT NULL DEFAULT '',`value` varchar(64) NOT NULL DEFAULT '')")
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

	_, err = d.ExecSQL("DROP TABLE `unittest.nv`")
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

	_, err = d.ExecSQL("DROP DATABASE `unittest`")
	if err != nil {
		t.Fatal(err)
	}

	err = d.Commit()
	if err != nil {
		t.Fatal(err)
	}

}

func TestOpenDatabaseSQLite(t *testing.T) {
	d, err := OpenDatabase("sqlite3", "file:test.db")

	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()

	err = d.Begin()
	if err != nil {
		t.Fatal(err)
	}

	_, err = d.ExecSQL("CREATE TABLE `unittest.nv` (`name` varchar(64) NOT NULL DEFAULT '',`value` varchar(64) NOT NULL DEFAULT '')")
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

	_, err = d.ExecSQL("DROP TABLE `unittest.nv`")
	if err != nil {
		t.Fatal(err)
	}

	err = d.Commit()
	if err != nil {
		t.Fatal(err)
	}
}

func TestDBInserts(t *testing.T) {
	d, err := OpenDatabase("mysql", "root:Baxter5537@/mysql")

	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()

	err = d.Begin()
	if err != nil {
		t.Fatal(err)
	}

	_, err = d.ExecSQL("DROP DATABASE IF EXISTS unittest")
	if err != nil {
		//t.Fatal(err)
		t.Log(err)
	}

	_, err = d.ExecSQL("CREATE DATABASE unittest")
	if err != nil {
		t.Fatal(err)
	}

	_, err = d.ExecSQL("USE unittest")
	if err != nil {
		t.Fatal(err)
	}

	_, err = d.ExecSQL("CREATE TABLE nv (`id` int(10) unsigned NOT NULL AUTO_INCREMENT,`name` varchar(64) NOT NULL DEFAULT '',`value` varchar(64) NOT NULL DEFAULT '',PRIMARY KEY (`id`))")
	if err != nil {
		t.Fatal(err)
	}

	err = d.Prepare("insert", "INSERT INTO nv (name,value) VALUES(?,?)")
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 100; i++ {
		_, err = d.Exec("insert", "name "+strconv.Itoa(i), "value "+strconv.Itoa(i))
		if err != nil {
			t.Fatal(err)
		}
	}

	// _, err = batchDB.ExecSQL("DROP TABLE nv")
	// if err != nil {
	// 	t.Fatal(err)
	// }

	err = d.Commit()
	if err != nil {
		t.Fatal(err)
	}

}

func TestRowsToMap(t *testing.T) {
	db, err := OpenDatabase("mysql", "root:Baxter5537@/unittest")

	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	err = db.Begin()
	if err != nil {
		t.Fatal(err)
	}

	rows, err := db.QuerySQL("select * from unittest.nv")
	if err != nil {
		//t.Fatal(err)
		t.Log(err)
	}

	m, err := RowsToMap(rows, "int,string,string")
	if err != nil {
		t.Log(err)
	}

	js, _ := json.Marshal(m)
	fmt.Println(string(js))

	err = db.Commit()
	if err != nil {
		t.Fatal(err)
	}

}
