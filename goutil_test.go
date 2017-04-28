package goutil

import "testing"

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
