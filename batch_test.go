package goutil

import (
	"fmt"
	"strconv"
	"testing"
)

func process(rows []interface{}) error {
	fmt.Println("--------------")
	for _, r := range rows {
		fmt.Printf("%v\n", r)
	}

	return nil
}

func TestInit(t *testing.T) {
	d, err := OpenDatabase("mysql", "root:Baxter5537@/mysql")

	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()

	var b = New(d, 5, 6, process)

	if b.Size() != 0 {
		t.Error("Expected 0, got ", b.Size())
	}
}

func TestAdd1(t *testing.T) {
	d, err := OpenDatabase("mysql", "root:Baxter5537@/mysql")

	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()

	var b = New(d, 5, 6, process)

	// add a row with lenght not equal to 6
	row := []interface{}{"0", "1", "a", "b", "c", "d", "e"}
	err = b.Add(row)

	if err == nil {
		t.Error("Expected and error and didn't get one ")
	}
}

func TestAdd2(t *testing.T) {
	d, err := OpenDatabase("mysql", "root:Baxter5537@/mysql")

	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()

	var b = New(d, 5, 6, process)

	for i := 0; i < 14; i++ {
		row := []interface{}{strconv.Itoa(i), "a", "b", "c", "d", "e"}
		b.Add(row)
	}

	if b.Size() != 4 {
		t.Error("Expected 4, got ", b.Size())
	}

	b.Call()

	b.Clear()

	if b.Size() != 0 {
		t.Error("Expected 0, got ", b.Size())
	}

}

func TestInserts(t *testing.T) {
	d, err := OpenDatabase("mysql", "root:Baxter5537@/mysql")

	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()

	var batchDB = New(d, 5, 6, process)
	defer batchDB.Close()

	err = batchDB.Begin()
	if err != nil {
		t.Fatal(err)
	}

	_, err = batchDB.ExecSQL("DROP DATABASE IF EXISTS unittest")
	if err != nil {
		//t.Fatal(err)
		t.Log(err)
	}

	_, err = batchDB.ExecSQL("CREATE DATABASE unittest")
	if err != nil {
		t.Fatal(err)
	}

	_, err = batchDB.ExecSQL("USE unittest")
	if err != nil {
		t.Fatal(err)
	}

	_, err = batchDB.ExecSQL("CREATE TABLE nv (`name` varchar(64) NOT NULL DEFAULT '',`value` varchar(64) NOT NULL DEFAULT '')")
	if err != nil {
		t.Fatal(err)
	}

	err = batchDB.Prepare("insert", "INSERT INTO nv (name,value) VALUES(?,?)")
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 1000; i++ {
		_, err = batchDB.Exec("insert", "a"+strconv.Itoa(i), strconv.Itoa(i))
		if err != nil {
			t.Fatal(err)
		}
	}

	// _, err = batchDB.ExecSQL("DROP TABLE nv")
	// if err != nil {
	// 	t.Fatal(err)
	// }

	err = batchDB.Commit()
	if err != nil {
		t.Fatal(err)
	}

}
