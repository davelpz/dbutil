package dbutil

import (
	"encoding/json"
	"fmt"
	"reflect"
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

	_, err = batchDB.ExecSQL("CREATE TABLE nv (`id` int(10) unsigned NOT NULL AUTO_INCREMENT,`name` varchar(64) NOT NULL DEFAULT '',`value` varchar(64) NOT NULL DEFAULT '',PRIMARY KEY (`id`))")
	if err != nil {
		t.Fatal(err)
	}

	err = batchDB.Prepare("insert", "INSERT INTO nv (name,value) VALUES(?,?)")
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 10; i++ {
		_, err = batchDB.Exec("insert", "name "+strconv.Itoa(i), "value "+strconv.Itoa(i))
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

func TestSelects(t *testing.T) {
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

	var rowMap = make(map[string]interface{})

	columns, _ := rows.Columns()
	count := len(columns)

	// types, _ := rows.ColumnTypes()
	// for _, t := range types {
	// 	fmt.Println(t.ScanType())
	// 	fmt.Println(t.DatabaseTypeName())
	// 	fmt.Println(t.Length())
	// 	fmt.Println(t.DecimalSize())
	// }

	for rows.Next() {
		values := make([]interface{}, count)
		valuePtrs := make([]interface{}, count)

		for i := range values {
			valuePtrs[i] = &values[i]
		}

		err = rows.Scan(valuePtrs...)
		if err != nil {
			fmt.Println(err)
			return
		}
		for i, col := range columns {
			var v interface{}

			var val interface{} = values[i]

			var val2 = valuePtrs[i].(*interface{})
			rowMap[col] = *val2

			fmt.Println(reflect.TypeOf(*val2))
			fmt.Println(reflect.ValueOf(*val2))

			// _, ok := val.(int)
			// fmt.Printf("int: %v\n", ok)

			// _, ok = val.(string)
			// fmt.Printf("string: %v\n", ok)

			b, ok := val.([]byte)

			if ok {
				v = string(b)
			} else {
				v = val
			}

			fmt.Println(col, v)
		}
		fmt.Printf("%v\n", rowMap)
		for key, val := range rowMap {
			fmt.Println("Key:", key, "Value Type:", reflect.TypeOf(val))
		}
	}

	err = db.Commit()
	if err != nil {
		t.Fatal(err)
	}

}

func TestSelects2(t *testing.T) {
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

	cols, _ := rows.Columns()
	//columns := make([]interface{}, len(cols))
	columnPointers := make([]interface{}, len(cols))
	columnPointers[0] = new(int)
	columnPointers[1] = new(string)
	columnPointers[2] = new(string)
	// for i := range columns {
	// 	columnPointers[i] = &columns[i]
	// }

	for rows.Next() {
		// Create a slice of interface{}'s to represent each column,
		// and a second slice to contain pointers to each item in the columns slice.

		// Scan the result into the column pointers...
		if err = rows.Scan(columnPointers...); err != nil {
			fmt.Println(err)
		}

		// Create our map, and retrieve the value for each column from the pointers slice,
		// storing it in the map with the name of the column as the key.
		m := make(map[string]interface{})
		for i, colName := range cols {
			// val := columnPointers[i].(*interface{})
			// m[colName] = *val
			val := columnPointers[i]
			m[colName] = val
		}

		// Outputs: map[columnName:value columnName2:value2 columnName3:value3 ...]
		fmt.Println(m)
		js, _ := json.Marshal(m)
		fmt.Println(string(js))
	}

	err = db.Commit()
	if err != nil {
		t.Fatal(err)
	}

}
