package database

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
	var b Batch
	b.Init(5, 6, process)

	if b.Size() != 0 {
		t.Error("Expected 0, got ", b.Size())
	}
}

func TestAdd1(t *testing.T) {
	var b Batch
	b.Init(5, 6, process)

	// add a row with lenght not equal to 6
	row := []interface{}{"0", "1", "a", "b", "c", "d", "e"}
	err := b.Add(row)

	if err == nil {
		t.Error("Expected and error and didn't get one ")
	}
}

func TestAdd2(t *testing.T) {
	var b Batch
	b.Init(5, 6, process)

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
