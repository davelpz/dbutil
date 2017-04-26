package goutil

import "errors"

// Batch - holdes batches of two dimensional arrays
type Batch struct {
	rows      []interface{}
	batchSize int
	rowSize   int
	callback  func(rows []interface{}) error
}

// Init - initialize a Batch struct
func (b *Batch) Init(batchSize int, rowSize int, callback func(rows []interface{}) error) {
	b.batchSize = batchSize
	b.rowSize = rowSize
	b.rows = make([]interface{}, 0, batchSize*rowSize)
	b.callback = callback
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
