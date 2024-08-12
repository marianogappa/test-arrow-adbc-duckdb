package main

import (
	"context"
	"fmt"
	"strings"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/marianogappa/poc-arrow-duckdb/sqlrunner"
)

func _makeSampleArrowRecord() arrow.Record {
	b := array.NewFloat64Builder(memory.DefaultAllocator)
	b.AppendValues([]float64{1, 2, 3}, nil)
	col := b.NewArray()

	// defer col.Release()
	// defer b.Release()
	// defer batchRecord.Release()

	// Create a record batch with the column
	schema := arrow.NewSchema([]arrow.Field{{Name: "column1", Type: arrow.PrimitiveTypes.Float64}}, nil)
	return array.NewRecord(schema, []arrow.Array{col}, int64(col.Len()))
}

// Helper function to format the value at a specific row index
func _formatValue(col arrow.Array, rowIdx int) string {
	switch col := col.(type) {
	case *array.Int32:
		return fmt.Sprintf("%d", col.Value(rowIdx))
	case *array.Float64:
		return fmt.Sprintf("%f", col.Value(rowIdx))
	case *array.String:
		return col.Value(rowIdx)
	// Add more cases as needed for other types
	default:
		return "?"
	}
}

// Print an arrow record to stdout in a Table format
func _printArrowRecord(rec arrow.Record) {
	schema := rec.Schema()
	numRows := int(rec.NumRows())
	numCols := int(rec.NumCols())

	fmt.Println("----------------------------------")
	fmt.Println("The record I'm about to print has:")
	fmt.Println("NumRows:", numRows)
	fmt.Println("NumCols:", numCols)
	fmt.Println("Schema:", schema)
	fmt.Println()

	// Print the header
	headers := make([]string, numCols)
	for i, field := range schema.Fields() {
		if i >= numCols {
			break
		}
		headers[i] = field.Name
	}
	fmt.Println(strings.Join(headers, "\t"))

	// Print the rows
	for rowIdx := 0; rowIdx < numRows; rowIdx++ {
		row := make([]string, numCols)
		for colIdx := 0; colIdx < numCols; colIdx++ {
			col := rec.Column(colIdx)
			row[colIdx] = _formatValue(col, rowIdx)
		}
		fmt.Println(strings.Join(row, "\t"))
	}
	fmt.Println()
	fmt.Println("I'm done printing the record")
	fmt.Println("----------------------------------")
}

func main() {
	// Create a sample arrow record
	rec := _makeSampleArrowRecord()
	fmt.Println("Record before running SQL:")
	_printArrowRecord(rec)

	// Run SQL
	ctx := context.Background()
	runner, err := sqlrunner.New(ctx)
	if err != nil {
		panic(err)
	}
	defer runner.Close()
	resultRecords, err := runner.RunSQLOnRecord(rec, "SELECT * FROM {{.Table}}")
	if err != nil {
		panic(err)
	}

	// Print the result records
	for _, resultRecord := range resultRecords {
		_printArrowRecord(resultRecord)
	}
}
