package main

import (
	"context"
	"fmt"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/marianogappa/poc-arrow-duckdb/sqlrunner"
)

func _makeSampleArrowRecord() arrow.Record {
	b := array.NewFloat64Builder(memory.DefaultAllocator)
	b.AppendValues([]float64{1, 2, 3}, nil)
	col := b.NewArray()

	defer col.Release()
	defer b.Release()

	// Create a record batch with the column
	schema := arrow.NewSchema([]arrow.Field{{Name: "column1", Type: arrow.PrimitiveTypes.Float64}}, nil)
	return array.NewRecord(schema, []arrow.Array{col}, int64(col.Len()))
}

func main() {
	// Create a sample arrow record
	rec := _makeSampleArrowRecord()
	fmt.Println("Record before running SQL:")
	fmt.Println(rec)

	// Run SQL
	ctx := context.Background()
	runner, err := sqlrunner.New(ctx)
	if err != nil {
		panic(err)
	}
	defer runner.Close()
	resultRecords, err := runner.RunSQLOnRecord(rec, "SELECT column1+1 FROM {{.Table}}")
	if err != nil {
		panic(err)
	}

	// Print the result records
	for _, resultRecord := range resultRecords {
		fmt.Println(resultRecord)
	}
}
