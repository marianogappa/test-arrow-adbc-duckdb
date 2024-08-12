package sqlrunner

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"html/template"
	"io"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-adbc/go/adbc/drivermgr"
	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/ipc"
)

const tempTable = "source_table"

type DuckDBSQLRunner struct {
	ctx  context.Context
	conn adbc.Connection
	db   adbc.Database
}

func New(ctx context.Context) (*DuckDBSQLRunner, error) {
	var drv drivermgr.Driver
	db, err := drv.NewDatabase(map[string]string{
		"driver":     "duckdb",
		"entrypoint": "duckdb_adbc_init",
		"path":       ":memory:",
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create new in-memory DuckDB database: %w", err)
	}

	conn, err := db.Open(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to open connection to new in-memory DuckDB database: %w", err)
	}
	return &DuckDBSQLRunner{ctx: ctx, conn: conn, db: db}, nil
}

func serializeRecord(record arrow.Record) (io.Reader, error) {
	buf := new(bytes.Buffer)
	wr := ipc.NewWriter(buf, ipc.WithSchema(record.Schema()))

	if err := wr.Write(record); err != nil {
		return nil, fmt.Errorf("failed to write record: %w", err)
	}

	if err := wr.Close(); err != nil {
		return nil, fmt.Errorf("failed to close writer: %w", err)
	}

	return buf, nil
}

func (r *DuckDBSQLRunner) importRecord(sr io.Reader) error {
	rdr, err := ipc.NewReader(sr)
	if err != nil {
		return fmt.Errorf("failed to create IPC reader: %w", err)
	}
	defer rdr.Release()

	stmt, err := r.conn.NewStatement()
	if err != nil {
		return fmt.Errorf("failed to create new statement: %w", err)
	}

	if err := stmt.SetOption(adbc.OptionKeyIngestMode, adbc.OptionValueIngestModeCreate); err != nil {
		return fmt.Errorf("failed to set ingest mode: %w", err)
	}
	// duckdb hasn't implemented temp table ingest yet unfortunately, would be good to update this!
	// stmt.SetOption(adbc.OptionValueIngestTemporary, adbc.OptionValueEnabled)
	// optional!
	// stmt.SetOption(adbc.OptionValueIngestTargetCatalog, "catalog")
	if err := stmt.SetOption(adbc.OptionKeyIngestTargetTable, tempTable); err != nil {
		return fmt.Errorf("failed to set ingest target table: %w", err)
	}

	if err := stmt.BindStream(r.ctx, rdr); err != nil {
		return fmt.Errorf("failed to bind stream: %w", err)
	}

	if _, err := stmt.ExecuteUpdate(r.ctx); err != nil {
		return fmt.Errorf("failed to execute update: %w", err)
	}

	return stmt.Close()
}

func parseSQL(sql string) (string, error) {
	tmpl, err := template.New("sql").Parse(sql)
	if err != nil {
		return "", err
	}

	var sqlBuffer bytes.Buffer
	err = tmpl.Execute(&sqlBuffer, map[string]string{"Table": tempTable})
	if err != nil {
		return "", err
	}
	return sqlBuffer.String(), nil
}

func (r *DuckDBSQLRunner) runSQL(sql string, ignoreOutput bool) ([]arrow.Record, error) {
	stmt, err := r.conn.NewStatement()
	if err != nil {
		return nil, fmt.Errorf("failed to create new statement: %w", err)
	}
	defer stmt.Close()

	sql, err = parseSQL(sql)
	if err != nil {
		return nil, fmt.Errorf("failed to parse SQL: %w", err)
	}

	if err := stmt.SetSqlQuery(sql); err != nil {
		return nil, fmt.Errorf("failed to set SQL query: %w", err)
	}
	out, n, err := stmt.ExecuteQuery(r.ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to execute query: %w", err)
	}
	defer out.Release()
	if ignoreOutput {
		return nil, nil
	}

	result := make([]arrow.Record, 0, n)
	for out.Next() {
		rec := out.Record()
		rec.Retain() // .Next() will release the record, so we need to retain it
		result = append(result, rec)
	}
	if out.Err() != nil {
		return nil, out.Err()
	}
	// We can't defer out.Release() here because we need to return the result
	// defer out.Release()
	return result, nil
}

func (r *DuckDBSQLRunner) RunSQLOnRecord(record arrow.Record, sqls ...string) ([]arrow.Record, error) {
	if len(sqls) == 0 {
		return nil, errors.New("no SQL statement provided")
	}
	serializedRecord, err := serializeRecord(record)
	if err != nil {
		return nil, fmt.Errorf("failed to serialize record: %w", err)
	}
	if err := r.importRecord(serializedRecord); err != nil {
		return nil, fmt.Errorf("failed to import record: %w", err)
	}
	var result []arrow.Record
	for i, sql := range sqls {
		result, err = r.runSQL(sql, i != len(sqls)-1)
		if err != nil {
			return nil, fmt.Errorf("failed to run SQL: %w", err)
		}
	}

	if _, err := r.runSQL("DROP TABLE "+tempTable, true); err != nil {
		return nil, fmt.Errorf("failed to drop temp table after running query: %w", err)
	}
	return result, nil
}

func (r *DuckDBSQLRunner) Close() {
	r.conn.Close()
	r.db.Close()
}
