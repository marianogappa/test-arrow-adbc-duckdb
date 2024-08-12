# test-arrow-adbc-duckdb

```shell
Record before running SQL:
----------------------------------
The record I'm about to print has:
NumRows: 3
NumCols: 1
Schema: schema:
  fields: 1
    - column1: type=float64

column1
1.000000
2.000000
3.000000

I'm done printing the record
----------------------------------
----------------------------------
The record I'm about to print has:
NumRows: 3
NumCols: 1
Schema: schema:
  fields: 1
    - (column1 + 1): type=float64, nullable

(column1 + 1)
2.000000
3.000000
4.000000

I'm done printing the record
----------------------------------
```
