# qduck

Enables KDB/Q to run duckdb queries.

## Installation

Download the duckdb c/c++ library:
https://duckdb.org/docs/installation/?version=stable&environment=cplusplus&platform=linux&download_method=direct&architecture=x86_64

Unzip.

Option 1: Add libduckdb to your LD_LIBRARY_PATH.

```
export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/path/to/libduckdb
```

Option 2: Add duckdb library globally:

```
sudo cp ./libduckdb/libduckdb.so /usr/local/lib/
sudo ldconfig
```

Run make.

Copy/symlink the qduck.so file to into $QHOME/l64.

## Test

Create sample data with duckdb. (assumes you have duckdb cli installed)

```
$ cd test
$ duckdb < sample.sql
```

Run sample code.

```
q qduck.q
q)t
col_boolean col_tinyint col_smallint col_integer col_bigint          col_float col_double col_varchar col_date   col_timestamp                ..
----------------------------------------------------------------------------------------------------------------------------------------------..
1           127         0W           0W          0W                  3.14      2.718282   Apple       2025.04.19 2025.04.19D12:34:56.000000000..
0           -128                                                     -3.14     -2.718282  Banana      2000.01.01 2000.01.01D00:00:00.000000000..
1           42          12345        123456789   1234567890123456789 1.23      4.56       Cherry      2010.06.15 2010.06.15D08:30:00.000000000..
0           0           0            0           0                   0         0          Date        1999.12.31 1999.12.31D23:59:59.000000000..
```