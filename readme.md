# qduck - DuckDB client API for KDB/Q

This client API integrates DuckDB into KDB/Q, allowing users to query open data formats (CSV, Parquet, and Iceberg) on S3 buckets directly and return results in native KDB table format. This simplifies existing ETL processing and provides immediate access to new datasets without needing to convert to on-disk KDB format first.

## Example

```
q)x)SELECT * FROM 'https://datahub.io/core/inflation/_r/-/data/inflation-gdp.csv' WHERE "Country Code" = 'USA' ORDER BY Year DESC LIMIT 10;
Country       Country Code Year Inflation
-----------------------------------------
United States USA          2023 4.116338 
United States USA          2022 8.0028   
United States USA          2021 4.697859 
United States USA          2020 1.233584 
United States USA          2019 1.81221  
United States USA          2018 2.442583 
United States USA          2017 2.13011  
United States USA          2016 1.261583 
United States USA          2015 0.1186271
United States USA          2014 1.622223 
/ to set a variable with query results
q)t:.x.e "SELECT * FROM 'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-01.parquet';"
q)count t
3475226
```

Additional examples are provided in `example.q`

## Build and Run

This client API requires the duckdb c/c++ library which is available here:

https://duckdb.org/docs/installation/?version=stable&environment=cplusplus&platform=linux&download_method=direct&architecture=x86_64

Unzip to your preferred location.

Option 1: Add libduckdb to your LD_LIBRARY_PATH.

```
export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/path/to/libduckdb
```

Option 2: Add duckdb library globally:

```
sudo cp ./libduckdb/libduckdb.so /usr/local/lib/
sudo ldconfig
```

Run `make all run`

*Note: KDB/Q requires shared libraries to be within the $QHOME/l64. As to not pollute your existing $QHOME directory, the contents of your $QHOME are copied into the build folder and run from the build directory. You may need to change this to suit your needs.*
