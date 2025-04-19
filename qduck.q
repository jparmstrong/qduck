
init_duckdb:`qduck 2: (`init_duckdb; 1);
duck:`qduck 2: (`run_query; 1);

init_duckdb `$":memory:";

\

// Examples of loading different file formats with DuckDB

t1:duck `$"SELECT * FROM 'sample/sample.parquet';"

t2:duck `$"SELECT * FROM 'sample/test.csv';"

t3:duck `$"SELECT * FROM 'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-01.parquet' limit 10;"

-1 "Load from s3 bucket";
duck `$"INSTALL httpfs; LOAD httpfs; INSTALL aws; LOAD aws;"
duck `$"CREATE OR REPLACE SECRET secret ( TYPE s3,  PROVIDER credential_chain );"

t3:duck `$"SELECT * FROM read_csv_auto('s3://aws-glue-jpa-datasets/raw/titanic/set=test/test.csv');"
