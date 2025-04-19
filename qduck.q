
init_duckdb:`qduck 2: (`init_duckdb; 1);
query:`qduck 2: (`run_query; 1);

init_duckdb `$":memory:";

-1 "Loaded sample/sample.parquet to t table";
t:query `$"SELECT * FROM 'sample/sample.parquet';"
