-- Connect to DuckDB
.open :memory:

-- Install required extensions
INSTALL httpfs;
LOAD httpfs;
INSTALL aws;
LOAD aws;

CREATE OR REPLACE SECRET secret ( TYPE s3,  PROVIDER credential_chain );

SELECT * FROM read_csv_auto('s3://aws-glue-jpa-datasets/raw/titanic/set=test/test.csv');