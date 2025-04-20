

/ if you want to set a variable use .x.e
t1:.x.e "SELECT * FROM 'sample/sample.parquet';"

t2:.x.e "SELECT * FROM 'sample/test.csv';"

/ to run stanard duckdb sql, use x)
x)SELECT * FROM 'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-01.parquet' limit 10;

x)SELECT * FROM 'https://datahub.io/core/inflation/_r/-/data/inflation-gdp.csv' limit 10;
/
Country Country Code Year Inflation
-----------------------------------
Aruba   ABW          1985 4.032258 
Aruba   ABW          1986 1.073966 
Aruba   ABW          1987 3.643045 
Aruba   ABW          1988 3.121868 
Aruba   ABW          1989 3.991628 
Aruba   ABW          1990 5.836688 
Aruba   ABW          1991 5.555556 
Aruba   ABW          1992 3.873375 
Aruba   ABW          1993 5.21556  
Aruba   ABW          1994 6.31108  
\

-1 "Load from s3 bucket";
x)INSTALL httpfs; LOAD httpfs; INSTALL aws; LOAD aws;
x)CREATE OR REPLACE SECRET secret ( TYPE s3,  PROVIDER credential_chain );
x)SELECT * FROM read_csv_auto('s3://<bucket>/<prefix>.csv');
