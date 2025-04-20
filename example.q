/ if you want to set a variable use .x.e
t1:.x.e "SELECT * FROM 'sample/sample.parquet';"

t2:.x.e "SELECT * FROM 'sample/test.csv';"

/ to run standard duckdb sql, use x)
x)SELECT * FROM 'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-01.parquet' limit 10;
/
VendorID tpep_pickup_datetime          tpep_dropoff_datetime         passenger_count trip_distance RatecodeID store_and_fwd_flag PULocationID DOLocation..
--------------------------------------------------------------------------------------------------------------------------------------------------------..
1        2025.01.01D00:18:38.000000000 2025.01.01D00:26:59.000000000 1               1.6           1          N                  229          237       ..
1        2025.01.01D00:32:40.000000000 2025.01.01D00:35:13.000000000 1               0.5           1          N                  236          237       ..
1        2025.01.01D00:44:04.000000000 2025.01.01D00:46:01.000000000 1               0.6           1          N                  141          141       ..
2        2025.01.01D00:14:27.000000000 2025.01.01D00:20:01.000000000 3               0.52          1          N                  244          244       ..
2        2025.01.01D00:21:34.000000000 2025.01.01D00:25:06.000000000 3               0.66          1          N                  244          116       ..
2        2025.01.01D00:48:24.000000000 2025.01.01D01:08:26.000000000 2               2.63          1          N                  239          68        ..
1        2025.01.01D00:14:47.000000000 2025.01.01D00:16:15.000000000 0               0.4           1          N                  170          170       ..
1        2025.01.01D00:39:27.000000000 2025.01.01D00:51:51.000000000 0               1.6           1          N                  234          148       ..
1        2025.01.01D00:53:43.000000000 2025.01.01D01:13:23.000000000 0               2.8           1          N                  148          170       ..
2        2025.01.01D00:00:02.000000000 2025.01.01D00:09:36.000000000 1               1.71          1          N                  237          262       ..
\

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
