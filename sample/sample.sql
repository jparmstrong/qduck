-- Connect to DuckDB
.open :memory:

-- Create a table with all DuckDB-supported types
CREATE TABLE sample (
    col_boolean BOOLEAN,
    col_tinyint TINYINT,
    col_smallint SMALLINT,
    col_integer INTEGER,
    col_bigint BIGINT,
    col_float FLOAT,
    col_double DOUBLE,
    col_varchar VARCHAR,
    col_date DATE,
    col_timestamp TIMESTAMP,
    col_time TIME,
    col_interval INTERVAL
);

-- Insert sample data into the table
INSERT INTO sample VALUES
    (TRUE, 127, 32767, 2147483647, 9223372036854775807, 3.14, 2.718281828459, 'Apple', DATE '2025-04-19', TIMESTAMP '2025-04-19 12:34:56' AT TIME ZONE 'UTC', TIME '12:34:56', INTERVAL '1 year 2 months 3 days'),
    (FALSE, -128, -32768, -2147483648, -9223372036854775808, -3.14, -2.718281828459, 'Banana', DATE '2000-01-01', TIMESTAMP '2000-01-01 00:00:00' AT TIME ZONE 'UTC', TIME '00:00:00', INTERVAL '2 years 5 days'),
    (TRUE, 42, 12345, 123456789, 1234567890123456789, 1.23, 4.56, 'Cherry', DATE '2010-06-15', TIMESTAMP '2010-06-15 08:30:00' AT TIME ZONE 'UTC', TIME '08:30:00', INTERVAL '6 months 10 days'),
    (FALSE, 0, 0, 0, 0, 0.0, 0.0, 'Date', DATE '1999-12-31', TIMESTAMP '1999-12-31 23:59:59' AT TIME ZONE 'UTC', TIME '23:59:59', INTERVAL '0 years 0 days');

-- Write the table to a Parquet file
COPY sample TO 'sample.parquet' (FORMAT PARQUET);

-- Verify the Parquet file was created
SELECT * FROM 'sample.parquet';