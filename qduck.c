#include <duckdb.h>
#include <stdio.h>
#include "k.h"

// Initialize the DuckDB database and context
duckdb_database db;
duckdb_connection con;

// Function to initialize the DuckDB instance
K init_duckdb(K path) {
    if (path->t != -11) return krr("type");
    const char* db_path = path->s;
    if (duckdb_open(db_path, &db) != DuckDBSuccess) {
        return krr("Could not open DuckDB");
    }
    if (duckdb_connect(db, &con) != DuckDBSuccess) {
        duckdb_close(&db);
        return krr("Could not connect to DuckDB");
    }
    return (K)0;
}

// Function to execute SQL queries and return results as KDB table
K run_query(K query) {
    if (query->t != -11) return krr("type");
    const char* sql = query->s;
    
    duckdb_result result;
    if (duckdb_query(con, sql, &result) != DuckDBSuccess) {
        return krr("Query failed");
    }
    
    // Convert DuckDB result to KDB+/q KTable
    int ncols = duckdb_column_count(&result);
    int nrows = duckdb_row_count(&result);
    
    K cols = ktn(0, ncols);  // List of columns
    K col_names = ktn(KS, ncols);  // List of column names
    
    for (int i = 0; i < ncols; i++) {
        const char* col_name = duckdb_column_name(&result, i);
        kS(col_names)[i] = ss((S)col_name); // Set column name
    
        // Create appropriate KDB column type
        duckdb_type col_type = duckdb_column_type(&result, i);
        K col;
    
        switch (col_type) {
            case DUCKDB_TYPE_BOOLEAN:
                col = ktn(KB, nrows);  // Boolean column
                for (int j = 0; j < nrows; j++) {
                    kG(col)[j] = ((bool*)duckdb_column_data(&result, i))[j];
                }
                break;
            case DUCKDB_TYPE_TINYINT:
                col = ktn(KH, nrows);  // Short column (KDB+ short)
                for (int j = 0; j < nrows; j++) {
                    kH(col)[j] = ((int8_t*)duckdb_column_data(&result, i))[j];
                }
                break;
            case DUCKDB_TYPE_SMALLINT:
                col = ktn(KH, nrows);  // Short column (KDB+ short)
                for (int j = 0; j < nrows; j++) {
                    kH(col)[j] = ((int16_t*)duckdb_column_data(&result, i))[j];
                }
                break;
            case DUCKDB_TYPE_INTEGER:
                col = ktn(KI, nrows);  // Integer column
                for (int j = 0; j < nrows; j++) {
                    kI(col)[j] = ((int32_t*)duckdb_column_data(&result, i))[j];
                }
                break;
            case DUCKDB_TYPE_BIGINT:
                col = ktn(KJ, nrows);  // Long column (KDB+ long)
                for (int j = 0; j < nrows; j++) {
                    kJ(col)[j] = ((int64_t*)duckdb_column_data(&result, i))[j];
                }
                break;
            case DUCKDB_TYPE_FLOAT:
                col = ktn(KE, nrows);  // Real column (KDB+ real)
                for (int j = 0; j < nrows; j++) {
                    kE(col)[j] = ((float*)duckdb_column_data(&result, i))[j];
                }
                break;
            case DUCKDB_TYPE_DOUBLE:
                col = ktn(KF, nrows);  // Float column (KDB+ float)
                for (int j = 0; j < nrows; j++) {
                    kF(col)[j] = ((double*)duckdb_column_data(&result, i))[j];
                }
                break;
            case DUCKDB_TYPE_VARCHAR:
                col = ktn(KS, nrows);  // String column
                for (int j = 0; j < nrows; j++) {
                    kS(col)[j] = ss((S)((char**)duckdb_column_data(&result, i))[j]);
                }
                break;
            case DUCKDB_TYPE_DATE:
                col = ktn(KI, nrows);  // Date column (KDB+ date)
                for (int j = 0; j < nrows; j++) {
                    int32_t duckdb_date = ((int32_t*)duckdb_column_data(&result, i))[j];
                    kI(col)[j] = duckdb_date - 10957;  // Adjust epoch from 1970 to 2000
                }
                col->t = KD;  // Set type tag to date
                break;
            case DUCKDB_TYPE_TIMESTAMP:
                col = ktn(KJ, nrows);  // Timestamp column (KDB+ datetime)
                for (int j = 0; j < nrows; j++) {
                    int64_t duckdb_timestamp = ((int64_t*)duckdb_column_data(&result, i))[j];
                    kJ(col)[j] = (duckdb_timestamp - (int64_t)10957 * 86400 * 1000000) * 1000;
                }
                col->t = KP;  // Set type tag to timestamp
                break;
            case DUCKDB_TYPE_TIME:
                col = ktn(KI, nrows);  // Time column (KDB+ time)
                for (int j = 0; j < nrows; j++) {
                    int64_t duckdb_time = ((int64_t*)duckdb_column_data(&result, i))[j];
                    kI(col)[j] = duckdb_time / 1000;  // Convert microseconds to milliseconds
                }
                col->t = KT;  // Set type tag to time
                break;
            case DUCKDB_TYPE_INTERVAL:
                col = ktn(0, nrows);  // Interval column (KDB+ list of intervals)
                for (int j = 0; j < nrows; j++) {
                    duckdb_interval* interval = &((duckdb_interval*)duckdb_column_data(&result, i))[j];
                    K interval_list = knk(3, ki(interval->months), ki(interval->days), kj(interval->micros));
                    kK(col)[j] = interval_list;
                }
                break;
            default:
                duckdb_destroy_result(&result);
                return krr("Unsupported column type");
        }
    
        // Add column to table
        kK(cols)[i] = col;
    }

    duckdb_destroy_result(&result);
    
    // Create a dictionary with column names and column data
    K dict = xD(col_names, cols);
    
    return xT(dict);  // Flip the dictionary into a table and return it
}

// Function to close DuckDB connection
K close_duckdb(K _) {
    duckdb_disconnect(&con);
    duckdb_close(&db);
    return (K)0;
}

