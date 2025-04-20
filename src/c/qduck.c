#include <duckdb.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "k.h"

// Initialize the DuckDB database and context
duckdb_database db;
duckdb_connection con;

// Function to initialize the DuckDB instance
K init(K path) {
    if (path->t != KC) return krr("type");

    char* db_path = (char*)malloc(path->n + 1);
    if (!db_path) return krr("memory");
    memcpy(db_path, path->G0, path->n);
    db_path[path->n] = '\0';

    if (duckdb_open(db_path, &db) != DuckDBSuccess) {
        free(db_path);
        return krr("Could not open DuckDB");
    }
    if (duckdb_connect(db, &con) != DuckDBSuccess) {
        duckdb_close(&db);
        free(db_path);
        return krr("Could not connect to DuckDB");
    }
    free(db_path);
    return (K)0;
}

// Function to execute SQL queries and return results as KDB table
K query(K query) {
    if (query->t != KC) return krr("type");

    char* sql = (char*)malloc(query->n + 1);
    if (!sql) return krr("memory");
    memcpy(sql, query->G0, query->n);
    sql[query->n] = '\0';
    
    duckdb_result result;
    if (duckdb_query(con, sql, &result) != DuckDBSuccess) {
        free(sql);
        return krr("Query failed");
    }
    free(sql);

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
                    kG(col)[j] = duckdb_value_is_null(&result, i, j) ? 0x00 : ((bool*)duckdb_column_data(&result, i))[j];
                }
                break;
            case DUCKDB_TYPE_TINYINT:
                col = ktn(KH, nrows);  // Short column (KDB+ short)
                for (int j = 0; j < nrows; j++) {
                    kH(col)[j] = duckdb_value_is_null(&result, i, j) ? nh : ((int8_t*)duckdb_column_data(&result, i))[j];
                }
                break;
            case DUCKDB_TYPE_SMALLINT:
                col = ktn(KH, nrows);  // Short column (KDB+ short)
                for (int j = 0; j < nrows; j++) {
                    kH(col)[j] = duckdb_value_is_null(&result, i, j) ? nh : ((int16_t*)duckdb_column_data(&result, i))[j];
                }
                break;
            case DUCKDB_TYPE_INTEGER:
                col = ktn(KI, nrows);  // Integer column
                for (int j = 0; j < nrows; j++) {
                    kI(col)[j] = duckdb_value_is_null(&result, i, j) ? ni : ((int32_t*)duckdb_column_data(&result, i))[j];
                }
                break;
            case DUCKDB_TYPE_BIGINT:
                col = ktn(KJ, nrows);  // Long column (KDB+ long)
                for (int j = 0; j < nrows; j++) {
                    kJ(col)[j] = duckdb_value_is_null(&result, i, j) ? nj : ((int64_t*)duckdb_column_data(&result, i))[j];
                }
                break;
            case DUCKDB_TYPE_FLOAT:
                col = ktn(KE, nrows);  // Real column (KDB+ real)
                for (int j = 0; j < nrows; j++) {
                    kE(col)[j] = duckdb_value_is_null(&result, i, j) ? nf : ((float*)duckdb_column_data(&result, i))[j];
                }
                break;
            case DUCKDB_TYPE_DOUBLE:
                col = ktn(KF, nrows);  // Float column (KDB+ float)
                for (int j = 0; j < nrows; j++) {
                    kF(col)[j] = duckdb_value_is_null(&result, i, j) ? nf : ((double*)duckdb_column_data(&result, i))[j];
                }
                break;
            case DUCKDB_TYPE_VARCHAR:
                col = ktn(KS, nrows);  // String column
                for (int j = 0; j < nrows; j++) {
                    kS(col)[j] = duckdb_value_is_null(&result, i, j) ? ss("") : ss((S)((char**)duckdb_column_data(&result, i))[j]);
                }
                break;
            case DUCKDB_TYPE_DATE:
                col = ktn(KI, nrows);  // Date column (KDB+ date)
                for (int j = 0; j < nrows; j++) {
                    kI(col)[j] = duckdb_value_is_null(&result, i, j) ? ni : ((int32_t*)duckdb_column_data(&result, i))[j] - 10957;
                }
                col->t = KD;  // Set type tag to date
                break;
            case DUCKDB_TYPE_TIMESTAMP:
                col = ktn(KJ, nrows);  // Timestamp column (KDB+ datetime)
                for (int j = 0; j < nrows; j++) {
                    kJ(col)[j] = duckdb_value_is_null(&result, i, j) ? nj : (((int64_t*)duckdb_column_data(&result, i))[j] - (int64_t)10957 * 86400 * 1000000) * 1000;
                }
                col->t = KP;  // Set type tag to timestamp
                break;
            case DUCKDB_TYPE_TIME:
                col = ktn(KI, nrows);  // Time column (KDB+ time)
                for (int j = 0; j < nrows; j++) {
                    kI(col)[j] = duckdb_value_is_null(&result, i, j) ? ni : ((int64_t*)duckdb_column_data(&result, i))[j] / 1000;
                }
                col->t = KT;  // Set type tag to time
                break;
            case DUCKDB_TYPE_INTERVAL:
                col = ktn(0, nrows);  // Create a general list for intervals
                for (int j = 0; j < nrows; j++) {
                    if (duckdb_value_is_null(&result, i, j)) {
                        kK(col)[j] = knk(3, ki(ni), ki(ni), kj(nj));  // Null interval
                    } else {
                        duckdb_interval* interval = &((duckdb_interval*)duckdb_column_data(&result, i))[j];
                        kK(col)[j] = knk(3, ki(interval->months), ki(interval->days), kj(interval->micros));
                    }
                }
                break;
            default:
                duckdb_destroy_result(&result);
                printf("Unsupported column type: %d\n", col_type);
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
K close(K _) {
    duckdb_disconnect(&con);
    duckdb_close(&db);
    return (K)0;
}

