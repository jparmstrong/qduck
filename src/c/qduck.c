#include<duckdb.h>
#include<stdlib.h>
#include<string.h>
#include"k.h"

#define dkO(p,d)(duckdb_open(p,d)!=DuckDBSuccess)
#define dkC(d,c)(duckdb_connect(d,c)!=DuckDBSuccess)
#define dkQ(c,s,r)(duckdb_query(c,s,r)!=DuckDBSuccess)
#define dkD(r)duckdb_destroy_result(r)
#define dkCC(r)duckdb_column_count(r)
#define dkRC(r)duckdb_row_count(r)
#define dkCN(r,i)duckdb_column_name(r,i)
#define dkCT(r,i)duckdb_column_type(r,i)
#define dkCD(r,i)duckdb_column_data(r,i)
#define dkN(r,i,j)duckdb_value_is_null(r,i,j)
duckdb_database d;duckdb_connection c;

// Initialize DuckDB
K init(K p) {
    if (p->t != KC) return krr("type");
    char* s = malloc(p->n + 1);
    if (!s) return krr("memory");
    memcpy(s, p->G0, p->n);
    s[p->n] = '\0';
    if (dkO(s, &d)) { free(s); return krr("open"); }
    if (dkC(d, &c)) { duckdb_close(&d); free(s); return krr("connect"); }
    free(s);
    return (K)0;
}

// Execute query
K query(K q){
    if(q->t!=KC)return krr("type");
    char*s=malloc(q->n+1);
    if(!s)return krr("memory");
    memcpy(s,q->G0,q->n);
    s[q->n]='\0';
    duckdb_result r;
    if(dkQ(c,s,&r)){free(s);return krr("query");}
    free(s);
    int nc=dkCC(&r),nr=dkRC(&r);
    K cols=ktn(0,nc),names=ktn(KS,nc);
    for(int x=0;x<nc;x++){
        kS(names)[x]=ss((S)dkCN(&r,x));
        duckdb_type t=dkCT(&r,x);
        K col;
        switch(t){
            case DUCKDB_TYPE_BOOLEAN:
                col=ktn(KB,nr);
                DO(nr,kG(col)[i]=dkN(&r,x,i)?0x00:((bool*)dkCD(&r,x))[i]);
                break;
            case DUCKDB_TYPE_TINYINT:
                col=ktn(KH,nr);
                DO(nr,kH(col)[i]=dkN(&r,x,i)?nh:((int8_t*)dkCD(&r,x))[i]);
                break;
            case DUCKDB_TYPE_SMALLINT:
                col=ktn(KH,nr);
                DO(nr,kH(col)[i]=dkN(&r,x,i)?nh:((int16_t*)dkCD(&r,x))[i]);
                break;
            case DUCKDB_TYPE_INTEGER:
                col=ktn(KI,nr);
                DO(nr,kI(col)[i]=dkN(&r,x,i)?ni:((int32_t*)dkCD(&r,x))[i]);
                break;
            case DUCKDB_TYPE_BIGINT:
                col=ktn(KJ,nr);
                DO(nr,kJ(col)[i]=dkN(&r,x,i)?nj:((int64_t*)dkCD(&r,x))[i]);
                break;
            case DUCKDB_TYPE_FLOAT:
                col=ktn(KE,nr);
                DO(nr,kE(col)[i]=dkN(&r,x,i)?nf:((float*)dkCD(&r,x))[i]);
                break;
            case DUCKDB_TYPE_DOUBLE:
                col=ktn(KF,nr);
                DO(nr,kF(col)[i]=dkN(&r,x,i)?nf:((double*)dkCD(&r,x))[i]);
                break;
            case DUCKDB_TYPE_VARCHAR:
                col=ktn(KS,nr);
                DO(nr,kS(col)[i]=dkN(&r,x,i)?ss(""):ss((S)((char**)dkCD(&r,x))[i]));
                break;
            case DUCKDB_TYPE_DATE:
                col=ktn(KI,nr);
                DO(nr,kI(col)[i]=dkN(&r,x,i)?ni:((int32_t*)dkCD(&r,x))[i]-10957);
                col->t=KD;
                break;
            case DUCKDB_TYPE_TIMESTAMP:
                col=ktn(KJ,nr);
                DO(nr,kJ(col)[i]=dkN(&r,x,i)?nj:(((int64_t*)dkCD(&r,x))[i]-(int64_t)10957*86400*1000000)*1000);
                col->t=KP;
                break;
            case DUCKDB_TYPE_TIME:
                col=ktn(KI,nr);
                DO(nr,kI(col)[i]=dkN(&r,x,i)?ni:((int64_t*)dkCD(&r,x))[i]/1000);
                col->t=KT;
                break;
            case DUCKDB_TYPE_INTERVAL:
                col=ktn(0,nr);
                DO(nr,kK(col)[i]=dkN(&r,x,i)?knk(3,ki(ni),ki(ni),kj(nj)):knk(3,ki(((duckdb_interval*)dkCD(&r,x))[i].months),ki(((duckdb_interval*)dkCD(&r,x))[i].days),kj(((duckdb_interval*)dkCD(&r,x))[i].micros)));
                break;
            default:
                dkD(&r);
                return krr("unsupported");
        }
        kK(cols)[x]=col;
    }
    dkD(&r);
    return xT(xD(names,cols));
}

// Close DuckDB
K close(K _) {
    duckdb_disconnect(&c);
    duckdb_close(&d);
    return (K)0;
}

