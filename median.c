#include <postgres.h>
#include <fmgr.h>
#include <catalog/pg_type.h>
#include <utils/timestamp.h>
#include <utils/lsyscache.h>

#ifdef PG_MODULE_MAGIC
PG_MODULE_MAGIC;
#endif

typedef struct AllData {
    Oid		type;
    int16	type_len;
    bool	type_by_val;
    char	type_align;

    int     total_nodes;
    int     count;
    Datum   *data;
} ALLDATA;

typedef struct AllData *ALLDATA_P;
ALLDATA_P allData;

int compare_original(const void *p, const void *q);
int compare_ints(const void *p, const void *q);
int compare_floats(const void *p, const void *q);
int compare_doubles(const void *p, const void *q);
int compare_varchars(const void *p, const void *q);
int compare_timestamps(const void *p, const void *q);

typedef int (*comparator)(const void *, const void *);
comparator select_comparator(void);

#define NUM_NODES 100

// TODO: need to replace these - must be something in PG we can use.
#define MIN(a,b) (((a)<(b))?(a):(b))
#define MAX(a,b) (((a)>(b))?(a):(b))

PG_FUNCTION_INFO_V1(median_transfn);

/*
 * Median state transfer function.
 *
 * This function is called for every value in the set that we are calculating
 * the median for. On first call, the aggregate state, if any, needs to be
 * initialized.
 */
Datum
median_transfn(PG_FUNCTION_ARGS) {
    MemoryContext agg_context;
    Datum datum;

    if (!AggCheckCallContext(fcinfo, &agg_context)) {
        elog(ERROR, "median_transfn called in non-aggregate context");
    }

    MemoryContextSwitchTo(agg_context);

    allData = (ALLDATA_P) (PG_ARGISNULL(0) ? NULL : PG_GETARG_POINTER(0));
    if (allData == NULL) {
        // first time.
        if ((allData = MemoryContextAllocZero(agg_context, sizeof(ALLDATA))) == NULL) {
            elog(ERROR, "can\'t allocate allData struct");
        }

        if ((allData->data = MemoryContextAllocZero(agg_context, NUM_NODES * sizeof(Datum))) == NULL) {
            elog(ERROR, "can\'t allocate allData->data struct");
        }
        allData->total_nodes = NUM_NODES;
        allData->count = 0;

        // handle pass by value/pass by reference...
        allData->type = get_fn_expr_argtype(fcinfo->flinfo, 1);
        get_typlenbyvalalign(allData->type, &allData->type_len, &allData->type_by_val, &allData->type_align);

        if (!OidIsValid(allData->type)) {
            elog(ERROR, "could not determine data type of input");
        }
    }

    // check here for a null datum.
    if (PG_ARGISNULL(1)) {
        PG_RETURN_POINTER(allData);
    }

    datum = PG_GETARG_DATUM(1);
    allData->data[allData->count] = datum;
    allData->count++;

    if (allData->count == allData->total_nodes) {
        allData->total_nodes = allData->count * 2;

        // TODO: does this need a null check?
        allData->data = (Datum *) repalloc_huge(allData->data, allData->total_nodes * sizeof(Datum));
        if(allData->data == NULL) {
            elog(ERROR, "repalloc_huge() failed.");
        }
    }

    PG_RETURN_POINTER(allData);
}

PG_FUNCTION_INFO_V1(median_finalfn);

/*
 * Median final function.
 *
 * This function is called after all values in the median set has been
 * processed by the state transfer function. It should perform any necessary
 * post processing and clean up any temporary state.
 */
Datum
median_finalfn(PG_FUNCTION_ARGS) {
    MemoryContext agg_context;
    Datum median = 0;

    if (!AggCheckCallContext(fcinfo, &agg_context)) {
        elog(ERROR, "median_finalfn called in non-aggregate context");
    }

    MemoryContextSwitchTo(agg_context);

    allData = (ALLDATA *) (PG_ARGISNULL(0) ? NULL : PG_GETARG_POINTER(0));
    if (allData == NULL) {
        PG_RETURN_DATUM((Datum)NULL);
    }

    // no data points.
    if (allData->count == 0) {
        PG_RETURN_DATUM((Datum)NULL);
    }

    qsort(allData->data, allData->count, sizeof(Datum), select_comparator());

    if (allData->count % 2 == 1) {
        // Odd number of data items.
        median = allData->data[allData->count / 2];
    } else {
        // Even number of data items.
        // Return the lower of the pair.  Don't average, as this
        // will not be supported for some data types.
        median = allData->data[allData->count / 2 -1];
    }

    PG_RETURN_DATUM(median);
}


comparator
select_comparator(void)
{
    switch(allData->type) {
        case INT2OID:
        case INT4OID:
        case INT8OID:
            return compare_ints;

        case FLOAT4OID:
            return compare_floats;

        case FLOAT8OID:
            return compare_doubles;

        case VARCHAROID:
            return compare_varchars;

        case TIMESTAMPOID:
        case TIMESTAMPTZOID:
            return compare_timestamps;

        default:
            elog(LOG, "select_comparator unknown data type %d", allData->type);
            return compare_ints;
    }
}

//TODO verify sign logic.
int
compare_ints(const void *p, const void *q) {
    long x = *(const long  *) p;
    long y = *(const long *) q;

    if (x < y) {
        return -1;
    } else if (x > y) {
        return 1;
    }
    return 0;
}

int
compare_floats(const void *p, const void *q) {
    float x = DatumGetFloat4(*(const Datum*)p);
    float y = DatumGetFloat4(*(const Datum*)q);

    if (x < y) {
        return -1;
    } else if (x > y) {
        return 1;
    }
    return 0;
}

int
compare_doubles(const void *p, const void *q) {
    double x = DatumGetFloat8(*(const Datum*)p);
    double y = DatumGetFloat8(*(const Datum*)q);

    if (x < y) {
        return -1;
    } else if (x > y) {
        return 1;
    }
    return 0;
}

int
compare_varchars(const void *p, const void *q) {
    Datum x  = PointerGetDatum(PG_DETOAST_DATUM_COPY(*(const Datum *) p));
    Datum y  = PointerGetDatum(PG_DETOAST_DATUM_COPY(*(const Datum *) q));
    int len_x = VARSIZE(x)-VARHDRSZ;
    int len_y = VARSIZE(y)-VARHDRSZ;
    int rval = strncmp(VARDATA(x), VARDATA(y), MIN(len_x, len_y));

    if(rval == 0) {  // matched
        if(len_x < len_y) {
            return -1;
        } else if(len_x > len_y) {
            return 1;
        } else {
            return 0;
        }
    } else {
        return 0;
    }
}



int
compare_timestamps(const void *p, const void *q) {
    Timestamp x  = DatumGetTimestamp(*(const Datum *) p);
    Timestamp y  = DatumGetTimestamp(*(const Datum *) q);

    //TODO: Delete me:
    fprintf(stderr, "timestamps: x %lu  y %lu\n", x, y);

    if(x < y) {
        return -1;
    } else if(x > y) {
        return 1;
    }else {
        return 0;
    }

}
