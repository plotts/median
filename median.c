#include <postgres.h>
#include <fmgr.h>
#include <stdio.h>
#include <string.h>

#ifdef PG_MODULE_MAGIC
PG_MODULE_MAGIC;
#endif

typedef struct AllData {
    int total_nodes;
    int count;
    Datum *data;
} ALLDATA;

typedef struct AllData *ALLDATA_P;

ALLDATA_P allData;

int compare_data(const void *p, const void *q);

#define NUM_NODES 10

PG_FUNCTION_INFO_V1(median_transfn);

/*
 * Median state transfer function.
 *
 *
 * This function is called for every value in the set that we are calculating
 * the median for. On first call, the aggregate state, if any, needs to be
 * initialized.
 */
Datum
median_transfn(PG_FUNCTION_ARGS) {
    Oid datum_type = get_fn_expr_argtype(fcinfo->flinfo, 0);
    MemoryContext agg_context;
    Datum datum;

    if (!OidIsValid(datum_type)) {
        elog(ERROR, "could not determine data type of input");
    }

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
    }

    if (PG_ARGISNULL(1)) {
        PG_RETURN_POINTER(agg_context);

    } else {
        datum = PG_GETARG_DATUM(1);
        allData->data[allData->count] = datum;
        allData->count++;
    }

    if (allData->count == allData->total_nodes) {
        allData->total_nodes = allData->count * 2;

        // TODO: delete debugging code.
        fprintf(stderr, "doubling to %d Datum\n", allData->total_nodes);

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

    allData = (ALLDATA *) (PG_ARGISNULL(0) ? NULL : PG_GETARG_POINTER(0));

    if (!AggCheckCallContext(fcinfo, &agg_context)) {
        elog(ERROR, "median_finalfn called in non-aggregate context");
    }
    MemoryContextSwitchTo(agg_context);

    if (allData == NULL) {
        PG_RETURN_DATUM((Datum)NULL);
    }

    if (allData->count == 0) {
        elog(ERROR, "allData count is 0 - fatal error.");
    }

    qsort(allData->data, allData->count - 1, sizeof(Datum), compare_data);

    if (allData->count % 2 == 1) {
        // Odd number of data items.
        median = allData->data[allData->count / 2];
    } else {
        // Even number of data items.
        // Return the lower of the pair.  Don't average, as this
        // will not be supported for some data types.
        median = allData->data[allData->count / 2 - 1];
    }

    PG_RETURN_DATUM(median);
}

int compare_data(const void *p, const void *q) {
    Datum x = *(const Datum *) p;
    Datum y = *(const Datum *) q;

    if (x < y) {
        return -1;
    } else if (x > y) {
        return 1;
    }
    return 0;
}