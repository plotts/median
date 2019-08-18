#include <postgres.h>
#include <utils/lsyscache.h>
#include <fmgr.h>
#include <stdio.h>
#include <string.h>

#ifdef PG_MODULE_MAGIC
PG_MODULE_MAGIC;
#endif

typedef struct MyNode {
    Datum datum;
} MY_NODE;
typedef struct MyNode * MY_NODE_P;

typedef struct AllData {
    int numNodes;
    int count;
    struct MyNode *nodes;
} ALLDATA;

typedef struct AllData * ALLDATA_P;

ALLDATA_P allData;

int compare_datum(const void *p, const void *q);
#define NUM_NODES 10000

PG_FUNCTION_INFO_V1(median_transfn);

/*
 * Median state transfer function.
 *
 * This function is called for every value in the set that we are calculating
 * the median for. On first call, the aggregate state, if any, needs to be
 * initialized.
 */
Datum
median_transfn(PG_FUNCTION_ARGS)
{
    Oid datum_type = get_fn_expr_argtype(fcinfo->flinfo, 0);
    MemoryContext agg_context;
    Datum datum;

    if (!OidIsValid(datum_type)) {
        elog(ERROR, "could not determine data type of input");
    }

    if (!AggCheckCallContext(fcinfo, &agg_context)) {
        elog(ERROR, "median_transfn called in non-aggregate context");
    }

    allData = (ALLDATA_P)(PG_ARGISNULL(0) ? NULL : PG_GETARG_POINTER(0));
    if (allData == NULL)
    {

        if((allData = MemoryContextAllocZero(agg_context, sizeof(ALLDATA))) == NULL) {
            elog(ERROR, "can\'t allocate allData struct");
        }

        if((allData->nodes = MemoryContextAllocZero(agg_context, NUM_NODES * sizeof(MY_NODE))) == NULL) {
            elog(ERROR, "can\'t allocate allData->nodes struct");
        }

        allData->numNodes = NUM_NODES;
        allData->count = 0;
    }

    // TODO:  should NULLs be included as 0s?  or ignored?
    if (PG_ARGISNULL(1))
    {
        elog(ERROR, "null Datum");
    } else {
        datum = PG_GETARG_INT32(1);
        allData->nodes[allData->count].datum = datum;
        allData->count++;
    }

    if(allData->count == allData->numNodes)
    {
        allData->numNodes = allData->count * 2;
        fprintf(stderr, "doubled to size %ld\n", allData->numNodes *sizeof(MY_NODE));
        // TODO: does this need a null check?
        allData->nodes = (MY_NODE_P)repalloc_huge(allData->nodes, allData->numNodes * sizeof(MY_NODE));
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
median_finalfn(PG_FUNCTION_ARGS)
{
    MemoryContext agg_context;
    int32 myMean = 0;

    allData = (ALLDATA *) (PG_ARGISNULL(0) ? NULL : PG_GETARG_POINTER(0));

    if (!AggCheckCallContext(fcinfo, &agg_context))
    {
        elog(ERROR, "median_finalfn called in non-aggregate context");
    }

    if(allData == NULL)
    {
        elog(ERROR, "allData is NULL - fatal error.");
    }

    if(allData->count == 0)
    {
        elog(ERROR, "allData count is 0 - fatal error.");
    }

    qsort(allData->nodes, allData->count-1, sizeof(MY_NODE), compare_datum);

    if(allData->count % 2 == 1)
    {
        // odd number of data items.
        myMean = allData->nodes[allData->count/2].datum;
    } else {
        // even number of data items.
        myMean = (allData->nodes[allData->count/2-1].datum + allData->nodes[allData->count/2].datum) / 2;
    }

    PG_RETURN_INT32(myMean);
}

int compare_datum(const void *p, const void *q) {
    long unsigned int x = ((MY_NODE_P)p)->datum;
    long unsigned int y = ((MY_NODE_P)q)->datum;

    if (x < y)
    {
        return -1;
    } else if (x > y) {
        return 1;
    }
    return 0;
}