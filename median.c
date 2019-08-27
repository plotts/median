#include <postgres.h>
#include <fmgr.h>
#include <catalog/pg_type.h>
#include <utils/timestamp.h>
#include <utils/lsyscache.h>

#ifdef PG_MODULE_MAGIC
PG_MODULE_MAGIC;
#endif

typedef struct Element {
    Datum   datum;
    int    sequence;
} ELEMENT;

typedef struct AllData
{
    Oid		type;
    int16	type_len;
    bool	type_by_val;
    char	type_align;

    int     total_nodes;
    int     count;
    int     sequence;
    int     sorted_by;
    ELEMENT   *data_items;
} ALL_DATA;

typedef struct AllData *ALL_DATA_P;
ALL_DATA_P all_data;

#define DEBUG 1

#define SORTED_BY_SEQUENCE 1
#define SORTED_BY_DATUM  2
#define UNSORTED 3

#define NUM_NODES 100
#define MIN(a,b) (((a)<(b))?(a):(b))

typedef int (*comparator)(const void *p, const void *q);
comparator select_comparator(void);

int compare_ints(const void *p, const void *q);
int compare_ints_sequence_descending(const void *p, const void *q);
int compare_doubles(const void *p, const void *q);
int compare_varchars(const void *p, const void *q);
int compare_timestamps(const void *p, const void *q);

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
    MemoryContext agg_context;

    if (!AggCheckCallContext(fcinfo, &agg_context))
    {
        elog(ERROR, "median_transfn called in non-aggregate context");
    }

    MemoryContextSwitchTo(agg_context);

    all_data = (ALL_DATA_P) (PG_ARGISNULL(0) ? NULL : PG_GETARG_POINTER(0));
    if (all_data == NULL)
    {
        // first time.
        if ((all_data = MemoryContextAllocZero(agg_context, sizeof(ALL_DATA))) == NULL)
        {
            elog(ERROR, "can\'t allocate all_data struct");
        }

        if ((all_data->data_items = (ELEMENT *) MemoryContextAllocZero(agg_context, NUM_NODES * sizeof(ELEMENT))) == NULL)
        {
            elog(ERROR, "can\'t allocate all_data->data struct");
        }

        all_data->total_nodes = NUM_NODES;
        all_data->count = 0;
        all_data->sequence = 0;

        // find info for pass by value/pass by reference...
        all_data->type = get_fn_expr_argtype(fcinfo->flinfo, 1);
        get_typlenbyvalalign(all_data->type, &all_data->type_len, &all_data->type_by_val, &all_data->type_align);

        if (!OidIsValid(all_data->type))
        {
            elog(ERROR, "could not determine data type of input");
        }
    }

    // check here for a null datum.
    if (PG_ARGISNULL(1))
    {
        PG_RETURN_POINTER(all_data);
    }

    all_data->data_items[all_data->count].datum = PG_GETARG_DATUM(1);
    all_data->data_items[all_data->count].sequence = all_data->sequence;
    all_data->count++;
    all_data->sequence++;
    all_data->sorted_by = UNSORTED;

    if (all_data->count == all_data->total_nodes)
    {
        all_data->total_nodes = all_data->count * 2;
        all_data->data_items = (ELEMENT *) repalloc_huge(all_data->data_items, all_data->total_nodes * sizeof(ELEMENT));
        if(all_data->data_items == NULL)
        {
            elog(ERROR, "repalloc_huge() failed for data_items.");
        }
    }

    PG_RETURN_POINTER(all_data);
}


PG_FUNCTION_INFO_V1(median_invfn);
/*
 * Median inverse transition function.
 *
 * This function is called only when calculating a moving aggregate.
 * When the oldest row goes out of the window, this function is called to remove
 * the value from the calculation.
 *
 * This is done by re-sorting the ELEMENTs by sequence number, in descending
 * order.  The oldest value is at the end of the array of ELEMENTS and
 * we decrement the count of items in the array - the last ELEMENT
 * is positioned to be overwritten by the next incoming value.
 *
 * Flag the array to indicate it has been sorted by the sequence number.
 *
 */
Datum
median_invfn(PG_FUNCTION_ARGS) {

    MemoryContext agg_context;

    if (!AggCheckCallContext(fcinfo, &agg_context))
    {
        elog(ERROR, "median_invfn called in non-aggregate context");
    }

    MemoryContextSwitchTo(agg_context);

    all_data = (ALL_DATA_P) (PG_ARGISNULL(0) ? NULL : PG_GETARG_POINTER(0));
    if (all_data == NULL)
    {
        elog(ERROR, "median_invfn: alldata is null");
    }

    if(all_data->sorted_by != SORTED_BY_SEQUENCE) {
        qsort(all_data->data_items, all_data->count, sizeof(ELEMENT), compare_ints_sequence_descending);
        all_data->sorted_by = SORTED_BY_SEQUENCE;
    }

    // decrement the count; preparing to overwrite the item that has the smallest sequence.
    all_data->count--;

    PG_RETURN_POINTER(all_data);
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
    Datum median;

    if (!AggCheckCallContext(fcinfo, &agg_context))
    {
        elog(ERROR, "median_finalfn called in non-aggregate context");
    }

    MemoryContextSwitchTo(agg_context);

    all_data = (ALL_DATA *) (PG_ARGISNULL(0) ? NULL : PG_GETARG_POINTER(0));
    if (all_data == NULL)
    {
        PG_RETURN_NULL();
    }

    // no data points?  no median.
    if (all_data->count == 0)
    {
        PG_RETURN_NULL();
    }


    if(all_data->sorted_by != SORTED_BY_DATUM)
    {
        qsort(all_data->data_items, all_data->count, sizeof(ELEMENT), select_comparator());
        all_data->sorted_by = SORTED_BY_DATUM;
    }


    if (all_data->count % 2 == 1)
    {
        // Odd number of data items.
        median = all_data->data_items[all_data->count / 2].datum;
    } else {
        // Even number of data items.
        // Return the lower of the pair.  Don't average, as this
        // will not be supported for some data types.
        median = all_data->data_items[all_data->count / 2 -1].datum;
    }

    PG_RETURN_DATUM(median);
}


comparator
select_comparator(void)
{
    switch(all_data->type)
    {
        case INT2OID:
        case INT4OID:
        case INT8OID:
            return compare_ints;

        case FLOAT4OID:
        case FLOAT8OID:
            return compare_doubles;

        case VARCHAROID:
        case TEXTOID:
            return compare_varchars;

        case TIMESTAMPOID:
        case TIMESTAMPTZOID:
            return compare_timestamps;

        default:
            elog(ERROR, "select_comparator:  unknown data type %d", all_data->type);
            break;
    }
}

int
compare_ints(const void *p, const void *q)
{
    long x = (*(const ELEMENT *) p).datum;
    long y = (*(const ELEMENT *) q).datum;
    if (x < y)
    {
        return -1;
    } else if (x > y) {
        return 1;
    }
    return 0;
}

int
compare_ints_sequence_descending(const void *p, const void *q)
{
    long x = (*(const ELEMENT *) p).sequence;
    long y = (*(const ELEMENT *) q).sequence;

    // Reversing this relational operator to get reverse sort.
    if (x > y)
    {
        return -1;
        // Reversing this relational operator, too.
    } else if (x < y) {
        return 1;
    }
    return 0;
}

int
compare_doubles(const void *p, const void *q)
{
    double x = DatumGetFloat8((*(const ELEMENT *)p).datum);
    double y = DatumGetFloat8((*(const ELEMENT *)q).datum);

    if (x < y) {
        return -1;
    } else if (x > y) {
        return 1;
    }
    return 0;
}

int
compare_varchars(const void *p, const void *q)
{
    Datum x  = PointerGetDatum(PG_DETOAST_DATUM_COPY((*(const ELEMENT *) p).datum));
    Datum y  = PointerGetDatum(PG_DETOAST_DATUM_COPY((*(const ELEMENT *) q).datum));
    int len_x = VARSIZE(x)-VARHDRSZ;
    int len_y = VARSIZE(y)-VARHDRSZ;
    int rval = strncmp(VARDATA(x), VARDATA(y), MIN(len_x, len_y));


    // post process this a bit.
    // bob == bobby if you only compare 3 characters.
    // handle different length strings which start
    // identically.
    if(rval == 0)
    {
        if(len_x < len_y)
        {
            return -1;
        } else if(len_x > len_y) {
            return 1;
        } else {
            return 0;
        }
    } else {
        return rval;
    }
}

int
compare_timestamps(const void *p, const void *q)
{
    Timestamp x  = DatumGetTimestamp((*(const ELEMENT *) p).datum);
    Timestamp y  = DatumGetTimestamp((*(const ELEMENT *) q).datum);

    if(x < y)
    {
        return -1;
    } else if(x > y) {
        return 1;
    } else {
        return 0;
    }

}
