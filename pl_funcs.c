#include "pathman.h"
#include "utils/typcache.h"
#include "utils/array.h"


/* declarations */
PG_FUNCTION_INFO_V1( on_partitions_created );
PG_FUNCTION_INFO_V1( on_partitions_updated );
PG_FUNCTION_INFO_V1( on_partitions_removed );
PG_FUNCTION_INFO_V1( find_range_partition );
PG_FUNCTION_INFO_V1( get_range_by_idx );
PG_FUNCTION_INFO_V1( get_partition_range );

/*
 * Callbacks
 */
Datum
on_partitions_created(PG_FUNCTION_ARGS)
{
	LWLockAcquire(load_config_lock, LW_EXCLUSIVE);

	/* Reload config */
	/* TODO: reload just the specified relation */
	load_relations_hashtable(false);

	LWLockRelease(load_config_lock);

	PG_RETURN_NULL();
}

Datum
on_partitions_updated(PG_FUNCTION_ARGS)
{
	Oid					relid;
	PartRelationInfo   *prel;

	/* Parent relation oid */
	relid = DatumGetInt32(PG_GETARG_DATUM(0));
	prel = (PartRelationInfo *)
		hash_search(relations, (const void *) &relid, HASH_FIND, 0);
	if (prel != NULL)
	{
		LWLockAcquire(load_config_lock, LW_EXCLUSIVE);
		remove_relation_info(relid);
		load_relations_hashtable(false);
		LWLockRelease(load_config_lock);
	}

	PG_RETURN_NULL();
}

Datum
on_partitions_removed(PG_FUNCTION_ARGS)
{
	Oid		relid;

	LWLockAcquire(load_config_lock, LW_EXCLUSIVE);

	/* parent relation oid */
	relid = DatumGetInt32(PG_GETARG_DATUM(0));
	remove_relation_info(relid);

	LWLockRelease(load_config_lock);

	PG_RETURN_NULL();
}

/*
 * Returns partition oid for specified parent relid and value
 */
Datum
find_range_partition(PG_FUNCTION_ARGS)
{
	int		relid = DatumGetInt32(PG_GETARG_DATUM(0));
	Datum	value = PG_GETARG_DATUM(1);
	Oid		value_type = get_fn_expr_argtype(fcinfo->flinfo, 1);
	int		pos;
	bool	found;
	RangeRelation	*rangerel;
	RangeEntry		*ranges;
	TypeCacheEntry	*tce;
	FmgrInfo		*cmp_func;

	tce = lookup_type_cache(value_type,
		TYPECACHE_EQ_OPR | TYPECACHE_LT_OPR | TYPECACHE_GT_OPR |
		TYPECACHE_CMP_PROC | TYPECACHE_CMP_PROC_FINFO);
	cmp_func = &tce->cmp_proc_finfo;

	rangerel = (RangeRelation *)
		hash_search(range_restrictions, (const void *) &relid, HASH_FIND, NULL);

	if (!rangerel)
		PG_RETURN_NULL();

	ranges = dsm_array_get_pointer(&rangerel->ranges);
	pos = range_binary_search(rangerel, cmp_func, value, &found);

	if (found)
		PG_RETURN_OID(ranges[pos].child_oid);

	PG_RETURN_NULL();
}

/*
 * Returns range (min, max) as output parameters
 *
 * first argument is the parent relid
 * second is the partition relid
 * third and forth are MIN and MAX output parameters
 */
Datum
get_partition_range(PG_FUNCTION_ARGS)
{
	int		parent_oid = DatumGetInt32(PG_GETARG_DATUM(0));
	int		child_oid = DatumGetInt32(PG_GETARG_DATUM(1));
	int		nelems = 2;
	int 	i;
	bool	found = false;
	Datum			   *elems;
	PartRelationInfo   *prel;
	RangeRelation	   *rangerel;
	RangeEntry		   *ranges;
	TypeCacheEntry	   *tce;
	ArrayType		   *arr;

	prel = (PartRelationInfo *)
		hash_search(relations, (const void *) &parent_oid, HASH_FIND, NULL);
	
	rangerel = (RangeRelation *)
		hash_search(range_restrictions, (const void *) &parent_oid, HASH_FIND, NULL);

	if (!prel || !rangerel)
		PG_RETURN_NULL();

	ranges = dsm_array_get_pointer(&rangerel->ranges);
	tce = lookup_type_cache(prel->atttype, 0);

	/* Looking for specified partition */
	for(i=0; i<rangerel->ranges.length; i++)
		if (ranges[i].child_oid == child_oid)
		{
			found = true;
			break;
		}

	if (found)
	{
		elems = palloc(nelems * sizeof(Datum));
		elems[0] = ranges[i].min;
		elems[1] = ranges[i].max;

		arr = construct_array(elems, nelems, prel->atttype,
							  tce->typlen, tce->typbyval, tce->typalign);
		PG_RETURN_ARRAYTYPE_P(arr);
	}

	PG_RETURN_NULL();
}


/*
 * Returns N-th range (in form of array)
 *
 * First argument is the parent relid.
 * Second argument is the index of the range (if it is negative then the last
 * range will be returned).
 */
Datum
get_range_by_idx(PG_FUNCTION_ARGS)
{
	int parent_oid = DatumGetInt32(PG_GETARG_DATUM(0));
	int idx = DatumGetInt32(PG_GETARG_DATUM(1));
	PartRelationInfo *prel;
	RangeRelation	*rangerel;
	RangeEntry		*ranges;
	RangeEntry		*re;
	Datum			*elems;
	TypeCacheEntry	*tce;

	prel = (PartRelationInfo *)
		hash_search(relations, (const void *) &parent_oid, HASH_FIND, NULL);

	rangerel = (RangeRelation *)
		hash_search(range_restrictions, (const void *) &parent_oid, HASH_FIND, NULL);

	if (!prel || !rangerel || idx >= (int)rangerel->ranges.length)
		PG_RETURN_NULL();

	tce = lookup_type_cache(prel->atttype, 0);
	ranges = dsm_array_get_pointer(&rangerel->ranges);
	if (idx >= 0)
		re = &ranges[idx];
	else
		re = &ranges[rangerel->ranges.length - 1];

	elems = palloc(2 * sizeof(Datum));
	elems[0] = re->min;
	elems[1] = re->max;

	PG_RETURN_ARRAYTYPE_P(
		construct_array(elems, 2, prel->atttype,
						tce->typlen, tce->typbyval, tce->typalign));
}
