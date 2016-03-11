/* ------------------------------------------------------------------------
 *
 * pl_funcs.c
 *		Utility C functions for stored procedures
 *
 * Copyright (c) 2015-2016, Postgres Professional
 *
 * ------------------------------------------------------------------------
 */
#include "pathman.h"
#include "utils/lsyscache.h"
#include "utils/typcache.h"
#include "utils/array.h"
#include "utils/snapmgr.h"
#include "access/nbtree.h"
#include "access/xact.h"
#include "catalog/pg_type.h"
#include "executor/spi.h"
#include "storage/lmgr.h"


/* declarations */
PG_FUNCTION_INFO_V1( on_partitions_created );
PG_FUNCTION_INFO_V1( on_partitions_updated );
PG_FUNCTION_INFO_V1( on_partitions_removed );
PG_FUNCTION_INFO_V1( find_or_create_range_partition);
PG_FUNCTION_INFO_V1( get_range_by_idx );
PG_FUNCTION_INFO_V1( get_partition_range );
PG_FUNCTION_INFO_V1( acquire_partitions_lock );
PG_FUNCTION_INFO_V1( release_partitions_lock );
PG_FUNCTION_INFO_V1( check_overlap );
PG_FUNCTION_INFO_V1( get_min_range_value );
PG_FUNCTION_INFO_V1( get_max_range_value );

/*
 * Callbacks
 */
Datum
on_partitions_created(PG_FUNCTION_ARGS)
{
	LWLockAcquire(pmstate->load_config_lock, LW_EXCLUSIVE);

	/* Reload config */
	/* TODO: reload just the specified relation */
	load_relations_hashtable(false);

	LWLockRelease(pmstate->load_config_lock);

	PG_RETURN_NULL();
}

Datum
on_partitions_updated(PG_FUNCTION_ARGS)
{
	Oid					relid;
	PartRelationInfo   *prel;

	/* Parent relation oid */
	relid = DatumGetInt32(PG_GETARG_DATUM(0));
	prel = get_pathman_relation_info(relid, NULL);
	if (prel != NULL)
	{
		LWLockAcquire(pmstate->load_config_lock, LW_EXCLUSIVE);
		remove_relation_info(relid);
		load_relations_hashtable(false);
		LWLockRelease(pmstate->load_config_lock);
	}

	PG_RETURN_NULL();
}

Datum
on_partitions_removed(PG_FUNCTION_ARGS)
{
	Oid		relid;

	LWLockAcquire(pmstate->load_config_lock, LW_EXCLUSIVE);

	/* parent relation oid */
	relid = DatumGetInt32(PG_GETARG_DATUM(0));
	remove_relation_info(relid);

	LWLockRelease(pmstate->load_config_lock);

	PG_RETURN_NULL();
}

/*
 * Returns partition oid for specified parent relid and value.
 * In case when partition isn't exist try to create one.
 */
Datum
find_or_create_range_partition(PG_FUNCTION_ARGS)
{
	int		relid = DatumGetInt32(PG_GETARG_DATUM(0));
	Datum	value = PG_GETARG_DATUM(1);
	Oid		value_type = get_fn_expr_argtype(fcinfo->flinfo, 1);
	int		pos;
	bool	found;
	RangeRelation	*rangerel;
	RangeEntry		*ranges;
	TypeCacheEntry	*tce;
	PartRelationInfo *prel;
	Oid				 cmp_proc_oid;
	FmgrInfo		 cmp_func;

	tce = lookup_type_cache(value_type,
		TYPECACHE_EQ_OPR | TYPECACHE_LT_OPR | TYPECACHE_GT_OPR |
		TYPECACHE_CMP_PROC | TYPECACHE_CMP_PROC_FINFO);

	prel = get_pathman_relation_info(relid, NULL);
	rangerel = get_pathman_range_relation(relid, NULL);

	if (!prel || !rangerel)
		PG_RETURN_NULL();

	cmp_proc_oid = get_opfamily_proc(tce->btree_opf,
									 value_type,
									 prel->atttype,
									 BTORDER_PROC);
	fmgr_info(cmp_proc_oid, &cmp_func);

	ranges = dsm_array_get_pointer(&rangerel->ranges);
	pos = range_binary_search(rangerel, &cmp_func, value, &found);

	/*
	 * If found then just return oid. Else create new partitions
	 */
	if (found)
		PG_RETURN_OID(ranges[pos].child_oid);
	/*
	 * If not found and value is between first and last partitions
	*/
	if (!found && pos >= 0)
		PG_RETURN_NULL();
	else
	{
		Oid child_oid;

		/* Lock config before appending new partitions */
		LWLockAcquire(pmstate->load_config_lock, LW_EXCLUSIVE);

		/* Restrict concurrent partition creation */
		LWLockAcquire(pmstate->edit_partitions_lock, LW_EXCLUSIVE);

		/*
		 * Check if someone else has already created partition.
		 */
		ranges = dsm_array_get_pointer(&rangerel->ranges);
		pos = range_binary_search(rangerel, &cmp_func, value, &found);
		if (found)
		{
			LWLockRelease(pmstate->edit_partitions_lock);
			LWLockRelease(pmstate->load_config_lock);
			PG_RETURN_OID(ranges[pos].child_oid);
		}

		/* Start background worker to create new partitions */
		child_oid = create_partitions_bg_worker(relid, value, value_type);

		// SPI_connect();
		// child_oid = create_partitions(relid, value, value_type);
		// SPI_finish();
		// elog(WARNING, "Worker finished");

		/* Release locks */
		LWLockRelease(pmstate->edit_partitions_lock);
		LWLockRelease(pmstate->load_config_lock);

		/* Repeat binary search */
		ranges = dsm_array_get_pointer(&rangerel->ranges);
		pos = range_binary_search(rangerel, &cmp_func, value, &found);
		if (found)
			PG_RETURN_OID(child_oid);
	}

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

	prel = get_pathman_relation_info(parent_oid, NULL);
	
	rangerel = get_pathman_range_relation(parent_oid, NULL);

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
		bool byVal = rangerel->by_val;

		elems = palloc(nelems * sizeof(Datum));
		elems[0] = PATHMAN_GET_DATUM(ranges[i].min, byVal);
		elems[1] = PATHMAN_GET_DATUM(ranges[i].max, byVal);

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

	prel = get_pathman_relation_info(parent_oid, NULL);

	rangerel = get_pathman_range_relation(parent_oid, NULL);

	if (!prel || !rangerel || idx >= (int)rangerel->ranges.length)
		PG_RETURN_NULL();

	tce = lookup_type_cache(prel->atttype, 0);
	ranges = dsm_array_get_pointer(&rangerel->ranges);
	if (idx >= 0)
		re = &ranges[idx];
	else
		re = &ranges[rangerel->ranges.length - 1];

	elems = palloc(2 * sizeof(Datum));
	elems[0] = PATHMAN_GET_DATUM(re->min, rangerel->by_val);
	elems[1] = PATHMAN_GET_DATUM(re->max, rangerel->by_val);

	PG_RETURN_ARRAYTYPE_P(
		construct_array(elems, 2, prel->atttype,
						tce->typlen, tce->typbyval, tce->typalign));
}

/*
 * Returns min value of the first range for relation
 */
Datum
get_min_range_value(PG_FUNCTION_ARGS)
{
	int parent_oid = DatumGetInt32(PG_GETARG_DATUM(0));
	PartRelationInfo *prel;
	RangeRelation	*rangerel;
	RangeEntry		*ranges;

	prel = get_pathman_relation_info(parent_oid, NULL);
	rangerel = get_pathman_range_relation(parent_oid, NULL);

	if (!prel || !rangerel || prel->parttype != PT_RANGE || rangerel->ranges.length == 0)
		PG_RETURN_NULL();

	ranges = dsm_array_get_pointer(&rangerel->ranges);
	PG_RETURN_DATUM(PATHMAN_GET_DATUM(ranges[0].min, rangerel->by_val));
}

/*
 * Returns max value of the last range for relation
 */
Datum
get_max_range_value(PG_FUNCTION_ARGS)
{
	int parent_oid = DatumGetInt32(PG_GETARG_DATUM(0));
	PartRelationInfo *prel;
	RangeRelation	 *rangerel;
	RangeEntry		 *ranges;

	prel = get_pathman_relation_info(parent_oid, NULL);
	rangerel = get_pathman_range_relation(parent_oid, NULL);

	if (!prel || !rangerel || prel->parttype != PT_RANGE || rangerel->ranges.length == 0)
		PG_RETURN_NULL();

	ranges = dsm_array_get_pointer(&rangerel->ranges);
	PG_RETURN_DATUM(PATHMAN_GET_DATUM(ranges[rangerel->ranges.length-1].max, rangerel->by_val));
}

/*
 * Checks if range overlaps with existing partitions.
 * Returns TRUE if overlaps and FALSE otherwise.
 */
Datum
check_overlap(PG_FUNCTION_ARGS)
{
	int parent_oid = DatumGetInt32(PG_GETARG_DATUM(0));
	Datum p1 = PG_GETARG_DATUM(1);
	Oid	  p1_type = get_fn_expr_argtype(fcinfo->flinfo, 1);
	Datum p2 = PG_GETARG_DATUM(2);
	Oid	  p2_type = get_fn_expr_argtype(fcinfo->flinfo, 2);
	PartRelationInfo *prel;
	RangeRelation	 *rangerel;
	RangeEntry		 *ranges;
	FmgrInfo		  cmp_func_1;
	FmgrInfo		  cmp_func_2;
	int i;
	bool byVal;

	prel = get_pathman_relation_info(parent_oid, NULL);
	rangerel = get_pathman_range_relation(parent_oid, NULL);

	if (!prel || !rangerel || prel->parttype != PT_RANGE)
		PG_RETURN_NULL();

	/* comparison functions */
	cmp_func_1 = *get_cmp_func(p1_type, prel->atttype);
	cmp_func_2 = *get_cmp_func(p2_type, prel->atttype);

	byVal = rangerel->by_val;
	ranges = (RangeEntry *) dsm_array_get_pointer(&rangerel->ranges);
	for (i=0; i<rangerel->ranges.length; i++)
	{
		int c1 = FunctionCall2(&cmp_func_1, p1,
								PATHMAN_GET_DATUM(ranges[i].max, byVal));
		int c2 = FunctionCall2(&cmp_func_2, p2,
								PATHMAN_GET_DATUM(ranges[i].min, byVal));

		if (c1 < 0 && c2 > 0)
			PG_RETURN_BOOL(true);
	}

	PG_RETURN_BOOL(false);
}

/*
 * Acquire partitions lock
 */
Datum
acquire_partitions_lock(PG_FUNCTION_ARGS)
{
	LWLockAcquire(pmstate->edit_partitions_lock, LW_EXCLUSIVE);
	PG_RETURN_NULL();
}

Datum
release_partitions_lock(PG_FUNCTION_ARGS)
{
	LWLockRelease(pmstate->edit_partitions_lock);
	PG_RETURN_NULL();
}
