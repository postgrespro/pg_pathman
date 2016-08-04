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
#include "init.h"
#include "utils.h"

#include "access/nbtree.h"
#include "access/xact.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/typcache.h"
#include "utils/array.h"
#include "utils/memutils.h"


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
PG_FUNCTION_INFO_V1( get_type_hash_func );
PG_FUNCTION_INFO_V1( get_hash );
PG_FUNCTION_INFO_V1( build_check_constraint_name_attnum );
PG_FUNCTION_INFO_V1( build_check_constraint_name_attname );


static void on_partitions_created_internal(Oid partitioned_table, bool add_callbacks);
static void on_partitions_updated_internal(Oid partitioned_table, bool add_callbacks);
static void on_partitions_removed_internal(Oid partitioned_table, bool add_callbacks);


/*
 * Callbacks
 */

static void
on_partitions_created_internal(Oid partitioned_table, bool add_callbacks)
{
	elog(DEBUG2, "on_partitions_created() [add_callbacks = %s] "
				 "triggered for relation %u",
		 (add_callbacks ? "true" : "false"), partitioned_table);
}

static void
on_partitions_updated_internal(Oid partitioned_table, bool add_callbacks)
{
	elog(DEBUG2, "on_partitions_updated() [add_callbacks = %s] "
				 "triggered for relation %u",
		 (add_callbacks ? "true" : "false"), partitioned_table);
}

static void
on_partitions_removed_internal(Oid partitioned_table, bool add_callbacks)
{
	elog(DEBUG2, "on_partitions_removed() [add_callbacks = %s] "
				 "triggered for relation %u",
		 (add_callbacks ? "true" : "false"), partitioned_table);
}

/*
 * Thin layer between pure c and pl/PgSQL
 */

Datum
on_partitions_created(PG_FUNCTION_ARGS)
{
	on_partitions_created_internal(PG_GETARG_OID(0), true);
	PG_RETURN_NULL();
}

Datum
on_partitions_updated(PG_FUNCTION_ARGS)
{
	on_partitions_updated_internal(PG_GETARG_OID(0), true);
	PG_RETURN_NULL();
}

Datum
on_partitions_removed(PG_FUNCTION_ARGS)
{
	on_partitions_removed_internal(PG_GETARG_OID(0), true);
	PG_RETURN_NULL();
}

/*
 * Returns partition oid for specified parent relid and value.
 * In case when partition doesn't exist try to create one.
 */
Datum
find_or_create_range_partition(PG_FUNCTION_ARGS)
{
	Oid					relid = PG_GETARG_OID(0);
	Datum				value = PG_GETARG_DATUM(1);
	Oid					value_type = get_fn_expr_argtype(fcinfo->flinfo, 1);
	PartRelationInfo   *prel;
	FmgrInfo			cmp_func;
	RangeEntry			found_rentry;
	search_rangerel_result search_state;

	prel = get_pathman_relation_info(relid, NULL);

	if (!prel)
		PG_RETURN_NULL();

	fill_type_cmp_fmgr_info(&cmp_func, value_type, prel->atttype);

	search_state = search_range_partition_eq(value, &cmp_func,prel,
											 &found_rentry);

	/*
	 * If found then just return oid, else create new partitions
	 */
	if (search_state == SEARCH_RANGEREL_FOUND)
		PG_RETURN_OID(found_rentry.child_oid);
	/*
	 * If not found and value is between first and last partitions
	 */
	else if (search_state == SEARCH_RANGEREL_GAP)
		PG_RETURN_NULL();
	else
	{
		Oid child_oid = InvalidOid;

		LWLockAcquire(pmstate->load_config_lock, LW_EXCLUSIVE);
		LWLockAcquire(pmstate->edit_partitions_lock, LW_EXCLUSIVE);

		/*
		 * Check if someone else has already created partition.
		 */
		search_state = search_range_partition_eq(value, &cmp_func, prel,
												 &found_rentry);
		if (search_state == SEARCH_RANGEREL_FOUND)
		{
			LWLockRelease(pmstate->load_config_lock);
			LWLockRelease(pmstate->edit_partitions_lock);

			PG_RETURN_OID(found_rentry.child_oid);
		}

		child_oid = create_partitions(relid, value, value_type);

		LWLockRelease(pmstate->load_config_lock);
		LWLockRelease(pmstate->edit_partitions_lock);

		PG_RETURN_OID(child_oid);
	}
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
	Oid					parent_oid = PG_GETARG_OID(0);
	Oid					child_oid = PG_GETARG_OID(1);
	const int			nelems = 2;
	int					i;
	bool				found = false;
	Datum			   *elems;
	PartRelationInfo   *prel;
	RangeEntry		   *ranges;
	Oid				   *parts;
	TypeCacheEntry	   *tce;
	ArrayType		   *arr;

	prel = get_pathman_relation_info(parent_oid, NULL);

	if (!prel)
		PG_RETURN_NULL();

	ranges = PrelGetRangesArray(prel, true);
	parts = PrelGetChildrenArray(prel, true);
	tce = lookup_type_cache(prel->atttype, 0);

	/* Looking for specified partition */
	for (i = 0; i < PrelChildrenCount(prel); i++)
		if (parts[i] == child_oid)
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
	Oid					parent_oid = PG_GETARG_OID(0);
	int					idx = PG_GETARG_INT32(1);
	PartRelationInfo   *prel;
	RangeEntry		   *ranges;
	RangeEntry			re;
	Datum			   *elems;
	TypeCacheEntry	   *tce;

	prel = get_pathman_relation_info(parent_oid, NULL);

	if (!prel || idx >= PrelChildrenCount(prel))
		PG_RETURN_NULL();

	tce = lookup_type_cache(prel->atttype, 0);
	ranges = PrelGetRangesArray(prel, true);
	if (idx >= 0)
		re = ranges[idx];
	else
		re = ranges[PrelChildrenCount(prel) - 1];

	elems = palloc(2 * sizeof(Datum));
	elems[0] = re.min;
	elems[1] = re.max;

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
	Oid					parent_oid = PG_GETARG_OID(0);
	PartRelationInfo   *prel;
	RangeEntry		   *ranges;

	prel = get_pathman_relation_info(parent_oid, NULL);

	if (!prel || prel->parttype != PT_RANGE || PrelChildrenCount(prel) == 0)
		PG_RETURN_NULL();

	ranges = PrelGetRangesArray(prel, true);

	PG_RETURN_DATUM(ranges[0].min);
}

/*
 * Returns max value of the last range for relation
 */
Datum
get_max_range_value(PG_FUNCTION_ARGS)
{
	Oid					parent_oid = PG_GETARG_OID(0);
	PartRelationInfo   *prel;
	RangeEntry		   *ranges;

	prel = get_pathman_relation_info(parent_oid, NULL);

	if (!prel || prel->parttype != PT_RANGE || PrelChildrenCount(prel) == 0)
		PG_RETURN_NULL();

	ranges = PrelGetRangesArray(prel, true);

	PG_RETURN_DATUM(ranges[PrelChildrenCount(prel) - 1].max);
}

/*
 * Checks if range overlaps with existing partitions.
 * Returns TRUE if overlaps and FALSE otherwise.
 */
Datum
check_overlap(PG_FUNCTION_ARGS)
{
	Oid					partitioned_table = PG_GETARG_OID(0);

	Datum				p1 = PG_GETARG_DATUM(1),
						p2 = PG_GETARG_DATUM(2);

	Oid					p1_type = get_fn_expr_argtype(fcinfo->flinfo, 1),
						p2_type = get_fn_expr_argtype(fcinfo->flinfo, 2);

	FmgrInfo			cmp_func_1,
						cmp_func_2;

	PartRelationInfo   *prel;
	RangeEntry		   *ranges;
	int					i;

	prel = get_pathman_relation_info(partitioned_table, NULL);

	if (!prel || prel->parttype != PT_RANGE)
		PG_RETURN_NULL();

	/* comparison functions */
	fill_type_cmp_fmgr_info(&cmp_func_1, p1_type, prel->atttype);
	fill_type_cmp_fmgr_info(&cmp_func_2, p2_type, prel->atttype);

	ranges = PrelGetRangesArray(prel, true);
	for (i = 0; i < PrelChildrenCount(prel); i++)
	{
		int c1 = FunctionCall2(&cmp_func_1, p1, ranges[i].max);
		int c2 = FunctionCall2(&cmp_func_2, p2, ranges[i].min);

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

/*
 * Returns hash function OID for specified type
 */
Datum
get_type_hash_func(PG_FUNCTION_ARGS)
{
	TypeCacheEntry *tce;
	Oid 			type_oid = PG_GETARG_OID(0);

	tce = lookup_type_cache(type_oid, TYPECACHE_HASH_PROC);

	PG_RETURN_OID(tce->hash_proc);
}

Datum
get_hash(PG_FUNCTION_ARGS)
{
	uint32	value = PG_GETARG_UINT32(0),
			part_count = PG_GETARG_UINT32(1);

	PG_RETURN_UINT32(make_hash(value, part_count));
}

Datum
build_check_constraint_name_attnum(PG_FUNCTION_ARGS)
{
	Oid			relid = PG_GETARG_OID(0);
	AttrNumber	attnum = PG_GETARG_INT16(1);
	const char *result;

	if (!get_rel_name(relid))
		elog(ERROR, "Invalid relation %u", relid);

	/* We explicitly do not support system attributes */
	if (attnum == InvalidAttrNumber || attnum < 0)
		elog(ERROR, "Cannot build check constraint name: "
					"invalid attribute number %i", attnum);

	result = build_check_constraint_name_internal(relid, attnum);

	PG_RETURN_TEXT_P(cstring_to_text(result));
}

Datum
build_check_constraint_name_attname(PG_FUNCTION_ARGS)
{
	Oid			relid = PG_GETARG_OID(0);
	text	   *attname = PG_GETARG_TEXT_P(1);
	AttrNumber	attnum = get_attnum(relid, text_to_cstring(attname));
	const char *result;

	if (!get_rel_name(relid))
		elog(ERROR, "Invalid relation %u", relid);

	if (attnum == InvalidAttrNumber)
		elog(ERROR, "Relation '%s' has no column '%s'",
			 get_rel_name(relid),
			 text_to_cstring(attname));

	result = build_check_constraint_name_internal(relid, attnum);

	PG_RETURN_TEXT_P(cstring_to_text(result));
}
