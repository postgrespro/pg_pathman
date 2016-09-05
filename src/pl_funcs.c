/* ------------------------------------------------------------------------
 *
 * pl_funcs.c
 *		Utility C functions for stored procedures
 *
 * Copyright (c) 2015-2016, Postgres Professional
 *
 * ------------------------------------------------------------------------
 */

#include "init.h"
#include "pathman.h"
#include "relation_info.h"
#include "utils.h"
#include "xact_handling.h"

#include "access/htup_details.h"
#include "access/nbtree.h"
#include "access/xact.h"
#include "catalog/indexing.h"
#include "commands/sequence.h"
#include "miscadmin.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include <utils/inval.h>
#include "utils/memutils.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"
#include "utils/typcache.h"


/* declarations */
PG_FUNCTION_INFO_V1( on_partitions_created );
PG_FUNCTION_INFO_V1( on_partitions_updated );
PG_FUNCTION_INFO_V1( on_partitions_removed );
PG_FUNCTION_INFO_V1( get_parent_of_partition_pl );
PG_FUNCTION_INFO_V1( get_attribute_type_name );
PG_FUNCTION_INFO_V1( find_or_create_range_partition);
PG_FUNCTION_INFO_V1( get_range_by_idx );
PG_FUNCTION_INFO_V1( get_range_by_part_oid );
PG_FUNCTION_INFO_V1( get_min_range_value );
PG_FUNCTION_INFO_V1( get_max_range_value );
PG_FUNCTION_INFO_V1( get_type_hash_func );
PG_FUNCTION_INFO_V1( get_hash_part_idx );
PG_FUNCTION_INFO_V1( check_overlap );
PG_FUNCTION_INFO_V1( build_range_condition );
PG_FUNCTION_INFO_V1( build_check_constraint_name_attnum );
PG_FUNCTION_INFO_V1( build_check_constraint_name_attname );
PG_FUNCTION_INFO_V1( build_update_trigger_func_name );
PG_FUNCTION_INFO_V1( build_update_trigger_name );
PG_FUNCTION_INFO_V1( is_date_type );
PG_FUNCTION_INFO_V1( is_attribute_nullable );
PG_FUNCTION_INFO_V1( add_to_pathman_config );
PG_FUNCTION_INFO_V1( invalidate_relcache );
PG_FUNCTION_INFO_V1( lock_partitioned_relation );
PG_FUNCTION_INFO_V1( prevent_relation_modification );
PG_FUNCTION_INFO_V1( debug_capture );


static void on_partitions_created_internal(Oid partitioned_table, bool add_callbacks);
static void on_partitions_updated_internal(Oid partitioned_table, bool add_callbacks);
static void on_partitions_removed_internal(Oid partitioned_table, bool add_callbacks);


/*
 * Extracted common check.
 */
static bool
check_relation_exists(Oid relid)
{
	return get_rel_type_id(relid) != InvalidOid;
}


/*
 * Callbacks.
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
	bool found;

	elog(DEBUG2, "on_partitions_updated() [add_callbacks = %s] "
				 "triggered for relation %u",
		 (add_callbacks ? "true" : "false"), partitioned_table);

	invalidate_pathman_relation_info(partitioned_table, &found);
}

static void
on_partitions_removed_internal(Oid partitioned_table, bool add_callbacks)
{
	elog(DEBUG2, "on_partitions_removed() [add_callbacks = %s] "
				 "triggered for relation %u",
		 (add_callbacks ? "true" : "false"), partitioned_table);
}

/*
 * Thin layer between pure C and pl/PgSQL.
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
 * Get parent of a specified partition.
 */
Datum
get_parent_of_partition_pl(PG_FUNCTION_ARGS)
{
	Oid					partition = PG_GETARG_OID(0);
	PartParentSearch	parent_search;
	Oid					parent;

	/* Fetch parent & write down search status */
	parent = get_parent_of_partition(partition, &parent_search);

	/* We MUST be sure :) */
	Assert(parent_search != PPS_NOT_SURE);

	/* It must be parent known by pg_pathman */
	if (parent_search == PPS_ENTRY_PART_PARENT)
		PG_RETURN_OID(parent);
	else
	{
		elog(ERROR, "\%s\" is not pg_pathman's partition",
			 get_rel_name_or_relid(partition));

		PG_RETURN_NULL();
	}
}

/*
 * Get type (as text) of a given attribute.
 */
Datum
get_attribute_type_name(PG_FUNCTION_ARGS)
{
	Oid			relid = PG_GETARG_OID(0);
	text	   *attname = PG_GETARG_TEXT_P(1);
	char	   *result;
	HeapTuple	tp;

	/* NOTE: for now it's the most efficient way */
	tp = SearchSysCacheAttName(relid, text_to_cstring(attname));
	if (HeapTupleIsValid(tp))
	{
		Form_pg_attribute att_tup = (Form_pg_attribute) GETSTRUCT(tp);
		result = format_type_be(att_tup->atttypid);
		ReleaseSysCache(tp);

		PG_RETURN_TEXT_P(cstring_to_text(result));
	}
	else
		elog(ERROR, "Cannot find type name for attribute \"%s\" "
					"of relation \"%s\"",
			 text_to_cstring(attname), get_rel_name_or_relid(relid));

	PG_RETURN_NULL(); /* keep compiler happy */
}

/*
 * Returns partition oid for specified parent relid and value.
 * In case when partition doesn't exist try to create one.
 */
Datum
find_or_create_range_partition(PG_FUNCTION_ARGS)
{
	Oid						parent_oid = PG_GETARG_OID(0);
	Datum					value = PG_GETARG_DATUM(1);
	Oid						value_type = get_fn_expr_argtype(fcinfo->flinfo, 1);
	const PartRelationInfo *prel;
	FmgrInfo				cmp_func;
	RangeEntry				found_rentry;
	search_rangerel_result	search_state;

	prel = get_pathman_relation_info(parent_oid);
	shout_if_prel_is_invalid(parent_oid, prel, PT_RANGE);

	fill_type_cmp_fmgr_info(&cmp_func, value_type, prel->atttype);

	/* Use available PartRelationInfo to find partition */
	search_state = search_range_partition_eq(value, &cmp_func, prel,
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
		Oid	child_oid = create_partitions(parent_oid, value, value_type);

		/* get_pathman_relation_info() will refresh this entry */
		invalidate_pathman_relation_info(parent_oid, NULL);

		PG_RETURN_OID(child_oid);
	}
}

/*
 * Returns range entry (min, max) (in form of array).
 *
 * arg #1 is the parent's Oid.
 * arg #2 is the partition's Oid.
 */
Datum
get_range_by_part_oid(PG_FUNCTION_ARGS)
{
	Oid						parent_oid = PG_GETARG_OID(0);
	Oid						child_oid = PG_GETARG_OID(1);
	uint32					i;
	RangeEntry			   *ranges;
	const PartRelationInfo *prel;

	prel = get_pathman_relation_info(parent_oid);
	shout_if_prel_is_invalid(parent_oid, prel, PT_RANGE);

	ranges = PrelGetRangesArray(prel);

	/* Look for the specified partition */
	for (i = 0; i < PrelChildrenCount(prel); i++)
		if (ranges[i].child_oid == child_oid)
		{
			ArrayType  *arr;
			Datum		elems[2] = { ranges[i].min, ranges[i].max };

			arr = construct_array(elems, 2, prel->atttype,
								  prel->attlen, prel->attbyval,
								  prel->attalign);

			PG_RETURN_ARRAYTYPE_P(arr);
		}

	/* No partition found, report error */
	elog(ERROR, "Relation \"%s\" has no partition \"%s\"",
		 get_rel_name_or_relid(parent_oid),
		 get_rel_name_or_relid(child_oid));

	PG_RETURN_NULL(); /* keep compiler happy */
}

/*
 * Returns N-th range entry (min, max) (in form of array).
 *
 * arg #1 is the parent's Oid.
 * arg #2 is the index of the range
 *		(if it is negative then the last range will be returned).
 */
Datum
get_range_by_idx(PG_FUNCTION_ARGS)
{
	Oid						parent_oid = PG_GETARG_OID(0);
	int						idx = PG_GETARG_INT32(1);
	Datum					elems[2];
	RangeEntry			   *ranges;
	const PartRelationInfo *prel;

	prel = get_pathman_relation_info(parent_oid);
	shout_if_prel_is_invalid(parent_oid, prel, PT_RANGE);

	/* Now we have to deal with 'idx' */
	if (idx < -1)
	{
		elog(ERROR, "Negative indices other than -1 (last partition) are not allowed");
	}
	else if (idx == -1)
	{
		idx = PrelLastChild(prel);
	}
	else if (((uint32) abs(idx)) >= PrelChildrenCount(prel))
	{
		elog(ERROR, "Partition #%d does not exist (total amount is %u)",
			 idx, PrelChildrenCount(prel));
	}

	ranges = PrelGetRangesArray(prel);

	elems[0] = ranges[idx].min;
	elems[1] = ranges[idx].max;

	PG_RETURN_ARRAYTYPE_P(construct_array(elems, 2,
										  prel->atttype,
										  prel->attlen,
										  prel->attbyval,
										  prel->attalign));
}

/*
 * Returns min value of the first range for relation.
 */
Datum
get_min_range_value(PG_FUNCTION_ARGS)
{
	Oid						parent_oid = PG_GETARG_OID(0);
	RangeEntry			   *ranges;
	const PartRelationInfo *prel;

	prel = get_pathman_relation_info(parent_oid);
	shout_if_prel_is_invalid(parent_oid, prel, PT_RANGE);

	ranges = PrelGetRangesArray(prel);

	PG_RETURN_DATUM(ranges[0].min);
}

/*
 * Returns max value of the last range for relation.
 */
Datum
get_max_range_value(PG_FUNCTION_ARGS)
{
	Oid						parent_oid = PG_GETARG_OID(0);
	RangeEntry			   *ranges;
	const PartRelationInfo *prel;

	prel = get_pathman_relation_info(parent_oid);
	shout_if_prel_is_invalid(parent_oid, prel, PT_RANGE);

	ranges = PrelGetRangesArray(prel);

	PG_RETURN_DATUM(ranges[PrelLastChild(prel)].max);
}

/*
 * Checks if range overlaps with existing partitions.
 * Returns TRUE if overlaps and FALSE otherwise.
 */
Datum
check_overlap(PG_FUNCTION_ARGS)
{
	Oid						parent_oid = PG_GETARG_OID(0);

	Datum					p1 = PG_GETARG_DATUM(1),
							p2 = PG_GETARG_DATUM(2);

	Oid						p1_type = get_fn_expr_argtype(fcinfo->flinfo, 1),
							p2_type = get_fn_expr_argtype(fcinfo->flinfo, 2);

	FmgrInfo				cmp_func_1,
							cmp_func_2;

	uint32					i;
	RangeEntry			   *ranges;
	const PartRelationInfo *prel;

	prel = get_pathman_relation_info(parent_oid);
	shout_if_prel_is_invalid(parent_oid, prel, PT_RANGE);

	/* comparison functions */
	fill_type_cmp_fmgr_info(&cmp_func_1, p1_type, prel->atttype);
	fill_type_cmp_fmgr_info(&cmp_func_2, p2_type, prel->atttype);

	ranges = PrelGetRangesArray(prel);
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
 * HASH-related stuff.
 */

/* Returns hash function's OID for a specified type. */
Datum
get_type_hash_func(PG_FUNCTION_ARGS)
{
	TypeCacheEntry *tce;
	Oid 			type_oid = PG_GETARG_OID(0);

	tce = lookup_type_cache(type_oid, TYPECACHE_HASH_PROC);

	PG_RETURN_OID(tce->hash_proc);
}

/* Wrapper for hash_to_part_index() */
Datum
get_hash_part_idx(PG_FUNCTION_ARGS)
{
	uint32	value = PG_GETARG_UINT32(0),
			part_count = PG_GETARG_UINT32(1);

	PG_RETURN_UINT32(hash_to_part_index(value, part_count));
}

/*
 * Traits.
 */

Datum
is_date_type(PG_FUNCTION_ARGS)
{
	PG_RETURN_BOOL(is_date_type_internal(PG_GETARG_OID(0)));
}

Datum
is_attribute_nullable(PG_FUNCTION_ARGS)
{
	Oid			relid = PG_GETARG_OID(0);
	text	   *attname = PG_GETARG_TEXT_P(1);
	bool		result = true;
	HeapTuple	tp;

	tp = SearchSysCacheAttName(relid, text_to_cstring(attname));
	if (HeapTupleIsValid(tp))
	{
		Form_pg_attribute att_tup = (Form_pg_attribute) GETSTRUCT(tp);
		result = !att_tup->attnotnull;
		ReleaseSysCache(tp);
	}
	else
		elog(ERROR, "Cannot find type name for attribute \"%s\" "
					"of relation \"%s\"",
			 text_to_cstring(attname), get_rel_name_or_relid(relid));

	PG_RETURN_BOOL(result); /* keep compiler happy */
}


/*
 * Useful string builders.
 */

/* Build range condition for a CHECK CONSTRAINT. */
Datum
build_range_condition(PG_FUNCTION_ARGS)
{
	text   *attname = PG_GETARG_TEXT_P(0);

	Datum	min_bound = PG_GETARG_DATUM(1),
			max_bound = PG_GETARG_DATUM(2);

	Oid		min_bound_type = get_fn_expr_argtype(fcinfo->flinfo, 1),
			max_bound_type = get_fn_expr_argtype(fcinfo->flinfo, 2);

	char   *subst_str; /* substitution string */
	char   *result;

	/* This is not going to trigger (not now, at least), just for the safety */
	if (min_bound_type != max_bound_type)
		elog(ERROR, "Cannot build range condition: "
					"boundaries should be of the same type");

	/* Check if we need single quotes */
	/* TODO: check for primitive types instead, that would be better */
	if (is_date_type_internal(min_bound_type) ||
		is_string_type_internal(min_bound_type))
	{
		subst_str = "%1$s >= '%2$s' AND %1$s < '%3$s'";
	}
	else
		subst_str = "%1$s >= %2$s AND %1$s < %3$s";

	/* Create range condition CSTRING */
	result = psprintf(subst_str,
					  text_to_cstring(attname),
					  datum_to_cstring(min_bound, min_bound_type),
					  datum_to_cstring(max_bound, max_bound_type));

	PG_RETURN_TEXT_P(cstring_to_text(result));
}

Datum
build_check_constraint_name_attnum(PG_FUNCTION_ARGS)
{
	Oid			relid = PG_GETARG_OID(0);
	AttrNumber	attnum = PG_GETARG_INT16(1);
	const char *result;

	if (!check_relation_exists(relid))
		elog(ERROR, "Invalid relation %u", relid);

	/* We explicitly do not support system attributes */
	if (attnum == InvalidAttrNumber || attnum < 0)
		elog(ERROR, "Cannot build check constraint name: "
					"invalid attribute number %i", attnum);

	result = build_check_constraint_name_internal(relid, attnum);

	PG_RETURN_TEXT_P(cstring_to_text(quote_identifier(result)));
}

Datum
build_check_constraint_name_attname(PG_FUNCTION_ARGS)
{
	Oid			relid = PG_GETARG_OID(0);
	text	   *attname = PG_GETARG_TEXT_P(1);
	AttrNumber	attnum = get_attnum(relid, text_to_cstring(attname));
	const char *result;

	if (!check_relation_exists(relid))
		elog(ERROR, "Invalid relation %u", relid);

	if (attnum == InvalidAttrNumber)
		elog(ERROR, "Relation \"%s\" has no column '%s'",
			 get_rel_name_or_relid(relid), text_to_cstring(attname));

	result = build_check_constraint_name_internal(relid, attnum);

	PG_RETURN_TEXT_P(cstring_to_text(quote_identifier(result)));
}

Datum
build_update_trigger_func_name(PG_FUNCTION_ARGS)
{
	Oid			relid = PG_GETARG_OID(0),
				nspid;
	const char *result;

	/* Check that relation exists */
	if (!check_relation_exists(relid))
		elog(ERROR, "Invalid relation %u", relid);

	nspid = get_rel_namespace(relid);
	result = psprintf("%s.%s",
					  quote_identifier(get_namespace_name(nspid)),
					  quote_identifier(psprintf("%s_upd_trig_func",
												get_rel_name(relid))));

	PG_RETURN_TEXT_P(cstring_to_text(result));
}

Datum
build_update_trigger_name(PG_FUNCTION_ARGS)
{
	Oid			relid = PG_GETARG_OID(0);
	const char *result; /* trigger's name can't be qualified */

	/* Check that relation exists */
	if (!check_relation_exists(relid))
		elog(ERROR, "Invalid relation %u", relid);

	result = quote_identifier(psprintf("%s_upd_trig", get_rel_name(relid)));

	PG_RETURN_TEXT_P(cstring_to_text(result));
}


/*
 * Try to add previously partitioned table to PATHMAN_CONFIG.
 */
Datum
add_to_pathman_config(PG_FUNCTION_ARGS)
{
	Oid					relid;
	text			   *attname;
	PartType			parttype;

	Relation			pathman_config;
	Datum				values[Natts_pathman_config];
	bool				isnull[Natts_pathman_config];
	HeapTuple			htup;
	CatalogIndexState	indstate;

	PathmanInitState	init_state;
	MemoryContext		old_mcxt = CurrentMemoryContext;

	if (PG_ARGISNULL(0))
		elog(ERROR, "parent_relid should not be null");

	if (PG_ARGISNULL(1))
		elog(ERROR, "attname should not be null");

	/* Read parameters */
	relid = PG_GETARG_OID(0);
	attname = PG_GETARG_TEXT_P(1);

	/* Check that relation exists */
	if (!check_relation_exists(relid))
		elog(ERROR, "Invalid relation %u", relid);

	if (get_attnum(relid, text_to_cstring(attname)) == InvalidAttrNumber)
		elog(ERROR, "Relation \"%s\" has no column '%s'",
			 get_rel_name_or_relid(relid), text_to_cstring(attname));

	/* Select partitioning type using 'range_interval' */
	parttype = PG_ARGISNULL(2) ? PT_HASH : PT_RANGE;

	/*
	 * Initialize columns (partrel, attname, parttype, range_interval).
	 */
	values[Anum_pathman_config_partrel - 1]			= ObjectIdGetDatum(relid);
	isnull[Anum_pathman_config_partrel - 1]			= false;

	values[Anum_pathman_config_attname - 1]			= PointerGetDatum(attname);
	isnull[Anum_pathman_config_attname - 1]			= false;

	values[Anum_pathman_config_parttype - 1]		= Int32GetDatum(parttype);
	isnull[Anum_pathman_config_parttype - 1]		= false;

	values[Anum_pathman_config_range_interval - 1]	= PG_GETARG_DATUM(2);
	isnull[Anum_pathman_config_range_interval - 1]	= PG_ARGISNULL(2);

	/* Insert new row into PATHMAN_CONFIG */
	pathman_config = heap_open(get_pathman_config_relid(), RowExclusiveLock);
	htup = heap_form_tuple(RelationGetDescr(pathman_config), values, isnull);
	simple_heap_insert(pathman_config, htup);
	indstate = CatalogOpenIndexes(pathman_config);
	CatalogIndexInsert(indstate, htup);
	CatalogCloseIndexes(indstate);
	heap_close(pathman_config, RowExclusiveLock);

	/* Now try to create a PartRelationInfo */
	PG_TRY();
	{
		/* Some flags might change during refresh attempt */
		save_pathman_init_state(&init_state);

		refresh_pathman_relation_info(relid, parttype, text_to_cstring(attname));
	}
	PG_CATCH();
	{
		ErrorData *edata;

		/* Switch to the original context & copy edata */
		MemoryContextSwitchTo(old_mcxt);
		edata = CopyErrorData();
		FlushErrorState();

		/* We have to restore all changed flags */
		restore_pathman_init_state(&init_state);

		/* Show error message */
		elog(ERROR, "%s", edata->message);

		FreeErrorData(edata);
	}
	PG_END_TRY();

	PG_RETURN_BOOL(true);
}


/*
 * Invalidate relcache for a specified relation.
 */
Datum
invalidate_relcache(PG_FUNCTION_ARGS)
{
	Oid		relid = PG_GETARG_OID(0);

	if (check_relation_exists(relid))
		CacheInvalidateRelcacheByRelid(relid);

	PG_RETURN_VOID();
}


/*
 * Acquire appropriate lock on a partitioned relation.
 */
Datum
lock_partitioned_relation(PG_FUNCTION_ARGS)
{
	Oid			relid = PG_GETARG_OID(0);

	/* Lock partitioned relation till transaction's end */
	xact_lock_partitioned_rel(relid, false);

	PG_RETURN_VOID();
}

/*
 * Lock relation exclusively & check for current isolation level.
 */
Datum
prevent_relation_modification(PG_FUNCTION_ARGS)
{
	Oid			relid = PG_GETARG_OID(0);

	/*
	 * Check that isolation level is READ COMMITTED.
	 * Else we won't be able to see new rows
	 * which could slip through locks.
	 */
	if (!xact_is_level_read_committed())
		ereport(ERROR,
				(errmsg("Cannot perform blocking partitioning operation"),
				 errdetail("Expected READ COMMITTED isolation level")));

	/*
	 * Check if table is being modified
	 * concurrently in a separate transaction.
	 */
	if (!xact_lock_rel_exclusive(relid, true))
		ereport(ERROR,
				(errmsg("Cannot perform blocking partitioning operation"),
				 errdetail("Table \"%s\" is being modified concurrently",
						   get_rel_name_or_relid(relid))));

	PG_RETURN_VOID();
}


/*
 * NOTE: used for DEBUG, set breakpoint here.
 */
Datum
debug_capture(PG_FUNCTION_ARGS)
{
	static float8 sleep_time = 0;
	DirectFunctionCall1(pg_sleep, Float8GetDatum(sleep_time));

	/* Write something (doesn't really matter) */
	elog(WARNING, "debug_capture [%u]", MyProcPid);

	PG_RETURN_VOID();
}
