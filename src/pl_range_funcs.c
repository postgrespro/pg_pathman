/* ------------------------------------------------------------------------
 *
 * pl_range_funcs.c
 *		Utility C functions for stored RANGE procedures
 *
 * Copyright (c) 2016, Postgres Professional
 *
 * ------------------------------------------------------------------------
 */

#include "pathman.h"
#include "relation_info.h"

#include "utils/array.h"
#include "utils/builtins.h"


/* Function declarations */

PG_FUNCTION_INFO_V1( find_or_create_range_partition);
PG_FUNCTION_INFO_V1( check_overlap );

PG_FUNCTION_INFO_V1( get_range_by_part_oid );
PG_FUNCTION_INFO_V1( get_range_by_idx );
PG_FUNCTION_INFO_V1( get_min_range_value );
PG_FUNCTION_INFO_V1( get_max_range_value );

PG_FUNCTION_INFO_V1( build_range_condition );


/*
 * -----------------------------
 *  Partition creation & checks
 * -----------------------------
 */

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
 * ------------------------
 *  Various useful getters
 * ------------------------
 */

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
 * ------------------------
 *  Useful string builders
 * ------------------------
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

	char   *result;

	/* This is not going to trigger (not now, at least), just for the safety */
	if (min_bound_type != max_bound_type)
		elog(ERROR, "Cannot build range condition: "
					"boundaries should be of the same type");

	/* Create range condition CSTRING */
	result = psprintf("%1$s >= '%2$s' AND %1$s < '%3$s'",
					  text_to_cstring(attname),
					  datum_to_cstring(min_bound, min_bound_type),
					  datum_to_cstring(max_bound, max_bound_type));

	PG_RETURN_TEXT_P(cstring_to_text(result));
}
