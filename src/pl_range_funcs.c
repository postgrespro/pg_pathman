/* ------------------------------------------------------------------------
 *
 * pl_range_funcs.c
 *		Utility C functions for stored RANGE procedures
 *
 * Copyright (c) 2016, Postgres Professional
 *
 * ------------------------------------------------------------------------
 */

#include "init.h"
#include "pathman.h"
#include "partition_creation.h"
#include "relation_info.h"
#include "utils.h"

#include "catalog/namespace.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"


/* Function declarations */

PG_FUNCTION_INFO_V1( create_single_range_partition_pl );
PG_FUNCTION_INFO_V1( find_or_create_range_partition);
PG_FUNCTION_INFO_V1( check_range_available_pl );

PG_FUNCTION_INFO_V1( get_part_range_by_oid );
PG_FUNCTION_INFO_V1( get_part_range_by_idx );

PG_FUNCTION_INFO_V1( build_range_condition );
PG_FUNCTION_INFO_V1( build_sequence_name );


/*
 * -----------------------------
 *  Partition creation & checks
 * -----------------------------
 */

/*
 * pl/PgSQL wrapper for the create_single_range_partition().
 */
Datum
create_single_range_partition_pl(PG_FUNCTION_ARGS)
{
	Oid			parent_relid;

	/* RANGE boundaries + value type */
	Datum		start_value,
				end_value;
	Oid			value_type;

	/* Optional: name & tablespace */
	RangeVar   *partition_name_rv;
	char	   *tablespace;

	/* Result (REGCLASS) */
	Oid			partition_relid;


	/* Handle 'parent_relid' */
	if (PG_ARGISNULL(0))
		elog(ERROR, "'parent_relid' should not be NULL");

	/* Handle 'start_value' */
	if (PG_ARGISNULL(1))
		elog(ERROR, "'start_value' should not be NULL");

	/* Handle 'end_value' */
	if (PG_ARGISNULL(2))
		elog(ERROR, "'end_value' should not be NULL");

	/* Fetch mandatory args */
	parent_relid	= PG_GETARG_OID(0);
	start_value		= PG_GETARG_DATUM(1);
	end_value		= PG_GETARG_DATUM(2);
	value_type		= get_fn_expr_argtype(fcinfo->flinfo, 1);

	/* Fetch 'partition_name' */
	if (!PG_ARGISNULL(3))
	{
		List   *qualified_name;
		text   *partition_name;

		partition_name = PG_GETARG_TEXT_P(3);
		qualified_name = textToQualifiedNameList(partition_name);
		partition_name_rv = makeRangeVarFromNameList(qualified_name);
	}
	else partition_name_rv = NULL; /* default */

	/* Fetch 'tablespace' */
	if (!PG_ARGISNULL(4))
	{
		tablespace = TextDatumGetCString(PG_GETARG_TEXT_P(4));
	}
	else tablespace = NULL; /* default */

	/* Create a new RANGE partition and return its Oid */
	partition_relid = create_single_range_partition_internal(parent_relid,
															 start_value,
															 end_value,
															 value_type,
															 partition_name_rv,
															 tablespace);

	PG_RETURN_OID(partition_relid);
}

/*
 * Returns partition oid for specified parent relid and value.
 * In case when partition doesn't exist try to create one.
 */
Datum
find_or_create_range_partition(PG_FUNCTION_ARGS)
{
	Oid						parent_relid = PG_GETARG_OID(0);
	Datum					value = PG_GETARG_DATUM(1);
	Oid						value_type = get_fn_expr_argtype(fcinfo->flinfo, 1);
	const PartRelationInfo *prel;
	FmgrInfo				cmp_func;
	RangeEntry				found_rentry;
	search_rangerel_result	search_state;

	prel = get_pathman_relation_info(parent_relid);
	shout_if_prel_is_invalid(parent_relid, prel, PT_RANGE);

	fill_type_cmp_fmgr_info(&cmp_func,
							getBaseType(value_type),
							getBaseType(prel->atttype));

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
		Oid	child_oid = create_partitions_for_value(parent_relid, value, value_type);

		/* get_pathman_relation_info() will refresh this entry */
		invalidate_pathman_relation_info(parent_relid, NULL);

		PG_RETURN_OID(child_oid);
	}
}

/*
 * Checks if range overlaps with existing partitions.
 * Returns TRUE if overlaps and FALSE otherwise.
 */
Datum
check_range_available_pl(PG_FUNCTION_ARGS)
{
	Oid		parent_relid = PG_GETARG_OID(0);

	Datum	start_value	= PG_GETARG_DATUM(1),
			end_value	= PG_GETARG_DATUM(2);
	Oid		value_type	= get_fn_expr_argtype(fcinfo->flinfo, 1);

	/* Raise ERROR if range overlaps with any partition */
	check_range_available(parent_relid,
						  start_value,
						  end_value,
						  value_type,
						  true);

	PG_RETURN_VOID();
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
get_part_range_by_oid(PG_FUNCTION_ARGS)
{
	Oid						partition_relid = InvalidOid,
							parent_relid;
	PartParentSearch		parent_search;
	uint32					i;
	RangeEntry			   *ranges;
	const PartRelationInfo *prel;

	if (PG_ARGISNULL(0))
		elog(ERROR, "'partition_relid' should not be NULL");
	else
		partition_relid = PG_GETARG_OID(0);

	parent_relid = get_parent_of_partition(partition_relid, &parent_search);
	if (parent_search != PPS_ENTRY_PART_PARENT)
		elog(ERROR, "relation \"%s\" is not a partition",
			 get_rel_name_or_relid(partition_relid));

	prel = get_pathman_relation_info(parent_relid);
	shout_if_prel_is_invalid(parent_relid, prel, PT_RANGE);

	ranges = PrelGetRangesArray(prel);

	/* Look for the specified partition */
	for (i = 0; i < PrelChildrenCount(prel); i++)
		if (ranges[i].child_oid == partition_relid)
		{
			ArrayType  *arr;
			Datum		elems[2] = { ranges[i].min, ranges[i].max };

			arr = construct_array(elems, 2, prel->atttype,
								  prel->attlen, prel->attbyval,
								  prel->attalign);

			PG_RETURN_ARRAYTYPE_P(arr);
		}

	/* No partition found, report error */
	elog(ERROR, "relation \"%s\" has no partition \"%s\"",
		 get_rel_name_or_relid(parent_relid),
		 get_rel_name_or_relid(partition_relid));

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
get_part_range_by_idx(PG_FUNCTION_ARGS)
{
	Oid						parent_relid = InvalidOid;
	int						partition_idx = 0;
	Datum					elems[2];
	RangeEntry			   *ranges;
	const PartRelationInfo *prel;

	if (PG_ARGISNULL(0))
		elog(ERROR, "'parent_relid' should not be NULL");
	else
		parent_relid = PG_GETARG_OID(0);

	if (PG_ARGISNULL(1))
		elog(ERROR, "'partition_idx' should not be NULL");
	else
		partition_idx = PG_GETARG_INT32(1);

	prel = get_pathman_relation_info(parent_relid);
	shout_if_prel_is_invalid(parent_relid, prel, PT_RANGE);

	/* Now we have to deal with 'idx' */
	if (partition_idx < -1)
	{
		elog(ERROR, "negative indices other than -1 (last partition) are not allowed");
	}
	else if (partition_idx == -1)
	{
		partition_idx = PrelLastChild(prel);
	}
	else if (((uint32) abs(partition_idx)) >= PrelChildrenCount(prel))
	{
		elog(ERROR, "partition #%d does not exist (total amount is %u)",
			 partition_idx, PrelChildrenCount(prel));
	}

	ranges = PrelGetRangesArray(prel);

	elems[0] = ranges[partition_idx].min;
	elems[1] = ranges[partition_idx].max;

	PG_RETURN_ARRAYTYPE_P(construct_array(elems, 2,
										  prel->atttype,
										  prel->attlen,
										  prel->attbyval,
										  prel->attalign));
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
		elog(ERROR, "cannot build range condition: "
					"boundaries should be of the same type");

	/* Create range condition CSTRING */
	result = psprintf("%1$s >= '%2$s' AND %1$s < '%3$s'",
					  text_to_cstring(attname),
					  datum_to_cstring(min_bound, min_bound_type),
					  datum_to_cstring(max_bound, max_bound_type));

	PG_RETURN_TEXT_P(cstring_to_text(result));
}

Datum
build_sequence_name(PG_FUNCTION_ARGS)
{
	Oid		parent_relid = PG_GETARG_OID(0);
	Oid		parent_nsp;
	char   *result;

	parent_nsp = get_rel_namespace(parent_relid);

	result = psprintf("%s.%s",
					  quote_identifier(get_namespace_name(parent_nsp)),
					  quote_identifier(build_sequence_name_internal(parent_relid)));

	PG_RETURN_TEXT_P(cstring_to_text(result));
}
