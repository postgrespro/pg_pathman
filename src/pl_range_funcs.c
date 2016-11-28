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
#include "parser/parse_relation.h"
#include "parser/parse_expr.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/ruleutils.h"


static char *deparse_constraint(Oid relid, Node *expr);
static ArrayType *construct_infinitable_array(Bound **elems,
											  uint32_t nelems,
											  Oid elmtype,
											  int elmlen,
											  bool elmbyval,
											  char elmalign);

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
	// Datum		start_value,
	// 			end_value;
	// bool		infinite_start,
	// 			infinite_end;
	Bound		start,
				end;
	Oid			value_type;

	/* Optional: name & tablespace */
	RangeVar   *partition_name_rv;
	char	   *tablespace;

	/* Result (REGCLASS) */
	Oid			partition_relid;


	/* Handle 'parent_relid' */
	if (PG_ARGISNULL(0))
		elog(ERROR, "'parent_relid' should not be NULL");

	/* Fetch mandatory args */
	parent_relid	= PG_GETARG_OID(0);
	// start_value		= PG_GETARG_DATUM(1);
	// end_value		= PG_GETARG_DATUM(2);
	// infinite_start	= PG_ARGISNULL(1);
	// infinite_end	= PG_ARGISNULL(2);
	value_type		= get_fn_expr_argtype(fcinfo->flinfo, 1);
	MakeBound(&start,
			  PG_GETARG_DATUM(1),
			  PG_ARGISNULL(1) ? MINUS_INFINITY : FINITE);
	MakeBound(&end,
			  PG_GETARG_DATUM(2),
			  PG_ARGISNULL(2) ? PLUS_INFINITY : FINITE);

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
															 &start,
															 &end,
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
	Oid			parent_relid = PG_GETARG_OID(0);
	Bound		start_value,
				end_value;
	Oid			value_type	= get_fn_expr_argtype(fcinfo->flinfo, 1);

	MakeBound(&start_value,
			  PG_GETARG_DATUM(1),
			  PG_ARGISNULL(1) ? MINUS_INFINITY : FINITE);
	MakeBound(&end_value,
			  PG_GETARG_DATUM(2),
			  PG_ARGISNULL(2) ? PLUS_INFINITY : FINITE);

	/* Raise ERROR if range overlaps with any partition */
	check_range_available(parent_relid,
						  &start_value,
						  &end_value,
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
			// Datum		elems[2] = { InfinitableGetValue(&ranges[i].min),
			// 						 InfinitableGetValue(&ranges[i].max) };
			// bool		nulls[2] = { IsInfinite(&ranges[i].min),
			// 						 IsInfinite(&ranges[i].max) };
			// int			dims[1] = { 2 };
			// int			lbs[1] = { 1 };

			// arr = construct_md_array(elems, nulls, 1, dims, lbs,
			// 					   prel->atttype, prel->attlen,
			// 					   prel->attbyval, prel->attalign);

			// arr = construct_array(elems, 2, prel->atttype,
			// 					  prel->attlen, prel->attbyval,
			// 					  prel->attalign);
			Bound *elems[2] = { &ranges[i].min, &ranges[i].max };


			arr = construct_infinitable_array(elems, 2,
											  prel->atttype, prel->attlen,
								   			  prel->attbyval, prel->attalign);

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
	// Datum					elems[2];
	Bound				   *elems[2];
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

	elems[0] = &ranges[partition_idx].min;
	elems[1] = &ranges[partition_idx].max;

	PG_RETURN_ARRAYTYPE_P(
		construct_infinitable_array(elems, 2,
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
	Oid		relid = PG_GETARG_OID(0);
	text   *attname = PG_GETARG_TEXT_P(1);

	Bound		min,
				max;
	Oid			bounds_type = get_fn_expr_argtype(fcinfo->flinfo, 2);
	Constraint *con;
	char	   *result;

	MakeBound(&min,
			  PG_GETARG_DATUM(2),
			  PG_ARGISNULL(2) ? MINUS_INFINITY : FINITE);
	MakeBound(&max,
			  PG_GETARG_DATUM(3),
			  PG_ARGISNULL(3) ? PLUS_INFINITY : FINITE);

	con = build_range_check_constraint(relid, text_to_cstring(attname),
									   &min, &max,
									   bounds_type);

	result = deparse_constraint(relid, con->raw_expr);

	PG_RETURN_TEXT_P(cstring_to_text(result));
}

/*
 * Transform constraint into cstring
 */
static char *
deparse_constraint(Oid relid, Node *expr)
{
	Relation		rel;
	RangeTblEntry  *rte;
	Node		   *cooked_expr;
	ParseState	   *pstate;
	List		   *context;
	char		   *result;

	context = deparse_context_for(get_rel_name(relid), relid);

	rel = heap_open(relid, NoLock);

	/* Initialize parse state */
	pstate = make_parsestate(NULL);
	rte = addRangeTableEntryForRelation(pstate, rel, NULL, false, true);
	addRTEtoQuery(pstate, rte, true, true, true);

	/* Transform constraint into executable expression (i.e. cook it) */
	cooked_expr = transformExpr(pstate, expr, EXPR_KIND_CHECK_CONSTRAINT);

	/* Transform expression into string */
	result = deparse_expression(cooked_expr, context, false, false);

	heap_close(rel, NoLock);

	return result;
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

/*
 * Build an 1d array of Bound elements
 *
 *		The main difference from construct_array() is that it will substitute
 *		infinite values with NULL's
 */
static ArrayType *
construct_infinitable_array(Bound **elems,
							uint32_t nelems,
							Oid elmtype,
							int elmlen,
							bool elmbyval,
							char elmalign)
{
	ArrayType  *arr;
	Datum	   *data;
	bool	   *nulls;
	int			dims[1] = { nelems };
	int			lbs[1] = { 1 };
	int 		i;

	data = palloc(sizeof(Datum) * nelems);
	nulls = palloc(sizeof(bool) * nelems);

	for (i = 0; i < nelems; i++)
	{
		data[i] = BoundGetValue(elems[i]);
		nulls[i] = IsInfinite(elems[i]);
	}

	arr = construct_md_array(data, nulls, 1, dims, lbs,
							 elmtype, elmlen,
							 elmbyval, elmalign);

	return arr;
}
