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
#include "xact_handling.h"

#include "access/xact.h"
#include "catalog/namespace.h"
#include "catalog/pg_type.h"
#include "catalog/heap.h"
#include "commands/tablecmds.h"
#include "executor/spi.h"
#include "nodes/nodeFuncs.h"
#include "parser/parse_relation.h"
#include "parser/parse_expr.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/numeric.h"
#include "utils/ruleutils.h"
#include "utils/syscache.h"

#if PG_VERSION_NUM >= 100000
#include "utils/regproc.h"
#include "utils/varlena.h"
#include <math.h>
#endif


/* Function declarations */

PG_FUNCTION_INFO_V1( create_single_range_partition_pl );
PG_FUNCTION_INFO_V1( create_range_partitions_internal );
PG_FUNCTION_INFO_V1( check_range_available_pl );
PG_FUNCTION_INFO_V1( generate_range_bounds_pl );

PG_FUNCTION_INFO_V1( get_part_range_by_oid );
PG_FUNCTION_INFO_V1( get_part_range_by_idx );

PG_FUNCTION_INFO_V1( build_range_condition );
PG_FUNCTION_INFO_V1( build_sequence_name );
PG_FUNCTION_INFO_V1( merge_range_partitions );
PG_FUNCTION_INFO_V1( drop_range_partition_expand_next );
PG_FUNCTION_INFO_V1( validate_interval_value );


static char *deparse_constraint(Oid relid, Node *expr);
static ArrayType *construct_infinitable_array(Bound *elems,
											  int nelems,
											  Oid elmtype,
											  int elmlen,
											  bool elmbyval,
											  char elmalign);
static void check_range_adjacence(Oid cmp_proc, Oid collid, List *ranges);
static void merge_range_partitions_internal(Oid parent,
											Oid *parts,
											uint32 nparts);
static void modify_range_constraint(Oid partition_relid,
									const char *expression,
									Oid expression_type,
									const Bound *lower,
									const Bound *upper);
static char *get_qualified_rel_name(Oid relid);
static void drop_table_by_oid(Oid relid);
static bool interval_is_trivial(Oid atttype,
								Datum interval,
								Oid interval_type);


/*
 * -----------------------------
 *  Partition creation & checks
 * -----------------------------
 */

/* pl/PgSQL wrapper for the create_single_range_partition(). */
Datum
create_single_range_partition_pl(PG_FUNCTION_ARGS)
{
	Oid			parent_relid,
				partition_relid;

	/* RANGE boundaries + value type */
	Bound		start,
				end;
	Oid			bounds_type;

	/* Optional: name & tablespace */
	RangeVar   *partition_name_rv;
	char	   *tablespace;

	Datum		values[Natts_pathman_config];
	bool		isnull[Natts_pathman_config];


	/* Handle 'parent_relid' */
	if (!PG_ARGISNULL(0))
	{
		parent_relid = PG_GETARG_OID(0);
	}
	else ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("'parent_relid' should not be NULL")));

	/* Check that table is partitioned by RANGE */
	if (!pathman_config_contains_relation(parent_relid, values, isnull, NULL, NULL) ||
		DatumGetPartType(values[Anum_pathman_config_parttype - 1]) != PT_RANGE)
	{
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("table \"%s\" is not partitioned by RANGE",
								get_rel_name_or_relid(parent_relid))));
	}

	bounds_type = get_fn_expr_argtype(fcinfo->flinfo, 1);

	start = PG_ARGISNULL(1) ?
				MakeBoundInf(MINUS_INFINITY) :
				MakeBound(PG_GETARG_DATUM(1));

	end = PG_ARGISNULL(2) ?
				MakeBoundInf(PLUS_INFINITY) :
				MakeBound(PG_GETARG_DATUM(2));

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
															 bounds_type,
															 partition_name_rv,
															 tablespace);

	PG_RETURN_OID(partition_relid);
}

Datum
create_range_partitions_internal(PG_FUNCTION_ARGS)
{
	Oid				parent_relid;
	int16			typlen;
	bool			typbyval;
	char			typalign;
	FmgrInfo		cmp_func;

	/* Partition names and tablespaces */
	char		  **partnames		= NULL;
	RangeVar	  **rangevars		= NULL;
	char		  **tablespaces		= NULL;
	int				npartnames		= 0;
	int				ntablespaces	= 0;

	/* Bounds */
	ArrayType	   *bounds;
	Oid				bounds_type;
	Datum		   *datums;
	bool		   *nulls;
	int				ndatums;
	int				i;

	/* Extract parent's Oid */
	if (!PG_ARGISNULL(0))
	{
		parent_relid = PG_GETARG_OID(0);
	}
	else ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("'parent_relid' should not be NULL")));

	/* Extract array of bounds */
	if (!PG_ARGISNULL(1))
	{
		bounds = PG_GETARG_ARRAYTYPE_P(1);
		bounds_type = ARR_ELEMTYPE(bounds);
	}
	else ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("'bounds' should not be NULL")));

	/* Extract partition names */
	if (!PG_ARGISNULL(2))
	{
		partnames = deconstruct_text_array(PG_GETARG_DATUM(2), &npartnames);
		rangevars = qualified_relnames_to_rangevars(partnames, npartnames);
	}

	/* Extract partition tablespaces */
	if (!PG_ARGISNULL(3))
		tablespaces = deconstruct_text_array(PG_GETARG_DATUM(3), &ntablespaces);

	/* Extract bounds */
	get_typlenbyvalalign(bounds_type, &typlen, &typbyval, &typalign);
	deconstruct_array(bounds, bounds_type,
					  typlen, typbyval, typalign,
					  &datums, &nulls, &ndatums);

	if (partnames && npartnames != ndatums - 1)
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("wrong length of 'partition_names' array"),
						errdetail("number of 'partition_names' must be less than "
								  "'bounds' array length by one")));

	if (tablespaces && ntablespaces != ndatums - 1)
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("wrong length of 'tablespaces' array"),
						errdetail("number of 'tablespaces' must be less than "
								  "'bounds' array length by one")));

	/* Check if bounds array is ascending */
	fill_type_cmp_fmgr_info(&cmp_func,
							getBaseType(bounds_type),
							getBaseType(bounds_type));

	/* Validate bounds */
	for (i = 0; i < ndatums; i++)
	{
		/* Disregard 1st bound */
		if (i == 0) continue;

		/* Check that bound is valid */
		if (nulls[i])
			ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
							errmsg("only first bound can be NULL")));

		/* Check that bounds are ascending */
		if (!nulls[i - 1] && !check_le(&cmp_func, InvalidOid,
									   datums[i - 1], datums[i]))
			ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
							errmsg("'bounds' array must be ascending")));
	}

	/* Create partitions using provided bounds */
	for (i = 0; i < ndatums - 1; i++)
	{
		Bound		start	= nulls[i] ?
								MakeBoundInf(MINUS_INFINITY) :
								MakeBound(datums[i]),

					end		= nulls[i + 1] ?
								MakeBoundInf(PLUS_INFINITY) :
								MakeBound(datums[i + 1]);

		RangeVar   *name	= rangevars ? rangevars[i] : NULL;

		char	   *tablespace = tablespaces ? tablespaces[i] : NULL;

		(void) create_single_range_partition_internal(parent_relid,
													  &start,
													  &end,
													  bounds_type,
													  name,
													  tablespace);
	}

	/* Return number of partitions */
	PG_RETURN_INT32(ndatums - 1);
}

/* Checks if range overlaps with existing partitions. */
Datum
check_range_available_pl(PG_FUNCTION_ARGS)
{
	Oid			parent_relid;
	Bound		start,
				end;
	Oid			value_type	= get_fn_expr_argtype(fcinfo->flinfo, 1);

	if (PG_ARGISNULL(0))
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("'parent_relid' should not be NULL")));

	parent_relid = PG_GETARG_OID(0);

	start = PG_ARGISNULL(1) ?
				MakeBoundInf(MINUS_INFINITY) :
				MakeBound(PG_GETARG_DATUM(1));

	end = PG_ARGISNULL(2) ?
				MakeBoundInf(PLUS_INFINITY) :
				MakeBound(PG_GETARG_DATUM(2));

	/* Raise ERROR if range overlaps with any partition */
	check_range_available(parent_relid,
						  &start,
						  &end,
						  value_type,
						  true);

	PG_RETURN_VOID();
}

/* Generate range bounds starting with 'value' using 'interval'. */
Datum
generate_range_bounds_pl(PG_FUNCTION_ARGS)
{
	/* Input params */
	Datum		value			= PG_GETARG_DATUM(0);
	Oid			value_type		= get_fn_expr_argtype(fcinfo->flinfo, 0);
	Datum		interval		= PG_GETARG_DATUM(1);
	Oid			interval_type	= get_fn_expr_argtype(fcinfo->flinfo, 1);
	int			count			= PG_GETARG_INT32(2);
	int			i;

	/* Operator */
	Oid			plus_op_func;
	Datum		plus_op_result;
	Oid			plus_op_result_type;

	/* Array */
	ArrayType  *array;
	int16		elemlen;
	bool		elembyval;
	char		elemalign;
	Datum	   *datums;

	Assert(!PG_ARGISNULL(0));
	Assert(!PG_ARGISNULL(1));
	Assert(!PG_ARGISNULL(2));

	if (count < 1)
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("'p_count' must be greater than zero")));

	/* We must provide count+1 bounds */
	count += 1;

	/* Find suitable addition operator for given value and interval */
	extract_op_func_and_ret_type("+", value_type, interval_type,
								 &plus_op_func,
								 &plus_op_result_type);

	/* Fetch type's information for array */
	get_typlenbyvalalign(value_type, &elemlen, &elembyval, &elemalign);

	datums = palloc(sizeof(Datum) * count);
	datums[0] = value;

	/* Calculate bounds */
	for (i = 1; i < count; i++)
	{
		/* Invoke addition operator and get a result */
		plus_op_result = OidFunctionCall2(plus_op_func, value, interval);

		/* Cast result to 'value_type' if needed */
		if (plus_op_result_type != value_type)
			plus_op_result = perform_type_cast(plus_op_result,
											   plus_op_result_type,
											   value_type, NULL);

		/* Update 'value' and store current bound */
		value = datums[i] = plus_op_result;
	}

	/* build an array based on calculated datums */
	array = construct_array(datums, count, value_type,
							elemlen, elembyval, elemalign);

	pfree(datums);

	PG_RETURN_ARRAYTYPE_P(array);
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
	Oid						partition_relid,
							parent_relid;
	PartParentSearch		parent_search;
	RangeEntry			   *ranges;
	const PartRelationInfo *prel;
	uint32					i;

	if (!PG_ARGISNULL(0))
	{
		partition_relid = PG_GETARG_OID(0);
	}
	else ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("'partition_relid' should not be NULL")));

	parent_relid = get_parent_of_partition(partition_relid, &parent_search);
	if (parent_search != PPS_ENTRY_PART_PARENT)
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("relation \"%s\" is not a partition",
							   get_rel_name_or_relid(partition_relid))));

	prel = get_pathman_relation_info(parent_relid);
	shout_if_prel_is_invalid(parent_relid, prel, PT_RANGE);

	/* Check type of 'dummy' (for correct output) */
	if (getBaseType(get_fn_expr_argtype(fcinfo->flinfo, 1)) != getBaseType(prel->ev_type))
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("pg_typeof(dummy) should be %s",
							   format_type_be(getBaseType(prel->ev_type)))));

	ranges = PrelGetRangesArray(prel);

	/* Look for the specified partition */
	for (i = 0; i < PrelChildrenCount(prel); i++)
		if (ranges[i].child_oid == partition_relid)
		{
			ArrayType  *arr;
			Bound		elems[2];

			elems[0] = ranges[i].min;
			elems[1] = ranges[i].max;

			arr = construct_infinitable_array(elems, 2,
											  prel->ev_type, prel->ev_len,
											  prel->ev_byval, prel->ev_align);

			PG_RETURN_ARRAYTYPE_P(arr);
		}

	/* No partition found, report error */
	ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					errmsg("relation \"%s\" has no partition \"%s\"",
						   get_rel_name_or_relid(parent_relid),
						   get_rel_name_or_relid(partition_relid))));

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
	Oid						parent_relid;
	int						partition_idx = 0;
	Bound					elems[2];
	RangeEntry			   *ranges;
	const PartRelationInfo *prel;

	if (!PG_ARGISNULL(0))
	{
		parent_relid = PG_GETARG_OID(0);
	}
	else ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("'parent_relid' should not be NULL")));

	if (!PG_ARGISNULL(1))
	{
		partition_idx = PG_GETARG_INT32(1);
	}
	else ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("'partition_idx' should not be NULL")));

	prel = get_pathman_relation_info(parent_relid);
	shout_if_prel_is_invalid(parent_relid, prel, PT_RANGE);

	/* Check type of 'dummy' (for correct output) */
	if (getBaseType(get_fn_expr_argtype(fcinfo->flinfo, 2)) != getBaseType(prel->ev_type))
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("pg_typeof(dummy) should be %s",
							   format_type_be(getBaseType(prel->ev_type)))));


	/* Now we have to deal with 'idx' */
	if (partition_idx < -1)
	{
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("negative indices other than -1"
							   " (last partition) are not allowed")));
	}
	else if (partition_idx == -1)
	{
		partition_idx = PrelLastChild(prel);
	}
	else if (((uint32) abs(partition_idx)) >= PrelChildrenCount(prel))
	{
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("partition #%d does not exist (total amount is %u)",
							   partition_idx, PrelChildrenCount(prel))));
	}

	ranges = PrelGetRangesArray(prel);

	/* Build args for construct_infinitable_array() */
	elems[0] = ranges[partition_idx].min;
	elems[1] = ranges[partition_idx].max;

	PG_RETURN_ARRAYTYPE_P(construct_infinitable_array(elems, 2,
													  prel->ev_type,
													  prel->ev_len,
													  prel->ev_byval,
													  prel->ev_align));
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
	Oid			partition_relid;
	char	   *expression;
	Node	   *expr;

	Bound		min,
				max;
	Oid			bounds_type = get_fn_expr_argtype(fcinfo->flinfo, 2);
	Constraint *con;
	char	   *result;

	if (!PG_ARGISNULL(0))
	{
		partition_relid = PG_GETARG_OID(0);
	}
	else ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("'partition_relid' should not be NULL")));

	if (!PG_ARGISNULL(1))
	{
		expression = TextDatumGetCString(PG_GETARG_TEXT_P(1));
	}
	else ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("'expression' should not be NULL")));;

	min = PG_ARGISNULL(2) ?
				MakeBoundInf(MINUS_INFINITY) :
				MakeBound(PG_GETARG_DATUM(2));

	max = PG_ARGISNULL(3) ?
				MakeBoundInf(PLUS_INFINITY) :
				MakeBound(PG_GETARG_DATUM(3));

	expr = parse_partitioning_expression(partition_relid, expression, NULL, NULL);
	con = build_range_check_constraint(partition_relid,
									   expr,
									   &min, &max,
									   bounds_type);

	result = deparse_constraint(partition_relid, con->raw_expr);

	PG_RETURN_TEXT_P(cstring_to_text(result));
}

/* Build name for sequence for auto partition naming */
Datum
build_sequence_name(PG_FUNCTION_ARGS)
{
	Oid		parent_relid = PG_GETARG_OID(0);
	Oid		parent_nsp;
	char   *result;

	if (!SearchSysCacheExists1(RELOID, ObjectIdGetDatum(parent_relid)))
		ereport(ERROR, (errmsg("relation \"%u\" does not exist", parent_relid)));

	parent_nsp = get_rel_namespace(parent_relid);

	result = psprintf("%s.%s",
					  quote_identifier(get_namespace_name(parent_nsp)),
					  quote_identifier(build_sequence_name_internal(parent_relid)));

	PG_RETURN_TEXT_P(cstring_to_text(result));
}


/*
 * Merge multiple partitions.
 * All data will be copied to the first one.
 * The rest of partitions will be dropped.
 */
Datum
merge_range_partitions(PG_FUNCTION_ARGS)
{
	Oid					parent = InvalidOid;
	PartParentSearch	parent_search;
	ArrayType		   *arr = PG_GETARG_ARRAYTYPE_P(0);

	Oid				   *partitions;
	Datum			   *datums;
	bool			   *nulls;
	int					nparts;
	int16				typlen;
	bool				typbyval;
	char				typalign;
	int					i;

	/* Validate array type */
	Assert(ARR_ELEMTYPE(arr) == REGCLASSOID);

	/* Extract Oids */
	get_typlenbyvalalign(REGCLASSOID, &typlen, &typbyval, &typalign);
	deconstruct_array(arr, REGCLASSOID,
					  typlen, typbyval, typalign,
					  &datums, &nulls, &nparts);

	/* Extract partition Oids from array */
	partitions = palloc(sizeof(Oid) * nparts);
	for (i = 0; i < nparts; i++)
		partitions[i] = DatumGetObjectId(datums[i]);

	if (nparts < 2)
		ereport(ERROR, (errmsg("cannot merge partitions"),
						errdetail("there must be at least two partitions")));

	/* Check if all partitions are from the same parent */
	for (i = 0; i < nparts; i++)
	{
		Oid cur_parent = get_parent_of_partition(partitions[i], &parent_search);

		/* If we couldn't find a parent, it's not a partition */
		if (parent_search != PPS_ENTRY_PART_PARENT)
			ereport(ERROR, (errmsg("cannot merge partitions"),
							errdetail("relation \"%s\" is not a partition",
									  get_rel_name_or_relid(partitions[i]))));

		/* 'parent' is not initialized */
		if (parent == InvalidOid)
			parent = cur_parent; /* save parent */

		/* Oops, parent mismatch! */
		if (cur_parent != parent)
			ereport(ERROR, (errmsg("cannot merge partitions"),
							errdetail("all relations must share the same parent")));
	}

	/* Now merge partitions */
	merge_range_partitions_internal(parent, partitions, nparts);

	PG_RETURN_VOID();
}

static void
merge_range_partitions_internal(Oid parent_relid, Oid *parts, uint32 nparts)
{
	Oid					    partition_relid;
	const PartRelationInfo *prel;
	List				   *rentry_list = NIL;
	RangeEntry			   *ranges,
						   *first,
						   *last;
	FmgrInfo				cmp_proc;
	int						i;
	Bound					min,
							max;

	prel = get_pathman_relation_info(parent_relid);
	shout_if_prel_is_invalid(parent_relid, prel, PT_RANGE);

	/* Fetch ranges array */
	ranges = PrelGetRangesArray(prel);

	/* Lock parent till transaction's end */
	LockRelationOid(parent_relid, ShareUpdateExclusiveLock);

	/* Process partitions */
	for (i = 0; i < nparts; i++)
	{
		int j;

		LockRelationOid(parts[i], AccessShareLock);

		/* Look for the specified partition */
		for (j = 0; j < PrelChildrenCount(prel); j++)
			if (ranges[j].child_oid == parts[i])
			{
				rentry_list = lappend(rentry_list, &ranges[j]);
				break;
			}
	}

	/* Check that partitions are adjacent */
	check_range_adjacence(prel->cmp_proc, prel->ev_collid, rentry_list);

	/* First determine the bounds of a new constraint */
	first = (RangeEntry *) linitial(rentry_list);
	last = (RangeEntry *) llast(rentry_list);

	/* Swap ranges if 'last' < 'first' */
	fmgr_info(prel->cmp_proc, &cmp_proc);
	if (cmp_bounds(&cmp_proc, prel->ev_collid, &last->min, &first->min) < 0)
	{
		RangeEntry *tmp = last;

		last = first;
		first = tmp;
	}

	/* We need to save bounds, they will be removed at invalidation */
	min = first->min;
	max = last->max;

	/* Create a new RANGE partition and return its Oid */
	partition_relid = create_single_range_partition_internal(parent_relid,
															 &min,
															 &max,
															 prel->ev_type,
															 NULL,
															 NULL);
	/* Make new relation visible */
	CommandCounterIncrement();

	if (SPI_connect() != SPI_OK_CONNECT)
		elog(ERROR, "could not connect using SPI");

	/* Copy the data from all partition to the first one */
	for (i = 1; i < nparts; i++)
	{
		/* Prevent modification on relation */
		LockRelationOid(parts[i], ShareLock);

		char *query = psprintf("INSERT INTO %s SELECT * FROM %s",
							   get_qualified_rel_name(partition_relid),
							   get_qualified_rel_name(parts[i]));

		SPI_exec(query, 0);
		pfree(query);
	}

	SPI_finish();

	/* Drop partitions */
	for (i = 0; i < nparts; i++)
	{
		LockRelationOid(parts[i], AccessExclusiveLock);
		drop_table_by_oid(parts[i]);
	}
}


/*
 * Drops partition and expands the next partition
 * so that it could cover the dropped one
 *
 * This function was written in order to support Oracle-like ALTER TABLE ...
 * DROP PARTITION. In Oracle partitions only have upper bound and when
 * partition is dropped the next one automatically covers freed range
 */
Datum
drop_range_partition_expand_next(PG_FUNCTION_ARGS)
{
	const PartRelationInfo *prel;
	PartParentSearch		parent_search;
	Oid						relid = PG_GETARG_OID(0),
							parent;
	RangeEntry			   *ranges;
	int						i;

	/* Get parent's relid */
	parent = get_parent_of_partition(relid, &parent_search);
	if (parent_search != PPS_ENTRY_PART_PARENT)
		elog(ERROR, "relation \"%s\" is not a partition",
			 get_rel_name_or_relid(relid));

	/* Fetch PartRelationInfo and perform some checks */
	prel = get_pathman_relation_info(parent);
	shout_if_prel_is_invalid(parent, prel, PT_RANGE);

	/* Fetch ranges array */
	ranges = PrelGetRangesArray(prel);

	/* Looking for partition in child relations */
	for (i = 0; i < PrelChildrenCount(prel); i++)
		if (ranges[i].child_oid == relid)
			break;

	/*
	 * It must be in ranges array because we already
	 * know that this table is a partition
	 */
	Assert(i < PrelChildrenCount(prel));

	/* Expand next partition if it exists */
	if (i < PrelChildrenCount(prel) - 1)
	{
		RangeEntry	   *cur = &ranges[i],
					   *next = &ranges[i + 1];

		/* Drop old constraint and create a new one */
		modify_range_constraint(next->child_oid,
								prel->expr_cstr,
								prel->ev_type,
								&cur->min,
								&next->max);
	}

	/* Finally drop this partition */
	drop_table_by_oid(relid);

	PG_RETURN_VOID();
}

/*
 * Takes text representation of interval value and checks
 * if it corresponds to partitioning expression.
 * NOTE: throws an ERROR if it fails to convert text to Datum.
 */
Datum
validate_interval_value(PG_FUNCTION_ARGS)
{
#define ARG_PARTREL			0
#define ARG_EXPRESSION		1
#define ARG_PARTTYPE		2
#define ARG_RANGE_INTERVAL	3
#define ARG_EXPRESSION_P	4

	Oid			partrel;
	PartType	parttype;
	char	   *expr_cstr;
	Oid			expr_type;

	if (PG_ARGISNULL(ARG_PARTREL))
	{
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("'partrel' should not be NULL")));
	}
	else partrel = PG_GETARG_OID(ARG_PARTREL);

	/* Check that relation exists */
	if (!SearchSysCacheExists1(RELOID, ObjectIdGetDatum(partrel)))
		elog(ERROR, "relation \"%u\" does not exist", partrel);

	if (PG_ARGISNULL(ARG_EXPRESSION))
	{
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("'expression' should not be NULL")));
	}
	else expr_cstr = TextDatumGetCString(PG_GETARG_TEXT_P(ARG_EXPRESSION));

	if (PG_ARGISNULL(ARG_PARTTYPE))
	{
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("'parttype' should not be NULL")));
	}
	else parttype = DatumGetPartType(PG_GETARG_DATUM(ARG_PARTTYPE));

	/*
	 * Fetch partitioning expression's type using
	 * either user's expression or parsed expression.
	 */
	if (PG_ARGISNULL(ARG_EXPRESSION_P))
	{
		Datum expr_datum;

		/* We'll have to parse expression with our own hands */
		expr_datum = cook_partitioning_expression(partrel, expr_cstr, &expr_type);

		/* Free both expressions */
		pfree(DatumGetPointer(expr_datum));
		pfree(expr_cstr);
	}
	else
	{
		char *expr_p_cstr;

		/* Good, let's use a cached parsed expression */
		expr_p_cstr = TextDatumGetCString(PG_GETARG_TEXT_P(ARG_EXPRESSION_P));
		expr_type = exprType(stringToNode(expr_p_cstr));

		/* Free both expressions */
		pfree(expr_p_cstr);
		pfree(expr_cstr);
	}

	/*
	 * NULL interval is fine for both HASH and RANGE.
	 * But for RANGE we need to make some additional checks.
	 */
	if (!PG_ARGISNULL(ARG_RANGE_INTERVAL))
	{
		Datum		interval_text = PG_GETARG_DATUM(ARG_RANGE_INTERVAL),
					interval_value;
		Oid			interval_type;

		if (parttype == PT_HASH)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("interval should be NULL for HASH partitioned table")));

		/* Try converting textual representation */
		interval_value = extract_binary_interval_from_text(interval_text,
														   expr_type,
														   &interval_type);

		/* Check that interval isn't trivial */
		if (interval_is_trivial(expr_type, interval_value, interval_type))
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("interval should not be trivial")));
	}

	PG_RETURN_BOOL(true);
}


/*
 * ------------------
 *  Helper functions
 * ------------------
 */

/*
 * Check if interval is insignificant to avoid infinite loops while adding
 * new partitions
 *
 * The main idea behind this function is to add specified interval to some
 * default value (zero for numeric types and current date/timestamp for datetime
 * types) and look if it is changed. If it is then return true.
 */
static bool
interval_is_trivial(Oid atttype, Datum interval, Oid interval_type)
{
	Oid			plus_op_func;
	Datum		plus_op_result;
	Oid			plus_op_result_type;

	Datum		default_value;

	FmgrInfo	cmp_func;
	int32		cmp_result;

	/*
	 * Generate default value.
	 *
	 * For float4 and float8 values we also check that they aren't NaN or INF.
	 */
	switch(atttype)
	{
		case INT2OID:
			default_value = Int16GetDatum(0);
			break;

		case INT4OID:
			default_value = Int32GetDatum(0);
			break;

		/* Take care of 32-bit platforms */
		case INT8OID:
			default_value = Int64GetDatum(0);
			break;

		case FLOAT4OID:
			{
				float4 f = DatumGetFloat4(interval);

				if (isnan(f) || is_infinite(f))
					ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
									errmsg("invalid floating point interval")));
				default_value = Float4GetDatum(0);
			}
			break;

		case FLOAT8OID:
			{
				float8 f = DatumGetFloat8(interval);

				if (isnan(f) || is_infinite(f))
					ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
									errmsg("invalid floating point interval")));
				default_value = Float8GetDatum(0);
			}
			break;

		case NUMERICOID:
			{
				Numeric		ni = DatumGetNumeric(interval),
							numeric;

				/* Test for NaN */
				if (numeric_is_nan(ni))
					ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
									errmsg("invalid numeric interval")));

				/* Building default value */
				numeric = DatumGetNumeric(
								DirectFunctionCall3(numeric_in,
													CStringGetDatum("0"),
													ObjectIdGetDatum(InvalidOid),
													Int32GetDatum(-1)));
				default_value = NumericGetDatum(numeric);
			}
			break;

		case TIMESTAMPOID:
		case TIMESTAMPTZOID:
			default_value = TimestampGetDatum(GetCurrentTimestamp());
			break;

		case DATEOID:
			{
				Datum ts = TimestampGetDatum(GetCurrentTimestamp());

				default_value = perform_type_cast(ts, TIMESTAMPTZOID, DATEOID, NULL);
			}
			break;

		default:
			return false;
	}

	/* Find suitable addition operator for default value and interval */
	extract_op_func_and_ret_type("+", atttype, interval_type,
								 &plus_op_func,
								 &plus_op_result_type);

	/* Invoke addition operator and get a result */
	plus_op_result = OidFunctionCall2(plus_op_func, default_value, interval);

	/*
	 * If operator result type isn't the same as original value then
	 * convert it. We need this to make sure that specified interval would
	 * change the _original_ value somehow. For example, if we add one second
	 * to a date then we'll get a timestamp which is one second later than
	 * original date (obviously). But when we convert it back to a date we will
	 * get the same original value meaning that one second interval wouldn't
	 * change original value anyhow. We should consider such interval as trivial
	 */
	if (plus_op_result_type != atttype)
	{
		plus_op_result = perform_type_cast(plus_op_result,
										   plus_op_result_type,
										   atttype, NULL);
		plus_op_result_type = atttype;
	}

	/*
	 * Compare it to the default_value.
	 *
	 * If they are the same then obviously interval is trivial.
	 */
	fill_type_cmp_fmgr_info(&cmp_func,
							getBaseType(atttype),
							getBaseType(plus_op_result_type));

	cmp_result = DatumGetInt32(FunctionCall2(&cmp_func,
											 default_value,
											 plus_op_result));
	if (cmp_result == 0)
		return true;

	else if (cmp_result > 0) /* Negative interval? */
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("interval should not be negative")));

	/* Everything is OK */
	return false;
}

/*
 * Drop old partition constraint and create
 * a new one with specified boundaries
 */
static void
modify_range_constraint(Oid partition_relid,
						const char *expression,
						Oid expression_type,
						const Bound *lower,
						const Bound *upper)
{
	Node		   *expr;
	Constraint	   *constraint;

	/* Drop old constraint */
	drop_pathman_check_constraint(partition_relid);

	/* Parse expression */
	expr = parse_partitioning_expression(partition_relid, expression, NULL, NULL);

	/* Build a new one */
	constraint = build_range_check_constraint(partition_relid,
											  expr,
											  lower,
											  upper,
											  expression_type);

	/* Add new constraint */
	add_pathman_check_constraint(partition_relid, constraint);
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

/*
 * Build an 1d array of Bound elements
 *
 *		The main difference from construct_array() is that
 *		it will substitute infinite values with NULLs
 */
static ArrayType *
construct_infinitable_array(Bound *elems,
							int nelems,
							Oid elemtype,
							int elemlen,
							bool elembyval,
							char elemalign)
{
	ArrayType  *arr;
	Datum	   *datums;
	bool	   *nulls;
	int			dims[1] = { nelems };
	int			lbs[1] = { 1 };
	int 		i;

	datums = palloc(sizeof(Datum) * nelems);
	nulls = palloc(sizeof(bool) * nelems);

	for (i = 0; i < nelems; i++)
	{
		datums[i] = IsInfinite(&elems[i]) ?
						(Datum) 0 :
						BoundGetValue(&elems[i]);
		nulls[i] = IsInfinite(&elems[i]);
	}

	arr = construct_md_array(datums, nulls, 1,
							 dims, lbs,
							 elemtype, elemlen,
							 elembyval, elemalign);

	return arr;
}

/*
 * Check that range entries are adjacent
 */
static void
check_range_adjacence(Oid cmp_proc, Oid collid, List *ranges)
{
	ListCell   *lc;
	RangeEntry *last = NULL;
	FmgrInfo	finfo;

	fmgr_info(cmp_proc, &finfo);

	foreach(lc, ranges)
	{
		RangeEntry *cur = (RangeEntry *) lfirst(lc);

		/* Skip first iteration */
		if (!last)
		{
			last = cur;
			continue;
		}

		/* Check that last and current partitions are adjacent */
		if ((cmp_bounds(&finfo, collid, &last->max, &cur->min) != 0) &&
			(cmp_bounds(&finfo, collid, &cur->max, &last->min) != 0))
		{
			ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
							errmsg("partitions \"%s\" and \"%s\" are not adjacent",
								   get_rel_name(last->child_oid),
								   get_rel_name(cur->child_oid))));
		}

		last = cur;
	}
}

/*
 * Return palloced fully qualified relation name as a cstring
 */
static char *
get_qualified_rel_name(Oid relid)
{
	Oid nspid = get_rel_namespace(relid);

	return psprintf("%s.%s",
					quote_identifier(get_namespace_name(nspid)),
					quote_identifier(get_rel_name(relid)));
}

/*
 * Drop table using it's Oid
 */
static void
drop_table_by_oid(Oid relid)
{
	DropStmt	   *n = makeNode(DropStmt);
	const char	   *relname = get_qualified_rel_name(relid);

	n->removeType	= OBJECT_TABLE;
	n->missing_ok	= false;
	n->objects		= list_make1(stringToQualifiedNameList(relname));
#if PG_VERSION_NUM < 100000
	n->arguments	= NIL;
#endif
	n->behavior		= DROP_RESTRICT;  /* default behavior */
	n->concurrent	= false;

	RemoveRelations(n);
}
