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
#include "parser/parse_relation.h"
#include "parser/parse_expr.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/ruleutils.h"


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
static void modify_range_constraint(Oid child_relid,
									const char *attname,
									AttrNumber attnum,
									Oid atttype,
									const Bound *lower,
									const Bound *upper);
static char *get_qualified_rel_name(Oid relid);
static void drop_table_by_oid(Oid relid);


/* Function declarations */

PG_FUNCTION_INFO_V1( create_single_range_partition_pl );
PG_FUNCTION_INFO_V1( find_or_create_range_partition);
PG_FUNCTION_INFO_V1( check_range_available_pl );

PG_FUNCTION_INFO_V1( get_part_range_by_oid );
PG_FUNCTION_INFO_V1( get_part_range_by_idx );

PG_FUNCTION_INFO_V1( build_range_condition );
PG_FUNCTION_INFO_V1( build_sequence_name );
PG_FUNCTION_INFO_V1( merge_range_partitions );
PG_FUNCTION_INFO_V1( drop_range_partition_expand_next );
PG_FUNCTION_INFO_V1( validate_interval_value );


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
	parent_relid = PG_GETARG_OID(0);
	value_type = get_fn_expr_argtype(fcinfo->flinfo, 1);

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
	Bound		start,
				end;
	Oid			value_type	= get_fn_expr_argtype(fcinfo->flinfo, 1);

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
			Bound		elems[2] = { ranges[i].min, ranges[i].max };

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
	Bound					elems[2];
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

	PG_RETURN_ARRAYTYPE_P(construct_infinitable_array(elems, 2,
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
	Oid			relid = PG_GETARG_OID(0);
	text	   *attname = PG_GETARG_TEXT_P(1);

	Bound		min,
				max;
	Oid			bounds_type = get_fn_expr_argtype(fcinfo->flinfo, 2);
	Constraint *con;
	char	   *result;

	min = PG_ARGISNULL(2) ?
				MakeBoundInf(MINUS_INFINITY) :
				MakeBound(PG_GETARG_DATUM(2));

	max = PG_ARGISNULL(3) ?
				MakeBoundInf(PLUS_INFINITY) :
				MakeBound(PG_GETARG_DATUM(3));

	con = build_range_check_constraint(relid, text_to_cstring(attname),
									   &min, &max,
									   bounds_type);

	result = deparse_constraint(relid, con->raw_expr);

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
merge_range_partitions_internal(Oid parent, Oid *parts, uint32 nparts)
{
	const PartRelationInfo *prel;
	List				   *rentry_list = NIL;
	RangeEntry			   *ranges,
						   *first,
						   *last;
	FmgrInfo				cmp_proc;
	int						i;

	prel = get_pathman_relation_info(parent);
	shout_if_prel_is_invalid(parent, prel, PT_RANGE);

	/* Fetch ranges array */
	ranges = PrelGetRangesArray(prel);

	/* Lock parent till transaction's end */
	xact_lock_partitioned_rel(parent, false);

	/* Process partitions */
	for (i = 0; i < nparts; i++)
	{
		int j;

		/* Lock partition in ACCESS EXCLUSIVE mode */
		prevent_relation_modification_internal(parts[0]);

		/* Look for the specified partition */
		for (j = 0; j < PrelChildrenCount(prel); j++)
			if (ranges[j].child_oid == parts[i])
			{
				rentry_list = lappend(rentry_list, &ranges[j]);
				break;
			}
	}

	/* Check that partitions are adjacent */
	check_range_adjacence(prel->cmp_proc, prel->attcollid, rentry_list);

	/* First determine the bounds of a new constraint */
	first = (RangeEntry *) linitial(rentry_list);
	last = (RangeEntry *) llast(rentry_list);

	/* Swap ranges if 'last' < 'first' */
	fmgr_info(prel->cmp_proc, &cmp_proc);
	if (cmp_bounds(&cmp_proc, prel->attcollid, &last->min, &first->min) < 0)
	{
		RangeEntry *tmp = last;

		last = first;
		first = tmp;
	}

	/* Drop old constraint and create a new one */
	modify_range_constraint(parts[0],
							get_relid_attribute_name(prel->key,
													 prel->attnum),
							prel->attnum,
							prel->atttype,
							&first->min,
							&last->max);

	/* Make constraint visible */
	CommandCounterIncrement();

	if (SPI_connect() != SPI_OK_CONNECT)
		elog(ERROR, "could not connect using SPI");

	/* Migrate the data from all partition to the first one */
	for (i = 1; i < nparts; i++)
	{
		char *query = psprintf("WITH part_data AS ( "
									"DELETE FROM %s RETURNING "
							   "*) "
							   "INSERT INTO %s SELECT * FROM part_data",
							   get_qualified_rel_name(parts[i]),
							   get_qualified_rel_name(parts[0]));

		SPI_exec(query, 0);
		pfree(query);
	}

	SPI_finish();

	/* Drop obsolete partitions */
	for (i = 1; i < nparts; i++)
		drop_table_by_oid(parts[i]);
}


/*
 * Drops partition and expands the next partition so that it cover dropped
 * one
 *
 * This function was written in order to support Oracle-like ALTER TABLE ...
 * DROP PARTITION. In Oracle partitions only have upper bound and when
 * partition is dropped the next one automatically covers freed range
 */
Datum
drop_range_partition_expand_next(PG_FUNCTION_ARGS)
{
	PartParentSearch		parent_search;
	const PartRelationInfo *prel;
	RangeEntry	   *ranges;
	Oid				relid = PG_GETARG_OID(0),
					parent;
	int				i;

	/* Get parent relid */
	parent = get_parent_of_partition(relid, &parent_search);
	if (parent_search != PPS_ENTRY_PART_PARENT)
		elog(ERROR, "relation \"%s\" is not a partition",
			 get_rel_name_or_relid(relid));

	prel = get_pathman_relation_info(parent);
	shout_if_prel_is_invalid(parent, prel, PT_RANGE);

	ranges = PrelGetRangesArray(prel);

	/* Looking for partition in child relations */
	for (i = 0; i < prel->children_count; i++)
		if (ranges[i].child_oid == relid)
			break;

	/*
	 * It must be in ranges array because we already know that table
	 * is a partition
	 */
	Assert(i < prel->children_count);

	/* If there is next partition then expand it */
	if (i < prel->children_count - 1)
	{
		RangeEntry	   *cur = &ranges[i],
					   *next = &ranges[i + 1];

		modify_range_constraint(next->child_oid,
						get_relid_attribute_name(prel->key, prel->attnum),
						prel->attnum,
						prel->atttype,
						&cur->min,
						&next->max);
	}

	drop_table_by_oid(relid);

	PG_RETURN_VOID();
}

/*
 * Takes text representation of interval value and checks if it is corresponds
 * to partitioning key. The function throws an error if it fails to convert
 * text to Datum
 */
Datum
validate_interval_value(PG_FUNCTION_ARGS)
{
	Oid			partrel = PG_GETARG_OID(0);
	text	   *attname = PG_GETARG_TEXT_P(1);
	PartType	parttype = DatumGetPartType(PG_GETARG_DATUM(2));
	Datum		range_interval = PG_GETARG_DATUM(3);

	char	   *attname_cstr;
	Oid			atttype; /* type of partitioned attribute */

	if (PG_ARGISNULL(0))
		elog(ERROR, "'partrel' should not be NULL");

	if (PG_ARGISNULL(1))
		elog(ERROR, "'attname' should not be NULL");

	if (PG_ARGISNULL(2))
		elog(ERROR, "'parttype' should not be NULL");

	/* it's OK if interval is NULL and table is HASH-partitioned */
	if (PG_ARGISNULL(3))
		PG_RETURN_BOOL(parttype == PT_HASH);

	/* Convert attname to CSTRING and fetch column's type */
	attname_cstr = text_to_cstring(attname);
	atttype = get_attribute_type(partrel, attname_cstr, false);

	/* Try converting textual representation */
	extract_binary_interval_from_text(range_interval, atttype, NULL);

	PG_RETURN_BOOL(true);
}


/*
 * ------------------
 *  Helper functions
 * ------------------
 *
 */

/*
 * Drop old partition constraint and create
 * a new one with specified boundaries
 */
static void
modify_range_constraint(Oid child_relid,
						const char *attname,
						AttrNumber attnum,
						Oid atttype,
						const Bound *lower,
						const Bound *upper)
{
	Constraint	   *constraint;
	Relation		partition_rel;
	char		   *attname_nonconst = pstrdup(attname);

	/* Drop old constraint */
	drop_check_constraint(child_relid, attnum);

	/* Build a new one */
	constraint = build_range_check_constraint(child_relid,
											  attname_nonconst,
											  lower,
											  upper,
											  atttype);

	/* Open the relation and add new check constraint */
	partition_rel = heap_open(child_relid, AccessExclusiveLock);
	AddRelationNewConstraints(partition_rel, NIL,
							  list_make1(constraint),
							  false, true, true);
	heap_close(partition_rel, NoLock);

	pfree(attname_nonconst);
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
			elog(ERROR, "partitions \"%s\" and \"%s\" are not adjacent",
						get_rel_name(last->child_oid),
						get_rel_name(cur->child_oid));
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
	Oid namespace = get_rel_namespace(relid);

	return psprintf("%s.%s",
					quote_identifier(get_namespace_name(namespace)),
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

	n->removeType = OBJECT_TABLE;
	n->missing_ok = false;
	n->objects = list_make1(stringToQualifiedNameList(relname));
	n->arguments = NIL;
	n->behavior = DROP_RESTRICT;  /* default behavior */
	n->concurrent = false;

	RemoveRelations(n);
}
