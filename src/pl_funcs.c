/* ------------------------------------------------------------------------
 *
 * pl_funcs.c
 *		Utility C functions for stored procedures
 *
 * Copyright (c) 2015-2016, Postgres Professional
 *
 * ------------------------------------------------------------------------
 */

#include "compat/pg_compat.h"

#include "init.h"
#include "pathman.h"
#include "partition_creation.h"
#include "partition_filter.h"
#include "relation_info.h"
#include "xact_handling.h"
#include "utils.h"

#include "access/htup_details.h"
#if PG_VERSION_NUM >= 120000
#include "access/heapam.h"
#include "access/relscan.h"
#include "access/table.h"
#include "access/tableam.h"
#endif
#include "access/xact.h"
#include "catalog/dependency.h"
#include "catalog/indexing.h"
#include "catalog/namespace.h"
#include "catalog/pg_type.h"
#include "commands/tablespace.h"
#include "commands/trigger.h"
#include "executor/executor.h"
#include "executor/spi.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "nodes/nodeFuncs.h"
#include "utils/builtins.h"
#include "utils/inval.h"
#include "utils/snapmgr.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"
#include "utils/typcache.h"

#if PG_VERSION_NUM < 110000
#include "catalog/pg_inherits_fn.h"
#endif


/* Function declarations */

PG_FUNCTION_INFO_V1( get_number_of_partitions_pl );
PG_FUNCTION_INFO_V1( get_partition_key_type_pl );
PG_FUNCTION_INFO_V1( get_partition_cooked_key_pl );
PG_FUNCTION_INFO_V1( get_cached_partition_cooked_key_pl );
PG_FUNCTION_INFO_V1( get_parent_of_partition_pl );
PG_FUNCTION_INFO_V1( get_base_type_pl );
PG_FUNCTION_INFO_V1( get_tablespace_pl );

PG_FUNCTION_INFO_V1( show_cache_stats_internal );
PG_FUNCTION_INFO_V1( show_partition_list_internal );

PG_FUNCTION_INFO_V1( build_check_constraint_name );

PG_FUNCTION_INFO_V1( validate_relname );
PG_FUNCTION_INFO_V1( validate_expression );
PG_FUNCTION_INFO_V1( is_date_type );
PG_FUNCTION_INFO_V1( is_operator_supported );
PG_FUNCTION_INFO_V1( is_tuple_convertible );

PG_FUNCTION_INFO_V1( add_to_pathman_config );
PG_FUNCTION_INFO_V1( pathman_config_params_trigger_func );

PG_FUNCTION_INFO_V1( prevent_part_modification );
PG_FUNCTION_INFO_V1( prevent_data_modification );

PG_FUNCTION_INFO_V1( validate_part_callback_pl );
PG_FUNCTION_INFO_V1( invoke_on_partition_created_callback );

PG_FUNCTION_INFO_V1( check_security_policy );

PG_FUNCTION_INFO_V1( debug_capture );
PG_FUNCTION_INFO_V1( pathman_version );

/* User context for function show_partition_list_internal() */
typedef struct
{
	Relation			pathman_config;
#if PG_VERSION_NUM >= 120000
	TableScanDesc		pathman_config_scan;
#else
	HeapScanDesc		pathman_config_scan;
#endif
	Snapshot			snapshot;

	PartRelationInfo   *current_prel;	/* selected PartRelationInfo */

	Size				child_number;	/* child we're looking at */
	SPITupleTable	   *tuptable;		/* buffer for tuples */
} show_partition_list_cxt;

/* User context for function show_pathman_cache_stats_internal() */
typedef struct
{
	MemoryContext		pathman_contexts[PATHMAN_MCXT_COUNT];
	HTAB			   *pathman_htables[PATHMAN_MCXT_COUNT];
	int					current_item;
} show_cache_stats_cxt;

/*
 * ------------------------
 *  Various useful getters
 * ------------------------
 */

/*
 * Return parent of a specified partition.
 */
Datum
get_parent_of_partition_pl(PG_FUNCTION_ARGS)
{
	Oid		partition = PG_GETARG_OID(0),
			parent = get_parent_of_partition(partition);

	if (!OidIsValid(parent))
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("\"%s\" is not a partition",
							   get_rel_name_or_relid(partition))));

	PG_RETURN_OID(parent);
}

/*
 * Return partition key type.
 */
Datum
get_partition_key_type_pl(PG_FUNCTION_ARGS)
{
	Oid					relid = PG_GETARG_OID(0);
	Oid					typid;
	PartRelationInfo   *prel;

	prel = get_pathman_relation_info(relid);
	shout_if_prel_is_invalid(relid, prel, PT_ANY);

	typid = prel->ev_type;

	close_pathman_relation_info(prel);

	PG_RETURN_OID(typid);
}

/*
 * Return cooked partition key.
 */
Datum
get_partition_cooked_key_pl(PG_FUNCTION_ARGS)
{
	/* Values extracted from PATHMAN_CONFIG */
	Datum		values[Natts_pathman_config];
	bool		isnull[Natts_pathman_config];

	Oid	relid = PG_GETARG_OID(0);
	char	   *expr_cstr;
	Node	   *expr;
	char	   *cooked_cstr;

	/* Check that table is registered in PATHMAN_CONFIG */
	if (!pathman_config_contains_relation(relid, values, isnull, NULL, NULL))
		elog(ERROR, "table \"%s\" is not partitioned",
			 get_rel_name_or_relid(relid));

	expr_cstr = TextDatumGetCString(values[Anum_pathman_config_expr - 1]);
	expr = cook_partitioning_expression(relid, expr_cstr, NULL);
	cooked_cstr = nodeToString(expr);

	pfree(expr_cstr);
	pfree(expr);

	PG_RETURN_TEXT_P(CStringGetTextDatum(cooked_cstr));
}

/*
 * Return cached cooked partition key.
 *
 * Used in tests for invalidation.
 */
Datum
get_cached_partition_cooked_key_pl(PG_FUNCTION_ARGS)
{
	Oid                 relid = PG_GETARG_OID(0);
	PartRelationInfo   *prel;
	Datum               res;

	prel = get_pathman_relation_info(relid);
	shout_if_prel_is_invalid(relid, prel, PT_ANY);
	res = CStringGetTextDatum(nodeToString(prel->expr));
	close_pathman_relation_info(prel);

	PG_RETURN_TEXT_P(res);
}

/*
 * Extract basic type of a domain.
 */
Datum
get_base_type_pl(PG_FUNCTION_ARGS)
{
	PG_RETURN_OID(getBaseType(PG_GETARG_OID(0)));
}

/*
 * Return tablespace name of a specified relation which must not be
 * natively partitioned.
 */
Datum
get_tablespace_pl(PG_FUNCTION_ARGS)
{
	Oid			relid = PG_GETARG_OID(0);
	Oid			tablespace_id;
	char	   *result;

	tablespace_id = get_rel_tablespace(relid);

	/* If tablespace id is InvalidOid then use the default tablespace */
	if (!OidIsValid(tablespace_id))
	{
		tablespace_id = GetDefaultTablespaceCompat(get_rel_persistence(relid), false);

		/* If tablespace is still invalid then use database's default */
		if (!OidIsValid(tablespace_id))
			tablespace_id = MyDatabaseTableSpace;
	}

	result = get_tablespace_name(tablespace_id);
	PG_RETURN_TEXT_P(cstring_to_text(result));
}

/*
 * ----------------------
 *  Common purpose VIEWs
 * ----------------------
 */

/*
 * List stats of all existing caches (memory contexts).
 */
Datum
show_cache_stats_internal(PG_FUNCTION_ARGS)
{
	show_cache_stats_cxt	   *usercxt;
	FuncCallContext			   *funccxt;

	/*
	 * Initialize tuple descriptor & function call context.
	 */
	if (SRF_IS_FIRSTCALL())
	{
		TupleDesc		tupdesc;
		MemoryContext	old_mcxt;

		funccxt = SRF_FIRSTCALL_INIT();

		if (!TopPathmanContext)
		{
			elog(ERROR, "pg_pathman's memory contexts are not initialized yet");
		}

		old_mcxt = MemoryContextSwitchTo(funccxt->multi_call_memory_ctx);

		usercxt = (show_cache_stats_cxt *) palloc(sizeof(show_cache_stats_cxt));

		usercxt->pathman_contexts[0] = TopPathmanContext;
		usercxt->pathman_contexts[1] = PathmanParentsCacheContext;
		usercxt->pathman_contexts[2] = PathmanStatusCacheContext;
		usercxt->pathman_contexts[3] = PathmanBoundsCacheContext;

		usercxt->pathman_htables[0] = NULL; /* no HTAB for this entry */
		usercxt->pathman_htables[1] = parents_cache;
		usercxt->pathman_htables[2] = status_cache;
		usercxt->pathman_htables[3] = bounds_cache;

		usercxt->current_item = 0;

		/* Create tuple descriptor */
		tupdesc = CreateTemplateTupleDescCompat(Natts_pathman_cache_stats, false);

		TupleDescInitEntry(tupdesc, Anum_pathman_cs_context,
						   "context", TEXTOID, -1, 0);
		TupleDescInitEntry(tupdesc, Anum_pathman_cs_size,
						   "size", INT8OID, -1, 0);
		TupleDescInitEntry(tupdesc, Anum_pathman_cs_used,
						   "used", INT8OID, -1, 0);
		TupleDescInitEntry(tupdesc, Anum_pathman_cs_entries,
						   "entries", INT8OID, -1, 0);

		funccxt->tuple_desc = BlessTupleDesc(tupdesc);
		funccxt->user_fctx = (void *) usercxt;

		MemoryContextSwitchTo(old_mcxt);
	}

	funccxt = SRF_PERCALL_SETUP();
	usercxt = (show_cache_stats_cxt *) funccxt->user_fctx;

	if (usercxt->current_item < lengthof(usercxt->pathman_contexts))
	{
		HTAB				   *current_htab;
		MemoryContext			current_mcxt;
		HeapTuple				htup;
		Datum					values[Natts_pathman_cache_stats];
		bool					isnull[Natts_pathman_cache_stats] = { 0 };

#if PG_VERSION_NUM >= 90600
		MemoryContextCounters	mcxt_stats;
#endif

		/* Select current memory context and hash table (cache) */
		current_mcxt = usercxt->pathman_contexts[usercxt->current_item];
		current_htab = usercxt->pathman_htables[usercxt->current_item];

		values[Anum_pathman_cs_context - 1]	=
				CStringGetTextDatum(simplify_mcxt_name(current_mcxt));

/* We can't check stats of mcxt prior to 9.6 */
#if PG_VERSION_NUM >= 90600

		/* Prepare context counters */
		memset(&mcxt_stats, 0, sizeof(mcxt_stats));

		/* NOTE: we do not consider child contexts if it's TopPathmanContext */
		McxtStatsInternal(current_mcxt, 0,
						  (current_mcxt != TopPathmanContext),
						  &mcxt_stats);

		values[Anum_pathman_cs_size - 1]	=
				Int64GetDatum(mcxt_stats.totalspace);

		values[Anum_pathman_cs_used - 1]	=
				Int64GetDatum(mcxt_stats.totalspace - mcxt_stats.freespace);

#else

		/* Set unsupported fields to NULL */
		isnull[Anum_pathman_cs_size - 1]	= true;
		isnull[Anum_pathman_cs_used - 1]	= true;
#endif

		values[Anum_pathman_cs_entries - 1]	=
				Int64GetDatum(current_htab ?
								  hash_get_num_entries(current_htab) :
								  0);

		/* Switch to next item */
		usercxt->current_item++;

		/* Form output tuple */
		htup = heap_form_tuple(funccxt->tuple_desc, values, isnull);

		SRF_RETURN_NEXT(funccxt, HeapTupleGetDatum(htup));
	}

	SRF_RETURN_DONE(funccxt);
}

/*
 * List all existing partitions and their parents.
 */
Datum
show_partition_list_internal(PG_FUNCTION_ARGS)
{
	show_partition_list_cxt	   *usercxt;
	FuncCallContext			   *funccxt;
	MemoryContext				old_mcxt;
	SPITupleTable			   *tuptable;

	/* Initialize tuple descriptor & function call context */
	if (SRF_IS_FIRSTCALL())
	{
		TupleDesc		 tupdesc;
		MemoryContext	 tuptab_mcxt;

		funccxt = SRF_FIRSTCALL_INIT();

		old_mcxt = MemoryContextSwitchTo(funccxt->multi_call_memory_ctx);

		usercxt = (show_partition_list_cxt *) palloc(sizeof(show_partition_list_cxt));

		/* Open PATHMAN_CONFIG with latest snapshot available */
		usercxt->pathman_config = heap_open(get_pathman_config_relid(false),
											AccessShareLock);
		usercxt->snapshot = RegisterSnapshot(GetLatestSnapshot());
#if PG_VERSION_NUM >= 120000
		usercxt->pathman_config_scan = table_beginscan(usercxt->pathman_config,
													  usercxt->snapshot, 0, NULL);
#else
		usercxt->pathman_config_scan = heap_beginscan(usercxt->pathman_config,
													  usercxt->snapshot, 0, NULL);
#endif

		usercxt->current_prel = NULL;

		/* Create tuple descriptor */
		tupdesc = CreateTemplateTupleDescCompat(Natts_pathman_partition_list, false);

		TupleDescInitEntry(tupdesc, Anum_pathman_pl_parent,
						   "parent", REGCLASSOID, -1, 0);
		TupleDescInitEntry(tupdesc, Anum_pathman_pl_partition,
						   "partition", REGCLASSOID, -1, 0);
		TupleDescInitEntry(tupdesc, Anum_pathman_pl_parttype,
						   "parttype", INT4OID, -1, 0);
		TupleDescInitEntry(tupdesc, Anum_pathman_pl_partattr,
						   "expr", TEXTOID, -1, 0);
		TupleDescInitEntry(tupdesc, Anum_pathman_pl_range_min,
						   "range_min", TEXTOID, -1, 0);
		TupleDescInitEntry(tupdesc, Anum_pathman_pl_range_max,
						   "range_max", TEXTOID, -1, 0);

		funccxt->tuple_desc = BlessTupleDesc(tupdesc);
		funccxt->user_fctx = (void *) usercxt;

		/* initialize tuple table context */
		tuptab_mcxt = AllocSetContextCreate(CurrentMemoryContext,
											"tuptable for pathman_partition_list",
											ALLOCSET_DEFAULT_SIZES);
		MemoryContextSwitchTo(tuptab_mcxt);

		/* Initialize tuple table for partitions list, we use it as buffer */
		tuptable = (SPITupleTable *) palloc0(sizeof(SPITupleTable));
		usercxt->tuptable = tuptable;
		tuptable->tuptabcxt = tuptab_mcxt;

		/* Set up initial allocations */
		tuptable->alloced = tuptable->free = PART_RELS_SIZE * CHILD_FACTOR;
		tuptable->vals = (HeapTuple *) palloc(tuptable->alloced * sizeof(HeapTuple));

		MemoryContextSwitchTo(old_mcxt);

		/* Iterate through pathman cache */
		for (;;)
		{
			HeapTuple			htup;
			Datum				values[Natts_pathman_partition_list];
			bool				isnull[Natts_pathman_partition_list] = { 0 };
			PartRelationInfo   *prel;

			/* Fetch next PartRelationInfo if needed */
			if (usercxt->current_prel == NULL)
			{
				HeapTuple	pathman_config_htup;
				Datum		parent_table;
				bool		parent_table_isnull;
				Oid			parent_table_oid;

				pathman_config_htup = heap_getnext(usercxt->pathman_config_scan,
												   ForwardScanDirection);
				if (!HeapTupleIsValid(pathman_config_htup))
					break;

				parent_table = heap_getattr(pathman_config_htup,
											Anum_pathman_config_partrel,
											RelationGetDescr(usercxt->pathman_config),
											&parent_table_isnull);

				Assert(parent_table_isnull == false);
				parent_table_oid = DatumGetObjectId(parent_table);

				usercxt->current_prel = get_pathman_relation_info(parent_table_oid);
				if (usercxt->current_prel == NULL)
					continue;

				usercxt->child_number = 0;
			}

			/* Alias to 'usercxt->current_prel' */
			prel = usercxt->current_prel;

			/* If we've run out of partitions, switch to the next 'prel' */
			if (usercxt->child_number >= PrelChildrenCount(prel))
			{
				/* Don't forget to close 'prel'! */
				close_pathman_relation_info(prel);

				usercxt->current_prel = NULL;
				usercxt->child_number = 0;

				continue;
			}

			/* Fill in common values */
			values[Anum_pathman_pl_parent - 1]		= PrelParentRelid(prel);
			values[Anum_pathman_pl_parttype - 1]	= prel->parttype;
			values[Anum_pathman_pl_partattr - 1]	= CStringGetTextDatum(prel->expr_cstr);

			switch (prel->parttype)
			{
				case PT_HASH:
					{
						Oid	 *children = PrelGetChildrenArray(prel),
							  child_oid = children[usercxt->child_number];

						values[Anum_pathman_pl_partition - 1] = child_oid;
						isnull[Anum_pathman_pl_range_min - 1] = true;
						isnull[Anum_pathman_pl_range_max - 1] = true;
					}
					break;

				case PT_RANGE:
					{
						RangeEntry *re;

						re = &PrelGetRangesArray(prel)[usercxt->child_number];

						values[Anum_pathman_pl_partition - 1] = re->child_oid;

						/* Lower bound text */
						if (!IsInfinite(&re->min))
						{
							Datum rmin = CStringGetTextDatum(
											BoundToCString(&re->min,
														   prel->ev_type));

							values[Anum_pathman_pl_range_min - 1] = rmin;
						}
						else isnull[Anum_pathman_pl_range_min - 1] = true;

						/* Upper bound text */
						if (!IsInfinite(&re->max))
						{
							Datum rmax = CStringGetTextDatum(
											BoundToCString(&re->max,
														   prel->ev_type));

							values[Anum_pathman_pl_range_max - 1] = rmax;
						}
						else isnull[Anum_pathman_pl_range_max - 1] = true;
					}
					break;

				default:
					WrongPartType(prel->parttype);
			}

			/* Fill tuptable */
			old_mcxt = MemoryContextSwitchTo(tuptable->tuptabcxt);

			/* Form output tuple */
			htup = heap_form_tuple(funccxt->tuple_desc, values, isnull);

			if (tuptable->free == 0)
			{
				/* Double the size of the pointer array */
				tuptable->free = tuptable->alloced;
				tuptable->alloced += tuptable->free;

				tuptable->vals = (HeapTuple *)
						repalloc_huge(tuptable->vals,
									  tuptable->alloced * sizeof(HeapTuple));
			}

			/* Add tuple to table and decrement 'free' */
			tuptable->vals[tuptable->alloced - tuptable->free] = htup;
			(tuptable->free)--;

			MemoryContextSwitchTo(old_mcxt);

			/* Switch to the next child */
			usercxt->child_number++;
		}

		/* Clean resources */
#if PG_VERSION_NUM >= 120000
		table_endscan(usercxt->pathman_config_scan);
#else
		heap_endscan(usercxt->pathman_config_scan);
#endif
		UnregisterSnapshot(usercxt->snapshot);
		heap_close(usercxt->pathman_config, AccessShareLock);

		usercxt->child_number = 0;
	}

	funccxt = SRF_PERCALL_SETUP();
	usercxt = (show_partition_list_cxt *) funccxt->user_fctx;
	tuptable = usercxt->tuptable;

	/* Iterate through used slots */
	if (usercxt->child_number < (tuptable->alloced - tuptable->free))
	{
		HeapTuple htup = usercxt->tuptable->vals[usercxt->child_number++];

		SRF_RETURN_NEXT(funccxt, HeapTupleGetDatum(htup));
	}

	SRF_RETURN_DONE(funccxt);
}


/*
 * --------
 *  Traits
 * --------
 */

/*
 * Check that relation exists.
 * NOTE: we pass REGCLASS as text, hence the function's name.
 */
Datum
validate_relname(PG_FUNCTION_ARGS)
{
	Oid relid;

	/* We don't accept NULL */
	if (PG_ARGISNULL(0))
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("relation should not be NULL"),
						errdetail("function " CppAsString(validate_relname)
								  " received NULL argument")));

	/* Fetch relation's Oid */
	relid = PG_GETARG_OID(0);

	if (!SearchSysCacheExists1(RELOID, ObjectIdGetDatum(relid)))
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("relation \"%u\" does not exist", relid),
						errdetail("triggered in function "
								  CppAsString(validate_relname))));

	PG_RETURN_VOID();
}

/*
 * Validate a partitioning expression.
 * NOTE: We need this in range functions because
 * we do many things before actual partitioning.
 */
Datum
validate_expression(PG_FUNCTION_ARGS)
{
	Oid			relid;
	char	   *expression;

	/* Fetch relation's Oid */
	if (!PG_ARGISNULL(0))
	{
		relid = PG_GETARG_OID(0);
	}
	else ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("'relid' should not be NULL")));

	/* Protect relation from concurrent drop */
	LockRelationOid(relid, AccessShareLock);

	if (!SearchSysCacheExists1(RELOID, ObjectIdGetDatum(relid)))
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("relation \"%u\" does not exist", relid),
						errdetail("triggered in function "
								  CppAsString(validate_expression))));

	if (!PG_ARGISNULL(1))
	{
		expression = TextDatumGetCString(PG_GETARG_TEXT_P(1));
	}
	else ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("'expression' should not be NULL")));

	/* Perform some checks */
	cook_partitioning_expression(relid, expression, NULL);

	UnlockRelationOid(relid, AccessShareLock);

	PG_RETURN_VOID();
}

Datum
is_date_type(PG_FUNCTION_ARGS)
{
	PG_RETURN_BOOL(is_date_type_internal(PG_GETARG_OID(0)));
}

/*
 * Bail out with ERROR if rel1 tuple can't be converted to rel2 tuple.
 */
Datum
is_tuple_convertible(PG_FUNCTION_ARGS)
{
	Relation	rel1,
				rel2;
	void *map; /* we don't actually need it */

	rel1 = heap_open(PG_GETARG_OID(0), AccessShareLock);
	rel2 = heap_open(PG_GETARG_OID(1), AccessShareLock);

	/* Try to build a conversion map */
	map = convert_tuples_by_name_map(RelationGetDescr(rel1),
									 RelationGetDescr(rel2),
									 ERR_PART_DESC_CONVERT);

	/* Now free map */
	pfree(map);

	heap_close(rel1, AccessShareLock);
	heap_close(rel2, AccessShareLock);

	/* still return true to avoid changing tests */
	PG_RETURN_BOOL(true);
}


/*
 * ------------------------
 *  Useful string builders
 * ------------------------
 */

Datum
build_check_constraint_name(PG_FUNCTION_ARGS)
{
	Oid			relid = PG_GETARG_OID(0);
	const char *result;

	if (!SearchSysCacheExists1(RELOID, ObjectIdGetDatum(relid)))
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("relation \"%u\" does not exist", relid)));

	result = build_check_constraint_name_relid_internal(relid);
	PG_RETURN_TEXT_P(cstring_to_text(quote_identifier(result)));
}


/*
 * ------------------------
 *  Cache & config updates
 * ------------------------
 */

/*
 * Try to add previously partitioned table to PATHMAN_CONFIG.
 */
Datum
add_to_pathman_config(PG_FUNCTION_ARGS)
{
	Oid					relid;
	char			   *expression;
	PartType			parttype;

	Oid				   *children;
	uint32				children_count;

	Relation			pathman_config;
	Datum				values[Natts_pathman_config];
	bool				isnull[Natts_pathman_config];
	HeapTuple			htup;

	Oid					expr_type;

	PathmanInitState	init_state;

	if (!IsPathmanReady())
		elog(ERROR, "pg_pathman is disabled");

	if (!PG_ARGISNULL(0))
	{
		relid = PG_GETARG_OID(0);
	}
	else ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("'parent_relid' should not be NULL")));

	/* Protect data + definition from concurrent modification */
	LockRelationOid(relid, AccessExclusiveLock);

	/* Check that relation exists */
	if (!SearchSysCacheExists1(RELOID, ObjectIdGetDatum(relid)))
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("relation \"%u\" does not exist", relid)));

	if (!PG_ARGISNULL(1))
	{
		expression = TextDatumGetCString(PG_GETARG_TEXT_P(1));
	}
	else ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("'expression' should not be NULL")));

	/* Check current user's privileges */
	if (!check_security_policy_internal(relid, GetUserId()))
	{
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("only the owner or superuser can change "
						"partitioning configuration of table \"%s\"",
						get_rel_name_or_relid(relid))));
	}

	/* Select partitioning type */
	switch (PG_NARGS())
	{
		/* HASH */
		case 2:
			{
				parttype = PT_HASH;

				values[Anum_pathman_config_range_interval - 1]	= (Datum) 0;
				isnull[Anum_pathman_config_range_interval - 1]	= true;
			}
			break;

		/* RANGE */
		case 3:
			{
				parttype = PT_RANGE;

				values[Anum_pathman_config_range_interval - 1]	= PG_GETARG_DATUM(2);
				isnull[Anum_pathman_config_range_interval - 1]	= PG_ARGISNULL(2);
			}
			break;

		default:
			elog(ERROR, "error in function " CppAsString(add_to_pathman_config));
			PG_RETURN_BOOL(false); /* keep compiler happy */
	}

	/* Parse and check expression */
	cook_partitioning_expression(relid, expression, &expr_type);

	/* Canonicalize user's expression (trim whitespaces etc) */
	expression = canonicalize_partitioning_expression(relid, expression);

	/* Check hash function for HASH partitioning */
	if (parttype == PT_HASH)
	{
		TypeCacheEntry *tce = lookup_type_cache(expr_type, TYPECACHE_HASH_PROC);

		if (!OidIsValid(tce->hash_proc))
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("no hash function for partitioning expression")));
	}

	/*
	 * Initialize columns (partrel, attname, parttype, range_interval).
	 */
	values[Anum_pathman_config_partrel - 1]		= ObjectIdGetDatum(relid);
	isnull[Anum_pathman_config_partrel - 1]		= false;

	values[Anum_pathman_config_parttype - 1]	= Int32GetDatum(parttype);
	isnull[Anum_pathman_config_parttype - 1]	= false;

	values[Anum_pathman_config_expr - 1]		= CStringGetTextDatum(expression);
	isnull[Anum_pathman_config_expr - 1]		= false;

	/* Insert new row into PATHMAN_CONFIG */
	pathman_config = heap_open(get_pathman_config_relid(false), RowExclusiveLock);

	htup = heap_form_tuple(RelationGetDescr(pathman_config), values, isnull);
	CatalogTupleInsert(pathman_config, htup);

	heap_close(pathman_config, RowExclusiveLock);

	/* Make changes visible */
	CommandCounterIncrement();

	/* Update caches only if this relation has children */
	if (FCS_FOUND == find_inheritance_children_array(relid, NoLock, true,
													 &children_count,
													 &children))
	{
		pfree(children);

		PG_TRY();
		{
			/* Some flags might change during refresh attempt */
			save_pathman_init_state(&init_state);

			/* Now try to create a PartRelationInfo */
			has_pathman_relation_info(relid);
		}
		PG_CATCH();
		{
			/* We have to restore changed flags */
			restore_pathman_init_state(&init_state);

			/* Rethrow ERROR */
			PG_RE_THROW();
		}
		PG_END_TRY();
	}

	/* Check if naming sequence exists */
	if (parttype == PT_RANGE)
	{
		RangeVar   *naming_seq_rv;
		Oid			naming_seq;

		naming_seq_rv = makeRangeVar(get_namespace_name(get_rel_namespace(relid)),
									 build_sequence_name_relid_internal(relid),
									 -1);

		naming_seq = RangeVarGetRelid(naming_seq_rv, AccessShareLock, true);
		if (OidIsValid(naming_seq))
		{
			ObjectAddress	parent,
							sequence;

			ObjectAddressSet(parent, RelationRelationId, relid);
			ObjectAddressSet(sequence, RelationRelationId, naming_seq);

			/* Now this naming sequence is a "part" of partitioned relation */
			recordDependencyOn(&sequence, &parent, DEPENDENCY_NORMAL);
		}
	}

	CacheInvalidateRelcacheByRelid(relid);

	PG_RETURN_BOOL(true);
}

/*
 * Invalidate relcache to refresh PartRelationInfo.
 */
Datum
pathman_config_params_trigger_func(PG_FUNCTION_ARGS)
{
	TriggerData	   *trigdata = (TriggerData *) fcinfo->context;
	Oid				pathman_config_params;
	Oid				pathman_config;
	Oid				partrel;
	Datum			partrel_datum;
	bool			partrel_isnull;

	/* Fetch Oid of PATHMAN_CONFIG_PARAMS */
	pathman_config_params = get_pathman_config_params_relid(true);
	pathman_config = get_pathman_config_relid(true);

	/* Handle "pg_pathman.enabled = f" case */
	if (!OidIsValid(pathman_config_params))
		goto pathman_config_params_trigger_func_return;

	/* Handle user calls */
	if (!CALLED_AS_TRIGGER(fcinfo))
		elog(ERROR, "this function should not be called directly");

	/* Handle wrong fire mode */
	if (!TRIGGER_FIRED_FOR_ROW(trigdata->tg_event))
		elog(ERROR, "%s: must be fired for row",
			 trigdata->tg_trigger->tgname);

	/* Handle wrong relation */
	if (RelationGetRelid(trigdata->tg_relation) != pathman_config_params &&
		RelationGetRelid(trigdata->tg_relation) != pathman_config)
		elog(ERROR, "%s: must be fired for relation \"%s\" or \"%s\"",
			 trigdata->tg_trigger->tgname,
			 get_rel_name(pathman_config_params),
			 get_rel_name(pathman_config));

	/*
	 * Extract partitioned relation's Oid.
	 * Hacky: 1 is attrnum of relid for both pathman_config and pathman_config_params
	 */
	partrel_datum = heap_getattr(trigdata->tg_trigtuple,
								 Anum_pathman_config_params_partrel,
								 RelationGetDescr(trigdata->tg_relation),
								 &partrel_isnull);
	Assert(partrel_isnull == false); /* partrel should not be NULL! */

	partrel = DatumGetObjectId(partrel_datum);

	/* Finally trigger pg_pathman's cache invalidation event */
	if (SearchSysCacheExists1(RELOID, ObjectIdGetDatum(partrel)))
		CacheInvalidateRelcacheByRelid(partrel);

pathman_config_params_trigger_func_return:
	/* Return the tuple we've been given */
	if (trigdata->tg_event & TRIGGER_EVENT_UPDATE)
		PG_RETURN_POINTER(trigdata->tg_newtuple);
	else
		PG_RETURN_POINTER(trigdata->tg_trigtuple);

}


/*
 * --------------------------
 *  Special locking routines
 * --------------------------
 */

/*
 * Prevent concurrent modifiction of partitioning schema.
 */
Datum
prevent_part_modification(PG_FUNCTION_ARGS)
{
	Oid relid = PG_GETARG_OID(0);

	/* Lock partitioned relation till transaction's end */
	LockRelationOid(relid, ShareUpdateExclusiveLock);

	PG_RETURN_VOID();
}

/*
 * Lock relation exclusively & check for current isolation level.
 */
Datum
prevent_data_modification(PG_FUNCTION_ARGS)
{
	Oid relid = PG_GETARG_OID(0);

	/*
	 * Check that isolation level is READ COMMITTED.
	 * Else we won't be able to see new rows
	 * which could slip through locks.
	 */
	if (!xact_is_level_read_committed())
		ereport(ERROR,
				(errmsg("Cannot perform blocking partitioning operation"),
				 errdetail("Expected READ COMMITTED isolation level")));

	LockRelationOid(relid, AccessExclusiveLock);

	PG_RETURN_VOID();
}


/*
 * -------------------------------------------
 *  User-defined partition creation callbacks
 * -------------------------------------------
 */

/*
 * Checks that callback function meets specific requirements.
 * It must have the only JSONB argument and BOOL return type.
 */
Datum
validate_part_callback_pl(PG_FUNCTION_ARGS)
{
	PG_RETURN_BOOL(validate_part_callback(PG_GETARG_OID(0),
										  PG_GETARG_BOOL(1)));
}

/*
 * Builds JSONB object containing new partition parameters
 * and invokes the callback.
 */
Datum
invoke_on_partition_created_callback(PG_FUNCTION_ARGS)
{
#define ARG_PARENT			0	/* parent table */
#define ARG_CHILD			1	/* partition */
#define ARG_CALLBACK		2	/* callback to be invoked */
#define ARG_RANGE_START		3	/* start_value */
#define ARG_RANGE_END		4	/* end_value */

	Oid						parent_relid,
							partition_relid;

	Oid						callback_oid = InvalidOid;
	init_callback_params	callback_params;


	/* NOTE: callback may be NULL */
	if (!PG_ARGISNULL(ARG_CALLBACK))
	{
		callback_oid = PG_GETARG_OID(ARG_CALLBACK);
	}

	/* If there's no callback function specified, we're done */
	if (callback_oid == InvalidOid)
		PG_RETURN_VOID();

	if (!PG_ARGISNULL(ARG_PARENT))
	{
		parent_relid = PG_GETARG_OID(ARG_PARENT);
	}
	else ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("'parent_relid' should not be NULL")));

	if (!PG_ARGISNULL(ARG_CHILD))
	{
		partition_relid = PG_GETARG_OID(ARG_CHILD);
	}
	else ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("'partition_relid' should not be NULL")));

	switch (PG_NARGS())
	{
		case 3:
			MakeInitCallbackHashParams(&callback_params,
									   callback_oid,
									   parent_relid,
									   partition_relid);
			break;

		case 5:
			{
				Bound	start,
						end;
				Oid		value_type;

				/* Fetch start & end values for RANGE + their type */
				start = PG_ARGISNULL(ARG_RANGE_START) ?
								MakeBoundInf(MINUS_INFINITY) :
								MakeBound(PG_GETARG_DATUM(ARG_RANGE_START));

				end = PG_ARGISNULL(ARG_RANGE_END) ?
								MakeBoundInf(PLUS_INFINITY) :
								MakeBound(PG_GETARG_DATUM(ARG_RANGE_END));

				value_type = get_fn_expr_argtype(fcinfo->flinfo, ARG_RANGE_START);

				MakeInitCallbackRangeParams(&callback_params,
											callback_oid,
											parent_relid,
											partition_relid,
											start,
											end,
											value_type);
			}
			break;

		default:
			elog(ERROR, "error in function "
						CppAsString(invoke_on_partition_created_callback));
	}

	/* Now it's time to call it! */
	invoke_part_callback(&callback_params);

	PG_RETURN_VOID();
}

/*
 * Function to be used for RLS rules on PATHMAN_CONFIG and
 * PATHMAN_CONFIG_PARAMS tables.
 * NOTE: check_security_policy_internal() is used under the hood.
 */
Datum
check_security_policy(PG_FUNCTION_ARGS)
{
	Oid relid = PG_GETARG_OID(0);

	if (!check_security_policy_internal(relid, GetUserId()))
	{
		elog(WARNING, "only the owner or superuser can change "
					  "partitioning configuration of table \"%s\"",
			 get_rel_name_or_relid(relid));

		PG_RETURN_BOOL(false);
	}

	/* Else return TRUE */
	PG_RETURN_BOOL(true);
}

/*
 * Check if type supports the specified operator ( + | - | etc ).
 */
Datum
is_operator_supported(PG_FUNCTION_ARGS)
{
	Oid		opid,
			typid	= PG_GETARG_OID(0);
	char   *opname	= TextDatumGetCString(PG_GETARG_TEXT_P(1));

	opid = compatible_oper_opid(list_make1(makeString(opname)),
								typid, typid, true);

	PG_RETURN_BOOL(OidIsValid(opid));
}


/*
 * -------
 *  DEBUG
 * -------
 */

/* NOTE: used for DEBUG, set breakpoint here */
Datum
debug_capture(PG_FUNCTION_ARGS)
{
	static float8 sleep_time = 0;
	DirectFunctionCall1(pg_sleep, Float8GetDatum(sleep_time));

	/* Write something (doesn't really matter) */
	elog(WARNING, "debug_capture [%u]", MyProcPid);

	PG_RETURN_VOID();
}

/* Return pg_pathman's shared library version */
Datum
pathman_version(PG_FUNCTION_ARGS)
{
	PG_RETURN_CSTRING(CURRENT_LIB_VERSION);
}
