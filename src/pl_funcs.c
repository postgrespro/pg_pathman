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
#include "catalog/dependency.h"
#include "catalog/indexing.h"
#include "catalog/namespace.h"
#include "catalog/pg_inherits_fn.h"
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


/* Function declarations */

PG_FUNCTION_INFO_V1( get_number_of_partitions_pl );
PG_FUNCTION_INFO_V1( get_parent_of_partition_pl );
PG_FUNCTION_INFO_V1( get_base_type_pl );
PG_FUNCTION_INFO_V1( get_partition_key_type );
PG_FUNCTION_INFO_V1( get_tablespace_pl );

PG_FUNCTION_INFO_V1( show_cache_stats_internal );
PG_FUNCTION_INFO_V1( show_partition_list_internal );

PG_FUNCTION_INFO_V1( build_update_trigger_name );
PG_FUNCTION_INFO_V1( build_update_trigger_func_name );
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

PG_FUNCTION_INFO_V1( create_update_triggers );
PG_FUNCTION_INFO_V1( pathman_update_trigger_func );
PG_FUNCTION_INFO_V1( create_single_update_trigger );
PG_FUNCTION_INFO_V1( has_update_trigger );

PG_FUNCTION_INFO_V1( debug_capture );
PG_FUNCTION_INFO_V1( get_pathman_lib_version );


/* User context for function show_partition_list_internal() */
typedef struct
{
	Relation				pathman_config;
	HeapScanDesc			pathman_config_scan;
	Snapshot				snapshot;

	const PartRelationInfo *current_prel;	/* selected PartRelationInfo */

	Size					child_number;	/* child we're looking at */
	SPITupleTable		   *tuptable;		/* buffer for tuples */
} show_partition_list_cxt;

/* User context for function show_pathman_cache_stats_internal() */
typedef struct
{
	MemoryContext			pathman_contexts[PATHMAN_MCXT_COUNT];
	HTAB				   *pathman_htables[PATHMAN_MCXT_COUNT];
	int						current_item;
} show_cache_stats_cxt;


static AttrNumber *pathman_update_trigger_build_attr_map(const PartRelationInfo *prel,
														 Relation child_rel);

static ExprState *pathman_update_trigger_build_expr_state(const PartRelationInfo *prel,
														  Relation source_rel,
														  HeapTuple new_tuple,
														  Oid *expr_type);

static void pathman_update_trigger_func_move_tuple(Relation source_rel,
												   Relation target_rel,
												   HeapTuple old_tuple,
												   HeapTuple new_tuple);


/*
 * ------------------------
 *  Various useful getters
 * ------------------------
 */

/*
 * Get number of relation's partitions managed by pg_pathman.
 */
Datum
get_number_of_partitions_pl(PG_FUNCTION_ARGS)
{
	Oid						parent = PG_GETARG_OID(0);
	const PartRelationInfo *prel;

	/* If we couldn't find PartRelationInfo, return 0 */
	if ((prel = get_pathman_relation_info(parent)) == NULL)
		PG_RETURN_INT32(0);

	PG_RETURN_INT32(PrelChildrenCount(prel));
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
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("\"%s\" is not a partition",
							   get_rel_name_or_relid(partition))));

		PG_RETURN_NULL();
	}
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
 * Return partition key type.
 */
Datum
get_partition_key_type(PG_FUNCTION_ARGS)
{
	Oid						relid = PG_GETARG_OID(0);
	const PartRelationInfo *prel;

	prel = get_pathman_relation_info(relid);
	shout_if_prel_is_invalid(relid, prel, PT_ANY);

	PG_RETURN_OID(prel->ev_type);
}

/*
 * Return tablespace name of a specified relation.
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
		tablespace_id = GetDefaultTablespace(get_rel_persistence(relid));

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

		old_mcxt = MemoryContextSwitchTo(funccxt->multi_call_memory_ctx);

		usercxt = (show_cache_stats_cxt *) palloc(sizeof(show_cache_stats_cxt));

		usercxt->pathman_contexts[0] = TopPathmanContext;
		usercxt->pathman_contexts[1] = PathmanRelationCacheContext;
		usercxt->pathman_contexts[2] = PathmanParentCacheContext;
		usercxt->pathman_contexts[3] = PathmanBoundCacheContext;

		usercxt->pathman_htables[0] = NULL; /* no HTAB for this entry */
		usercxt->pathman_htables[1] = partitioned_rels;
		usercxt->pathman_htables[2] = parent_cache;
		usercxt->pathman_htables[3] = bound_cache;

		usercxt->current_item = 0;

		/* Create tuple descriptor */
		tupdesc = CreateTemplateTupleDesc(Natts_pathman_cache_stats, false);

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
				CStringGetTextDatum(simpify_mcxt_name(current_mcxt));

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
		usercxt->pathman_config_scan = heap_beginscan(usercxt->pathman_config,
													  usercxt->snapshot, 0, NULL);

		usercxt->current_prel = NULL;

		/* Create tuple descriptor */
		tupdesc = CreateTemplateTupleDesc(Natts_pathman_partition_list, false);

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
			const PartRelationInfo *prel;
			HeapTuple				htup;
			Datum					values[Natts_pathman_partition_list];
			bool					isnull[Natts_pathman_partition_list] = { 0 };

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
											datum_to_cstring(BoundGetValue(&re->min),
															 prel->ev_type));

							values[Anum_pathman_pl_range_min - 1] = rmin;
						}
						else isnull[Anum_pathman_pl_range_min - 1] = true;

						/* Upper bound text */
						if (!IsInfinite(&re->max))
						{
							Datum rmax = CStringGetTextDatum(
											datum_to_cstring(BoundGetValue(&re->max),
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
		heap_endscan(usercxt->pathman_config_scan);
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


Datum
is_tuple_convertible(PG_FUNCTION_ARGS)
{
	Relation	rel1,
				rel2;
	bool		res = true;

	rel1 = heap_open(PG_GETARG_OID(0), AccessShareLock);
	rel2 = heap_open(PG_GETARG_OID(1), AccessShareLock);

	PG_TRY();
	{
		void *map; /* we don't actually need it */

		/* Try to build a conversion map */
		map = convert_tuples_by_name_map(RelationGetDescr(rel1),
										 RelationGetDescr(rel2),
										 ERR_PART_DESC_CONVERT);
		/* Now free map */
		pfree(map);
	}
	PG_CATCH();
	{
		res = false;
	}
	PG_END_TRY();

	heap_close(rel1, AccessShareLock);
	heap_close(rel2, AccessShareLock);

	PG_RETURN_BOOL(res);
}

/*
 * ------------------------
 *  Useful string builders
 * ------------------------
 */

Datum
build_update_trigger_name(PG_FUNCTION_ARGS)
{
	Oid			relid = PG_GETARG_OID(0);
	const char *result;

	/* Check that relation exists */
	if (!SearchSysCacheExists1(RELOID, ObjectIdGetDatum(relid)))
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("relation \"%u\" does not exist", relid)));

	result = quote_identifier(build_update_trigger_name_internal(relid));

	PG_RETURN_TEXT_P(cstring_to_text(result));
}

Datum
build_update_trigger_func_name(PG_FUNCTION_ARGS)
{
	Oid			relid = PG_GETARG_OID(0);
	Oid			nspid;
	const char *result,
			   *func_name;

	/* Check that relation exists */
	if (!SearchSysCacheExists1(RELOID, ObjectIdGetDatum(relid)))
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("relation \"%u\" does not exist", relid)));

	nspid = get_rel_namespace(relid);

	func_name = build_update_trigger_func_name_internal(relid);
	result = psprintf("%s.%s",
					  quote_identifier(get_namespace_name(nspid)),
					  quote_identifier(func_name));

	PG_RETURN_TEXT_P(cstring_to_text(result));
}

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
	Datum				expr_datum;

	PathmanInitState	init_state;

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
	expr_datum = cook_partitioning_expression(relid, expression, &expr_type);

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

	values[Anum_pathman_config_cooked_expr - 1]	= expr_datum;
	isnull[Anum_pathman_config_cooked_expr - 1]	= false;

	/* Insert new row into PATHMAN_CONFIG */
	pathman_config = heap_open(get_pathman_config_relid(false), RowExclusiveLock);

	htup = heap_form_tuple(RelationGetDescr(pathman_config), values, isnull);
	CatalogTupleInsert(pathman_config, htup);

	heap_close(pathman_config, RowExclusiveLock);

	/* Update caches only if this relation has children */
	if (FCS_FOUND == find_inheritance_children_array(relid, NoLock, true,
													 &children_count,
													 &children))
	{
		pfree(children);

		/* Now try to create a PartRelationInfo */
		PG_TRY();
		{
			/* Some flags might change during refresh attempt */
			save_pathman_init_state(&init_state);

			refresh_pathman_relation_info(relid,
										  values,
										  false); /* initialize immediately */
		}
		PG_CATCH();
		{
			/* We have to restore all changed flags */
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
									 build_sequence_name_internal(relid),
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
	Oid				partrel;
	Datum			partrel_datum;
	bool			partrel_isnull;

	/* Fetch Oid of PATHMAN_CONFIG_PARAMS */
	pathman_config_params = get_pathman_config_params_relid(true);

	/* Handle "pg_pathman.enabled = t" case */
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
	if (RelationGetRelid(trigdata->tg_relation) != pathman_config_params)
		elog(ERROR, "%s: must be fired for relation \"%s\"",
			 trigdata->tg_trigger->tgname,
			 get_rel_name(pathman_config_params));

	/* Extract partitioned relation's Oid */
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
 * Acquire appropriate lock on a partitioned relation.
 */
Datum
prevent_part_modification(PG_FUNCTION_ARGS)
{
	Oid			relid = PG_GETARG_OID(0);

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
	prevent_data_modification_internal(PG_GETARG_OID(0));

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
 * --------------------------
 *  Update trigger machinery
 * --------------------------
 */

/* Behold: the update trigger itself */
Datum
pathman_update_trigger_func(PG_FUNCTION_ARGS)
{
	TriggerData			   *trigdata = (TriggerData *) fcinfo->context;

	Relation				source_rel;

	Oid						parent_relid,
							source_relid,
							target_relid;

	HeapTuple				old_tuple,
							new_tuple;

	Datum					value;
	Oid						value_type;
	bool					isnull;

	Oid					   *parts;
	int						nparts;

	ExprContext			   *econtext;
	ExprState			   *expr_state;
	MemoryContext		    old_mcxt;
	PartParentSearch		parent_search;
	const PartRelationInfo *prel;

	/* Handle user calls */
	if (!CALLED_AS_TRIGGER(fcinfo))
		elog(ERROR, "this function should not be called directly");

	/* Handle wrong fire mode */
	if (!TRIGGER_FIRED_FOR_ROW(trigdata->tg_event))
		elog(ERROR, "%s: must be fired for row",
			 trigdata->tg_trigger->tgname);

	/* Make sure that trigger was fired during UPDATE command */
	if (!TRIGGER_FIRED_BY_UPDATE(trigdata->tg_event))
		elog(ERROR, "this function should only be used as UPDATE trigger");

	/* Get source relation and its Oid */
	source_rel		= trigdata->tg_relation;
	source_relid	= RelationGetRelid(trigdata->tg_relation);

	/* Fetch old & new tuples */
	old_tuple = trigdata->tg_trigtuple;
	new_tuple = trigdata->tg_newtuple;

	/* Find parent relation and partitioning info */
	parent_relid = get_parent_of_partition(source_relid, &parent_search);
	if (parent_search != PPS_ENTRY_PART_PARENT)
		elog(ERROR, "relation \"%s\" is not a partition",
			 RelationGetRelationName(source_rel));

	/* Fetch partition dispatch info */
	prel = get_pathman_relation_info(parent_relid);
	shout_if_prel_is_invalid(parent_relid, prel, PT_ANY);

	/* Execute partitioning expression */
	econtext = CreateStandaloneExprContext();
	old_mcxt = MemoryContextSwitchTo(econtext->ecxt_per_tuple_memory);
	expr_state = pathman_update_trigger_build_expr_state(prel,
														 source_rel,
														 new_tuple,
														 &value_type);
	value = ExecEvalExprCompat(expr_state, econtext, &isnull,
							   mult_result_handler);
	MemoryContextSwitchTo(old_mcxt);

	if (isnull)
		elog(ERROR, ERR_PART_ATTR_NULL);

	/* Search for matching partitions */
	parts = find_partitions_for_value(value, value_type, prel, &nparts);

	/* We can free expression context now */
	FreeExprContext(econtext, false);

	if (nparts > 1)
		elog(ERROR, ERR_PART_ATTR_MULTIPLE);
	else if (nparts == 0)
	{
		 target_relid = create_partitions_for_value(parent_relid,
													value, value_type);

		 /* get_pathman_relation_info() will refresh this entry */
		 invalidate_pathman_relation_info(parent_relid, NULL);
	}
	else target_relid = parts[0];

	pfree(parts);

	/* Convert tuple if target partition has changed */
	if (target_relid != source_relid)
	{
		Relation	target_rel;
		LOCKMODE	lockmode = RowExclusiveLock; /* UPDATE */

		/* Lock partition and check if it exists */
		LockRelationOid(target_relid, lockmode);
		if (!SearchSysCacheExists1(RELOID, ObjectIdGetDatum(target_relid)))
			elog(ERROR, ERR_PART_ATTR_NO_PART, datum_to_cstring(value, value_type));

		/* Open partition */
		target_rel = heap_open(target_relid, lockmode);

		/* Move tuple from source relation to the selected partition */
		pathman_update_trigger_func_move_tuple(source_rel, target_rel,
											   old_tuple, new_tuple);

		/* Close partition */
		heap_close(target_rel, lockmode);

		/* We've made some changes */
		PG_RETURN_VOID();
	}

	/* Just return NEW tuple */
	PG_RETURN_POINTER(new_tuple);
}

struct replace_vars_cxt
{
	HeapTuple		new_tuple;
	TupleDesc		tuple_desc;
	AttrNumber	   *attributes_map;
};

/* Replace Vars with values from 'new_tuple' (Consts) */
static Node *
replace_vars_with_consts(Node *node, struct replace_vars_cxt *ctx)
{
	const TypeCacheEntry *typcache;

	if (IsA(node, Var))
	{
		Var			*var = (Var *) node;
		AttrNumber	 varattno = ctx->attributes_map[var->varattno - 1];
		Oid			 vartype;
		Const		*new_const = makeNode(Const);
		HeapTuple	 htup;

		Assert(var->varno == PART_EXPR_VARNO);
		if (varattno == 0)
			elog(ERROR, ERR_PART_DESC_CONVERT);

		/* we suppose that type can be different from parent */
		vartype = ctx->tuple_desc->attrs[varattno - 1]->atttypid;

		htup = SearchSysCache1(TYPEOID, ObjectIdGetDatum(vartype));
		if (HeapTupleIsValid(htup))
		{
			Form_pg_type typtup = (Form_pg_type) GETSTRUCT(htup);
			new_const->consttypmod = typtup->typtypmod;
			new_const->constcollid = typtup->typcollation;
			ReleaseSysCache(htup);
		}
		else elog(ERROR, "cache lookup failed for type %u", vartype);

		typcache = lookup_type_cache(vartype, 0);
		new_const->constbyval	= typcache->typbyval;
		new_const->constlen		= typcache->typlen;
		new_const->consttype	= vartype;
		new_const->location		= -1;

		/* extract value from NEW tuple */
		new_const->constvalue = heap_getattr(ctx->new_tuple,
											 varattno,
											 ctx->tuple_desc,
											 &new_const->constisnull);
		return (Node *) new_const;
	}

	return expression_tree_mutator(node, replace_vars_with_consts, (void *) ctx);
}

/*
 * Get attributes map between parent and child relation.
 * This is simplified version of functions that return TupleConversionMap.
 * And it should be faster if expression uses not all fields from relation.
 */
static AttrNumber *
pathman_update_trigger_build_attr_map(const PartRelationInfo *prel,
									  Relation child_rel)
{
	AttrNumber	i = -1;
	Oid			parent_relid = PrelParentRelid(prel);
	TupleDesc	child_descr = RelationGetDescr(child_rel);
	int			natts = child_descr->natts;
	AttrNumber *result = (AttrNumber *) palloc0(natts * sizeof(AttrNumber));

	while ((i = bms_next_member(prel->expr_atts, i)) >= 0)
	{
		int			j;
		AttrNumber	attnum = i + FirstLowInvalidHeapAttributeNumber;
		char	   *attname = get_attname(parent_relid, attnum);

		for (j = 0; j < natts; j++)
		{
			Form_pg_attribute att = child_descr->attrs[j];

			if (att->attisdropped)
				continue; /* attrMap[attnum - 1] is already 0 */

			if (strcmp(NameStr(att->attname), attname) == 0)
			{
				result[attnum - 1] = (AttrNumber) (j + 1);
				break;
			}
		}

		if (result[attnum - 1] == 0)
			elog(ERROR, "Couldn't find '%s' column in child relation", attname);
	}

	return result;
}

static ExprState *
pathman_update_trigger_build_expr_state(const PartRelationInfo *prel,
										Relation source_rel,
										HeapTuple new_tuple,
										Oid *expr_type)		/* ret value #1 */
{
	struct replace_vars_cxt		ctx;
	Node					   *expr;
	ExprState				   *expr_state;

	ctx.new_tuple =			new_tuple;
	ctx.attributes_map =	pathman_update_trigger_build_attr_map(prel, source_rel);
	ctx.tuple_desc =		RelationGetDescr(source_rel);

	expr = replace_vars_with_consts(prel->expr, &ctx);
	expr_state = ExecInitExpr((Expr *) expr, NULL);

	AssertArg(expr_type);
	*expr_type = exprType(expr);

	return expr_state;
}


/* Move tuple to new partition (delete 'old_tuple' + insert 'new_tuple') */
static void
pathman_update_trigger_func_move_tuple(Relation source_rel,
									   Relation target_rel,
									   HeapTuple old_tuple,
									   HeapTuple new_tuple)
{
	TupleDesc				source_tupdesc,
							target_tupdesc;
	HeapTuple				target_tuple;
	TupleConversionMap	   *conversion_map;

	/* HACK: use fake 'tdtypeid' in order to fool convert_tuples_by_name() */
	source_tupdesc = CreateTupleDescCopy(RelationGetDescr(source_rel));
	source_tupdesc->tdtypeid = InvalidOid;

	target_tupdesc = CreateTupleDescCopy(RelationGetDescr(target_rel));
	target_tupdesc->tdtypeid = InvalidOid;

	/* Build tuple conversion map */
	conversion_map = convert_tuples_by_name(source_tupdesc,
											target_tupdesc,
											ERR_PART_DESC_CONVERT);

	if (conversion_map)
	{
		/* Convert tuple */
		target_tuple = do_convert_tuple(new_tuple, conversion_map);

		/* Free tuple conversion map */
		free_conversion_map(conversion_map);
	}
	else target_tuple = new_tuple;

	/* Connect using SPI and execute a few queries */
	if (SPI_connect() == SPI_OK_CONNECT)
	{
		int			nvalues = RelationGetDescr(target_rel)->natts;
		Oid		   *types	= palloc(nvalues * sizeof(Oid));
		Datum	   *values	= palloc(nvalues * sizeof(Datum));
		char	   *nulls	= palloc(nvalues * sizeof(char));
		StringInfo	query	= makeStringInfo();
		int			i;

		/* Prepare query string */
		appendStringInfo(query, "DELETE FROM %s.%s WHERE ctid = $1",
						 quote_identifier(get_namespace_name(
												RelationGetNamespace(source_rel))),
						 quote_identifier(RelationGetRelationName(source_rel)));

		/* Build singe argument */
		types[0]	= TIDOID;
		values[0]	= PointerGetDatum(&old_tuple->t_self);
		nulls[0]	= ' ';

		/* DELETE FROM source_rel WHERE ctid = $1 */
		SPI_execute_with_args(query->data, 1, types, values, nulls, false, 0);

		resetStringInfo(query);

		/* Prepare query string */
		appendStringInfo(query, "INSERT INTO %s.%s VALUES (",
						 quote_identifier(get_namespace_name(
												RelationGetNamespace(target_rel))),
						 quote_identifier(RelationGetRelationName(target_rel)));
		for (i = 0; i < target_tupdesc->natts; i++)
		{
			AttrNumber	attnum = i + 1;
			bool		isnull;

			/* Build singe argument */
			types[i]	= target_tupdesc->attrs[i]->atttypid;
			values[i]	= heap_getattr(target_tuple, attnum, target_tupdesc, &isnull);
			nulls[i]	= isnull ? 'n' : ' ';

			/* Append "$N [,]" */
			appendStringInfo(query, (i != 0 ? ", $%i" : "$%i"), attnum);
		}
		appendStringInfoChar(query, ')');

		/* INSERT INTO target_rel VALUES($1, $2, $3 ...) */
		SPI_execute_with_args(query->data, nvalues, types, values, nulls, false, 0);

		/* Finally close SPI connection */
		SPI_finish();
	}
	/* Else emit error */
	else elog(ERROR, "could not connect using SPI");

	/* At last, free these temporary tuple descs */
	FreeTupleDesc(source_tupdesc);
	FreeTupleDesc(target_tupdesc);
}

/* Create UPDATE triggers for all partitions */
Datum
create_update_triggers(PG_FUNCTION_ARGS)
{
	Oid						parent = PG_GETARG_OID(0);
	Oid					   *children;
	const char			   *trigname;
	const PartRelationInfo *prel;
	uint32					i;
	List				   *columns;

	/* Check that table is partitioned */
	prel = get_pathman_relation_info(parent);
	shout_if_prel_is_invalid(parent, prel, PT_ANY);

	/* Acquire trigger and attribute names */
	trigname = build_update_trigger_name_internal(parent);

	/* Create trigger for parent */
	columns = PrelExpressionColumnNames(prel);
	create_single_update_trigger_internal(parent, trigname, columns);

	/* Fetch children array */
	children = PrelGetChildrenArray(prel);

	/* Create triggers for each partition */
	for (i = 0; i < PrelChildrenCount(prel); i++)
		create_single_update_trigger_internal(children[i], trigname, columns);

	PG_RETURN_VOID();
}

/* Create an UPDATE trigger for partition */
Datum
create_single_update_trigger(PG_FUNCTION_ARGS)
{
	Oid						parent = PG_GETARG_OID(0);
	Oid						child = PG_GETARG_OID(1);
	const char			   *trigname;
	const PartRelationInfo *prel;
	List				   *columns;

	/* Check that table is partitioned */
	prel = get_pathman_relation_info(parent);
	shout_if_prel_is_invalid(parent, prel, PT_ANY);

	/* Acquire trigger and attribute names */
	trigname = build_update_trigger_name_internal(parent);

	/* Generate list of columns used in expression */
	columns = PrelExpressionColumnNames(prel);
	create_single_update_trigger_internal(child, trigname, columns);

	PG_RETURN_VOID();
}

/* Check if relation has pg_pathman's update trigger */
Datum
has_update_trigger(PG_FUNCTION_ARGS)
{
	Oid parent_relid = PG_GETARG_OID(0);

	/* Check that relation exists */
	if (!SearchSysCacheExists1(RELOID, ObjectIdGetDatum(parent_relid)))
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("relation \"%u\" does not exist", parent_relid)));

	PG_RETURN_BOOL(has_update_trigger_internal(parent_relid));
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

/* NOTE: just in case */
Datum
get_pathman_lib_version(PG_FUNCTION_ARGS)
{
	PG_RETURN_CSTRING(psprintf("%x", CURRENT_LIB_VERSION));
}
