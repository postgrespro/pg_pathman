/* ------------------------------------------------------------------------
 *
 * partition_filter.c
 *		Select partition for INSERT operation
 *
 * Copyright (c) 2016-2020, Postgres Professional
 *
 * ------------------------------------------------------------------------
 */

#include "compat/pg_compat.h"
#include "init.h"
#include "nodes_common.h"
#include "pathman.h"
#include "partition_creation.h"
#include "partition_filter.h"
#include "utils.h"

#include "access/htup_details.h"
#if PG_VERSION_NUM >= 120000
#include "access/table.h"
#endif
#include "access/xact.h"
#include "catalog/pg_class.h"
#include "catalog/pg_type.h"
#include "foreign/fdwapi.h"
#include "foreign/foreign.h"
#include "nodes/nodeFuncs.h"
#include "rewrite/rewriteManip.h"
#include "utils/guc.h"
#include "utils/memutils.h"
#include "utils/syscache.h"


#define ALLOC_EXP	2


/*
 * HACK: 'estate->es_query_cxt' as data storage
 *
 * We use this struct as an argument for fake
 * MemoryContextCallback pf_memcxt_callback()
 * in order to attach some additional info to
 * EState (estate->es_query_cxt is involved).
 */
typedef struct
{
	int		estate_alloc_result_rels;	/* number of allocated result rels */
	bool	estate_not_modified;		/* did we modify EState somehow? */
} estate_mod_data;

/*
 * Allow INSERTs into any FDW \ postgres_fdw \ no FDWs at all.
 */
typedef enum
{
	PF_FDW_INSERT_DISABLED = 0,		/* INSERTs into FDWs are prohibited */
	PF_FDW_INSERT_POSTGRES,			/* INSERTs into postgres_fdw are OK */
	PF_FDW_INSERT_ANY_FDW			/* INSERTs into any FDWs are OK */
} PF_insert_fdw_mode;

static const struct config_enum_entry pg_pathman_insert_into_fdw_options[] = {
	{ "disabled",	PF_FDW_INSERT_DISABLED,	false },
	{ "postgres",	PF_FDW_INSERT_POSTGRES,	false },
	{ "any_fdw",	PF_FDW_INSERT_ANY_FDW,	false },
	{ NULL,			0,						false }
};


bool				pg_pathman_enable_partition_filter = true;
int					pg_pathman_insert_into_fdw = PF_FDW_INSERT_POSTGRES;

CustomScanMethods	partition_filter_plan_methods;
CustomExecMethods	partition_filter_exec_methods;


static ExprState *prepare_expr_state(const PartRelationInfo *prel,
									 Relation source_rel,
									 EState *estate);

static void prepare_rri_for_insert(ResultRelInfoHolder *rri_holder,
								   const ResultPartsStorage *rps_storage);

static void prepare_rri_returning_for_insert(ResultRelInfoHolder *rri_holder,
											 const ResultPartsStorage *rps_storage);

static void prepare_rri_fdw_for_insert(ResultRelInfoHolder *rri_holder,
									   const ResultPartsStorage *rps_storage);

static Node *fix_returning_list_mutator(Node *node, void *state);

static Index append_rte_to_estate(EState *estate, RangeTblEntry *rte, Relation child_rel);
static int append_rri_to_estate(EState *estate, ResultRelInfo *rri);

static void pf_memcxt_callback(void *arg);
static estate_mod_data * fetch_estate_mod_data(EState *estate);


void
init_partition_filter_static_data(void)
{
	partition_filter_plan_methods.CustomName 			= INSERT_NODE_NAME;
	partition_filter_plan_methods.CreateCustomScanState	= partition_filter_create_scan_state;

	partition_filter_exec_methods.CustomName			= INSERT_NODE_NAME;
	partition_filter_exec_methods.BeginCustomScan		= partition_filter_begin;
	partition_filter_exec_methods.ExecCustomScan		= partition_filter_exec;
	partition_filter_exec_methods.EndCustomScan			= partition_filter_end;
	partition_filter_exec_methods.ReScanCustomScan		= partition_filter_rescan;
	partition_filter_exec_methods.MarkPosCustomScan		= NULL;
	partition_filter_exec_methods.RestrPosCustomScan	= NULL;
	partition_filter_exec_methods.ExplainCustomScan		= partition_filter_explain;

	DefineCustomBoolVariable("pg_pathman.enable_partitionfilter",
							 "Enables the planner's use of " INSERT_NODE_NAME " custom node.",
							 NULL,
							 &pg_pathman_enable_partition_filter,
							 true,
							 PGC_USERSET,
							 0,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomEnumVariable("pg_pathman.insert_into_fdw",
							 "Allow INSERTS into FDW partitions.",
							 NULL,
							 &pg_pathman_insert_into_fdw,
							 PF_FDW_INSERT_POSTGRES,
							 pg_pathman_insert_into_fdw_options,
							 PGC_SUSET,
							 0,
							 NULL,
							 NULL,
							 NULL);

	RegisterCustomScanMethods(&partition_filter_plan_methods);
}


/*
 * ---------------------------
 *  Partition Storage (cache)
 * ---------------------------
 */

/* Initialize ResultPartsStorage (hash table etc) */
void
init_result_parts_storage(ResultPartsStorage *parts_storage,
						  Oid parent_relid,
						  ResultRelInfo *current_rri,
						  EState *estate,
						  CmdType cmd_type,
						  bool close_relations,
						  bool speculative_inserts,
						  rri_holder_cb init_rri_holder_cb,
						  void *init_rri_holder_cb_arg,
						  rri_holder_cb fini_rri_holder_cb,
						  void *fini_rri_holder_cb_arg)
{
	HASHCTL *result_rels_table_config = &parts_storage->result_rels_table_config;

	memset(result_rels_table_config, 0, sizeof(HASHCTL));
	result_rels_table_config->keysize = sizeof(Oid);
	result_rels_table_config->entrysize = sizeof(ResultPartsStorage);

	parts_storage->result_rels_table = hash_create("ResultRelInfo storage", 10,
												   result_rels_table_config,
												   HASH_ELEM | HASH_BLOBS);
	Assert(current_rri);
	parts_storage->base_rri = current_rri;

	Assert(estate);
	parts_storage->estate = estate;

	/* ResultRelInfoHolder initialization callback */
	parts_storage->init_rri_holder_cb = init_rri_holder_cb;
	parts_storage->init_rri_holder_cb_arg = init_rri_holder_cb_arg;

	/* ResultRelInfoHolder finalization callback */
	parts_storage->fini_rri_holder_cb = fini_rri_holder_cb;
	parts_storage->fini_rri_holder_cb_arg = fini_rri_holder_cb_arg;

	Assert(cmd_type == CMD_INSERT || cmd_type == CMD_UPDATE);
	parts_storage->command_type = cmd_type;
	parts_storage->speculative_inserts = speculative_inserts;

	/*
	 * Should ResultPartsStorage do ExecCloseIndices and heap_close on
	 * finalization?
	 */
	parts_storage->close_relations = close_relations;
	parts_storage->head_open_lock_mode  = RowExclusiveLock;

	/* Fetch PartRelationInfo for this partitioned relation */
	parts_storage->prel = get_pathman_relation_info(parent_relid);
	shout_if_prel_is_invalid(parent_relid, parts_storage->prel, PT_ANY);

	/* Build a partitioning expression state */
	parts_storage->prel_expr_state = prepare_expr_state(parts_storage->prel,
														parts_storage->base_rri->ri_RelationDesc,
														parts_storage->estate);

	/* Build expression context */
	parts_storage->prel_econtext = CreateExprContext(parts_storage->estate);
}

/* Free ResultPartsStorage (close relations etc) */
void
fini_result_parts_storage(ResultPartsStorage *parts_storage)
{
	HASH_SEQ_STATUS			stat;
	ResultRelInfoHolder	   *rri_holder; /* ResultRelInfo holder */

	hash_seq_init(&stat, parts_storage->result_rels_table);
	while ((rri_holder = (ResultRelInfoHolder *) hash_seq_search(&stat)) != NULL)
	{
		/* Call finalization callback if needed */
		if (parts_storage->fini_rri_holder_cb)
			parts_storage->fini_rri_holder_cb(rri_holder, parts_storage);

		/*
		 * Close indices, unless ExecEndPlan won't do that for us (this is
		 * is CopyFrom which misses it, not usual executor run, essentially).
		 * Otherwise, it is always automaticaly closed; in <= 11, relcache
		 * refs of rris managed heap_open/close on their own, and ExecEndPlan
		 * closed them directly. Since 9ddef3, relcache management
		 * of executor was centralized; now rri refs are copies of ones in
		 * estate->es_relations, which are closed in ExecEndPlan.
		 * So we push our rel there, and it is also automatically closed.
		 */
		if (parts_storage->close_relations)
		{
			ExecCloseIndices(rri_holder->result_rel_info);
			/* And relation itself */
			heap_close_compat(rri_holder->result_rel_info->ri_RelationDesc,
				   NoLock);
		}

		/* Free conversion-related stuff */
		if (rri_holder->tuple_map)
		{
			FreeTupleDesc(rri_holder->tuple_map->indesc);
			FreeTupleDesc(rri_holder->tuple_map->outdesc);

			free_conversion_map(rri_holder->tuple_map);
		}

		/* Don't forget to close 'prel'! */
		if (rri_holder->prel)
			close_pathman_relation_info(rri_holder->prel);
	}

	/* Finally destroy hash table */
	hash_destroy(parts_storage->result_rels_table);

	/* Don't forget to close 'prel'! */
	close_pathman_relation_info(parts_storage->prel);
}

/* Find a ResultRelInfo for the partition using ResultPartsStorage */
ResultRelInfoHolder *
scan_result_parts_storage(ResultPartsStorage *parts_storage, Oid partid)
{
#define CopyToResultRelInfo(field_name) \
	( child_result_rel_info->field_name = parts_storage->base_rri->field_name )

	ResultRelInfoHolder	   *rri_holder;
	bool					found;

	rri_holder = hash_search(parts_storage->result_rels_table,
							 (const void *) &partid,
							 HASH_FIND, &found);

	/* If not found, create & cache new ResultRelInfo */
	if (!found)
	{
		Relation		child_rel,
						base_rel;
		RangeTblEntry  *child_rte,
					   *parent_rte;
		Index			child_rte_idx;
		ResultRelInfo  *child_result_rel_info;
		List		   *translated_vars;
		MemoryContext	old_mcxt;

		/* Lock partition and check if it exists */
		LockRelationOid(partid, parts_storage->head_open_lock_mode);
		if (!SearchSysCacheExists1(RELOID, ObjectIdGetDatum(partid)))
		{
			UnlockRelationOid(partid, parts_storage->head_open_lock_mode);
			return NULL;
		}

		/* Switch to query-level mcxt for allocations */
		old_mcxt = MemoryContextSwitchTo(parts_storage->estate->es_query_cxt);

		/* Create a new cache entry for this partition */
		rri_holder = hash_search(parts_storage->result_rels_table,
								 (const void *) &partid,
								 HASH_ENTER, NULL);

		parent_rte = rt_fetch(parts_storage->base_rri->ri_RangeTableIndex,
							  parts_storage->estate->es_range_table);

		/* Get base relation */
		base_rel = parts_storage->base_rri->ri_RelationDesc;

		/* Open child relation and check if it is a valid target */
		child_rel = heap_open_compat(partid, NoLock);

		/* Build Var translation list for 'inserted_cols' */
		make_inh_translation_list(base_rel, child_rel, 0, &translated_vars);

		/* Create RangeTblEntry for partition */
		child_rte = makeNode(RangeTblEntry);
		child_rte->rtekind			= RTE_RELATION;
		child_rte->relid			= partid;
		child_rte->relkind			= child_rel->rd_rel->relkind;
		child_rte->eref				= parent_rte->eref;
		child_rte->requiredPerms	= parent_rte->requiredPerms;
		child_rte->checkAsUser		= parent_rte->checkAsUser;
		child_rte->insertedCols		= translate_col_privs(parent_rte->insertedCols,
														  translated_vars);
		child_rte->updatedCols		= translate_col_privs(parent_rte->updatedCols,
														  translated_vars);

		/* Check permissions for partition */
		ExecCheckRTPerms(list_make1(child_rte), true);

		/* Append RangeTblEntry to estate->es_range_table */
		child_rte_idx = append_rte_to_estate(parts_storage->estate, child_rte, child_rel);

		/* Create ResultRelInfo for partition */
		child_result_rel_info = makeNode(ResultRelInfo);

		InitResultRelInfoCompat(child_result_rel_info,
								child_rel,
								child_rte_idx,
								parts_storage->estate->es_instrument);

		if (parts_storage->command_type != CMD_DELETE)
			ExecOpenIndices(child_result_rel_info, parts_storage->speculative_inserts);

		/* Copy necessary fields from saved ResultRelInfo */
		CopyToResultRelInfo(ri_WithCheckOptions);
		CopyToResultRelInfo(ri_WithCheckOptionExprs);
		CopyToResultRelInfo(ri_projectReturning);
#if PG_VERSION_NUM >= 110000
		CopyToResultRelInfo(ri_onConflict);
#else
		CopyToResultRelInfo(ri_onConflictSetProj);
		CopyToResultRelInfo(ri_onConflictSetWhere);
#endif

		if (parts_storage->command_type != CMD_UPDATE)
			CopyToResultRelInfo(ri_junkFilter);
		else
			child_result_rel_info->ri_junkFilter = NULL;

		/* ri_ConstraintExprs will be initialized by ExecRelCheck() */
		child_result_rel_info->ri_ConstraintExprs = NULL;

		/* Check that this partition is a valid result relation */
		CheckValidResultRelCompat(child_result_rel_info,
								  parts_storage->command_type);

		/* Fill the ResultRelInfo holder */
		rri_holder->partid = partid;
		rri_holder->result_rel_info = child_result_rel_info;

		/*
		 * Generate parent->child tuple transformation map. We need to
		 * convert tuples because e.g. parent's TupleDesc might have dropped
		 * columns which child doesn't have at all because it was created after
		 * the drop.
		 */
		rri_holder->tuple_map = build_part_tuple_map(base_rel, child_rel);

		/* Default values */
		rri_holder->prel = NULL;
		rri_holder->prel_expr_state = NULL;

		if ((rri_holder->prel = get_pathman_relation_info(partid)) != NULL)
		{
			rri_holder->prel_expr_state =
					prepare_expr_state(rri_holder->prel, /* NOTE: this prel! */
									   parts_storage->base_rri->ri_RelationDesc,
									   parts_storage->estate);
		}

		/* Call initialization callback if needed */
		if (parts_storage->init_rri_holder_cb)
			parts_storage->init_rri_holder_cb(rri_holder, parts_storage);

		/* Append ResultRelInfo to storage->es_alloc_result_rels */
		append_rri_to_estate(parts_storage->estate, child_result_rel_info);

		/* Don't forget to switch back! */
		MemoryContextSwitchTo(old_mcxt);
	}

	return rri_holder;
}

/* Refresh PartRelationInfo for the partition in storage */
PartRelationInfo *
refresh_result_parts_storage(ResultPartsStorage *parts_storage, Oid partid)
{
	if (partid == PrelParentRelid(parts_storage->prel))
	{
		close_pathman_relation_info(parts_storage->prel);
		parts_storage->prel = get_pathman_relation_info(partid);
		shout_if_prel_is_invalid(partid, parts_storage->prel, PT_ANY);

		return parts_storage->prel;
	}
	else
	{
		ResultRelInfoHolder *rri_holder;

		rri_holder = hash_search(parts_storage->result_rels_table,
								 (const void *) &partid,
								 HASH_FIND, NULL);

		/* We must have entry (since we got 'prel' from it) */
		Assert(rri_holder && rri_holder->prel);

		close_pathman_relation_info(rri_holder->prel);
		rri_holder->prel = get_pathman_relation_info(partid);
		shout_if_prel_is_invalid(partid, rri_holder->prel, PT_ANY);

		return rri_holder->prel;
	}
}

/* Build tuple conversion map (e.g. parent has a dropped column) */
TupleConversionMap *
build_part_tuple_map(Relation base_rel, Relation child_rel)
{
	TupleConversionMap *tuple_map;
	TupleDesc			child_tupdesc,
						parent_tupdesc;

	/* HACK: use fake 'tdtypeid' in order to fool convert_tuples_by_name() */
	child_tupdesc = CreateTupleDescCopy(RelationGetDescr(child_rel));
	child_tupdesc->tdtypeid = InvalidOid;

	parent_tupdesc = CreateTupleDescCopy(RelationGetDescr(base_rel));
	parent_tupdesc->tdtypeid = InvalidOid;

	/* Generate tuple transformation map and some other stuff */
	tuple_map = convert_tuples_by_name_compat(parent_tupdesc,
									   child_tupdesc,
									   ERR_PART_DESC_CONVERT);

	/* If map is one-to-one, free unused TupleDescs */
	if (!tuple_map)
	{
		FreeTupleDesc(child_tupdesc);
		FreeTupleDesc(parent_tupdesc);
	}

	return tuple_map;
}


/*
 * -----------------------------------
 *  Partition search helper functions
 * -----------------------------------
 */

/*
 * Find matching partitions for 'value' using PartRelationInfo.
 */
Oid *
find_partitions_for_value(Datum value, Oid value_type,
						  const PartRelationInfo *prel,
						  int *nparts)
{
#define CopyToTempConst(const_field, attr_field) \
	( temp_const.const_field = prel->attr_field )

	Const			temp_const;	/* temporary const for expr walker */
	WalkerContext	wcxt;
	List		   *ranges = NIL;

	/* Prepare dummy Const node */
	NodeSetTag(&temp_const, T_Const);
	temp_const.location = -1;

	/* Fill const with value ... */
	temp_const.constvalue	= value;
	temp_const.consttype	= value_type;
	temp_const.constisnull	= false;

	/* ... and some other important data */
	CopyToTempConst(consttypmod, ev_typmod);
	CopyToTempConst(constcollid, ev_collid);
	CopyToTempConst(constlen,    ev_len);
	CopyToTempConst(constbyval,  ev_byval);

	/* We use 0 since varno doesn't matter for Const */
	InitWalkerContext(&wcxt, 0, prel, NULL);
	ranges = walk_expr_tree((Expr *) &temp_const, &wcxt)->rangeset;

	return get_partition_oids(ranges, nparts, prel, false);
}

/*
 * Smart wrapper for scan_result_parts_storage().
 */
ResultRelInfoHolder *
select_partition_for_insert(ResultPartsStorage *parts_storage,
							TupleTableSlot *slot)
{
	PartRelationInfo	   *prel = parts_storage->prel;
	ExprState			   *expr_state = parts_storage->prel_expr_state;
	ExprContext			   *expr_context = parts_storage->prel_econtext;

	Oid						parent_relid = PrelParentRelid(prel),
							partition_relid = InvalidOid;

	Datum					value;
	bool					isnull;
	bool					compute_value = true;

	Oid					   *parts;
	int						nparts;
	ResultRelInfoHolder	   *result;

	do
	{
		if (compute_value)
		{
			/* Prepare expression context */
			ResetExprContext(expr_context);
			expr_context->ecxt_scantuple = slot;

			/* Execute expression */
			value = ExecEvalExprCompat(expr_state, expr_context, &isnull);

			if (isnull)
				elog(ERROR, ERR_PART_ATTR_NULL);

			/* Ok, we have a value */
			compute_value = false;
		}

		/* Search for matching partitions */
		parts = find_partitions_for_value(value, prel->ev_type, prel, &nparts);

		if (nparts > 1)
		{
			elog(ERROR, ERR_PART_ATTR_MULTIPLE);
		}
		else if (nparts == 0)
		{
			partition_relid = create_partitions_for_value(parent_relid,
														  value, prel->ev_type);
		}
		else partition_relid = parts[0];

		/* Get ResultRelationInfo holder for the selected partition */
		result = scan_result_parts_storage(parts_storage, partition_relid);

		/* Somebody has dropped or created partitions */
		if ((nparts == 0 || result == NULL) && !PrelIsFresh(prel))
		{
			/* Try building a new 'prel' for this relation */
			prel = refresh_result_parts_storage(parts_storage, parent_relid);
		}

		/* This partition is a parent itself */
		if (result && result->prel)
		{
			prel = result->prel;
			expr_state = result->prel_expr_state;
			parent_relid = result->partid;
			compute_value = true;

			/* Repeat with a new dispatch */
			result = NULL;
		}

		Assert(prel);
	}
	/* Loop until we get some result */
	while (result == NULL);

	return result;
}

/*
 * Since 13 (e1551f96e64) AttrNumber[] and map_length was combined
 * into one struct AttrMap
 */
static ExprState *
prepare_expr_state(const PartRelationInfo *prel,
				   Relation source_rel,
				   EState *estate)
{
	ExprState	   *expr_state;
	MemoryContext	old_mcxt;
	Node		   *expr;

	/* Make sure we use query memory context */
	old_mcxt = MemoryContextSwitchTo(estate->es_query_cxt);

	/* Fetch partitioning expression (we don't care about varno) */
	expr = PrelExpressionForRelid(prel, PART_EXPR_VARNO);

	/* Should we try using map? */
	if (PrelParentRelid(prel) != RelationGetRelid(source_rel))
	{
#if PG_VERSION_NUM >= 130000
		AttrMap	   *map;
#else
		AttrNumber	   *map;
		int				map_length;
#endif
		TupleDesc		source_tupdesc = RelationGetDescr(source_rel);

		/* Remap expression attributes for source relation */
#if PG_VERSION_NUM >= 130000
		map = PrelExpressionAttributesMap(prel, source_tupdesc);
#else
		map = PrelExpressionAttributesMap(prel, source_tupdesc, &map_length);
#endif

		if (map)
		{
			bool found_whole_row;

#if PG_VERSION_NUM >= 130000
			expr = map_variable_attnos(expr, PART_EXPR_VARNO, 0, map,
											  InvalidOid,
											  &found_whole_row);
#else
			expr = map_variable_attnos_compat(expr, PART_EXPR_VARNO, 0, map,
											  map_length, InvalidOid,
											  &found_whole_row);
#endif

			if (found_whole_row)
				elog(ERROR, "unexpected whole-row reference"
							" found in partition key");

#if PG_VERSION_NUM >= 130000
			free_attrmap(map);
#else
			pfree(map);
#endif
		}
	}

	/* Prepare state for expression execution */
	expr_state = ExecInitExpr((Expr *) expr, NULL);
	MemoryContextSwitchTo(old_mcxt);

	return expr_state;
}

/*
 * --------------------------------
 *  PartitionFilter implementation
 * --------------------------------
 */

Plan *
make_partition_filter(Plan *subplan,
					  Oid parent_relid,
					  Index parent_rti,
					  OnConflictAction conflict_action,
					  CmdType command_type,
					  List *returning_list)
{
	CustomScan *cscan = makeNode(CustomScan);

	/* Currently we don't support ON CONFLICT clauses */
	if (conflict_action != ONCONFLICT_NONE)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("ON CONFLICT clause is not supported with partitioned tables")));

	/* Copy costs etc */
	cscan->scan.plan.startup_cost	= subplan->startup_cost;
	cscan->scan.plan.total_cost		= subplan->total_cost;
	cscan->scan.plan.plan_rows		= subplan->plan_rows;
	cscan->scan.plan.plan_width		= subplan->plan_width;

	/* Setup methods and child plan */
	cscan->methods = &partition_filter_plan_methods;
	cscan->custom_plans = list_make1(subplan);

	/* No physical relation will be scanned */
	cscan->scan.scanrelid = 0;

	/* Build an appropriate target list */
	cscan->scan.plan.targetlist = pfilter_build_tlist(subplan);

	/* Prepare 'custom_scan_tlist' for EXPLAIN (VERBOSE) */
	cscan->custom_scan_tlist = copyObject(cscan->scan.plan.targetlist);
	ChangeVarNodes((Node *) cscan->custom_scan_tlist, INDEX_VAR, parent_rti, 0);

	/* Pack partitioned table's Oid and conflict_action */
	cscan->custom_private = list_make4(makeInteger(parent_relid),
									   makeInteger(conflict_action),
									   returning_list,
									   makeInteger(command_type));

	return &cscan->scan.plan;
}

Node *
partition_filter_create_scan_state(CustomScan *node)
{
	PartitionFilterState *state;

	state = (PartitionFilterState *) palloc0(sizeof(PartitionFilterState));
	NodeSetTag(state, T_CustomScanState);

	/* Initialize base CustomScanState */
	state->css.flags	= node->flags;
	state->css.methods	= &partition_filter_exec_methods;

	/* Extract necessary variables */
	state->subplan				= (Plan *) linitial(node->custom_plans);
	state->partitioned_table	= (Oid) intVal(linitial(node->custom_private));
	state->on_conflict_action	= intVal(lsecond(node->custom_private));
	state->returning_list		= (List *) lthird(node->custom_private);
	state->command_type			= (CmdType) intVal(lfourth(node->custom_private));

	/* Check boundaries */
	Assert(state->on_conflict_action >= ONCONFLICT_NONE ||
		   state->on_conflict_action <= ONCONFLICT_UPDATE);

	/* There should be exactly one subplan */
	Assert(list_length(node->custom_plans) == 1);

	return (Node *) state;
}

void
partition_filter_begin(CustomScanState *node, EState *estate, int eflags)
{
	PartitionFilterState   *state = (PartitionFilterState *) node;
	Oid						parent_relid = state->partitioned_table;
	ResultRelInfo		   *current_rri;

	/* It's convenient to store PlanState in 'custom_ps' */
	node->custom_ps = list_make1(ExecInitNode(state->subplan, estate, eflags));

	/* Fetch current result relation (rri + rel) */
	current_rri = estate->es_result_relation_info;

	/* Init ResultRelInfo cache */
	init_result_parts_storage(&state->result_parts,
							  parent_relid, current_rri,
							  estate, state->command_type,
							  RPS_SKIP_RELATIONS,
							  state->on_conflict_action != ONCONFLICT_NONE,
							  RPS_RRI_CB(prepare_rri_for_insert, state),
							  RPS_RRI_CB(NULL, NULL));
}

TupleTableSlot *
partition_filter_exec(CustomScanState *node)
{
	PartitionFilterState   *state = (PartitionFilterState *) node;

	ExprContext			   *econtext = node->ss.ps.ps_ExprContext;
	EState				   *estate = node->ss.ps.state;
	PlanState			   *child_ps = (PlanState *) linitial(node->custom_ps);
	TupleTableSlot		   *slot;

	slot = ExecProcNode(child_ps);

	if (!TupIsNull(slot))
	{
		MemoryContext			old_mcxt;
		ResultRelInfoHolder	   *rri_holder;
		ResultRelInfo		   *rri;

		/* Switch to per-tuple context */
		old_mcxt = MemoryContextSwitchTo(GetPerTupleMemoryContext(estate));

		/* Search for a matching partition */
		rri_holder = select_partition_for_insert(&state->result_parts, slot);

		/* Switch back and clean up per-tuple context */
		MemoryContextSwitchTo(old_mcxt);
		ResetExprContext(econtext);

		rri = rri_holder->result_rel_info;

		/* Magic: replace parent's ResultRelInfo with ours */
		estate->es_result_relation_info = rri;

		/* If there's a transform map, rebuild the tuple */
		if (rri_holder->tuple_map)
		{
			Relation	child_rel = rri->ri_RelationDesc;

			/* xxx why old code decided to materialize it? */
#if PG_VERSION_NUM < 120000
			HeapTuple	htup_old,
						htup_new;

			htup_old = ExecMaterializeSlot(slot);
			htup_new = do_convert_tuple(htup_old, rri_holder->tuple_map);
			ExecClearTuple(slot);
#endif

			/*
			 * Allocate new slot if needed.
			 * For 12, it is sort of important to create BufferHeapTuple,
			 * though we will store virtual one there. Otherwise, ModifyTable
			 * decides to copy it to mt_scans slot which has tupledesc of
			 * parent.
			 */
			if (!state->tup_convert_slot)
				state->tup_convert_slot = MakeTupleTableSlotCompat(&TTSOpsBufferHeapTuple);

			/* TODO: why should we *always* set a new slot descriptor? */
			ExecSetSlotDescriptor(state->tup_convert_slot, RelationGetDescr(child_rel));
#if PG_VERSION_NUM >= 120000
			slot = execute_attr_map_slot(rri_holder->tuple_map->attrMap, slot, state->tup_convert_slot);
#else
			slot = ExecStoreTuple(htup_new, state->tup_convert_slot, InvalidBuffer, true);
#endif
		}

		return slot;
	}

	return NULL;
}

void
partition_filter_end(CustomScanState *node)
{
	PartitionFilterState   *state = (PartitionFilterState *) node;

	/* Executor will close rels via estate->es_result_relations */
	fini_result_parts_storage(&state->result_parts);

	Assert(list_length(node->custom_ps) == 1);
	ExecEndNode((PlanState *) linitial(node->custom_ps));

	/* Free slot for tuple conversion */
	if (state->tup_convert_slot)
		ExecDropSingleTupleTableSlot(state->tup_convert_slot);
}

void
partition_filter_rescan(CustomScanState *node)
{
	elog(ERROR, "partition_filter_rescan is not implemented");
}

void
partition_filter_explain(CustomScanState *node, List *ancestors, ExplainState *es)
{
	/* Nothing to do here now */
}



/*
 * Build partition filter's target list pointing to subplan tuple's elements.
 */
List *
pfilter_build_tlist(Plan *subplan)
{
	List	   *result_tlist = NIL;
	ListCell   *lc;

	foreach (lc, subplan->targetlist)
	{
		TargetEntry	   *tle = (TargetEntry *) lfirst(lc),
					   *newtle = NULL;

		if (IsA(tle->expr, Const))
		{
			/* TODO: maybe we should use copyObject(tle->expr)? */
			newtle = makeTargetEntry(tle->expr,
									 tle->resno,
									 tle->resname,
									 tle->resjunk);
		}
		else
		{
			Var *var = makeVar(INDEX_VAR,	/* point to subplan's elements */
							   tle->resno,
							   exprType((Node *) tle->expr),
							   exprTypmod((Node *) tle->expr),
							   exprCollation((Node *) tle->expr),
							   0);

			newtle = makeTargetEntry((Expr *) var,
									 tle->resno,
									 tle->resname,
									 tle->resjunk);
		}

		result_tlist = lappend(result_tlist, newtle);
	}

	return result_tlist;
}

/*
 * ----------------------------------------------
 *  Additional init steps for ResultPartsStorage
 * ----------------------------------------------
 */

/* Main trigger */
static void
prepare_rri_for_insert(ResultRelInfoHolder *rri_holder,
					   const ResultPartsStorage *rps_storage)
{
	prepare_rri_returning_for_insert(rri_holder, rps_storage);
	prepare_rri_fdw_for_insert(rri_holder, rps_storage);
}

/* Prepare 'RETURNING *' tlist & projection */
static void
prepare_rri_returning_for_insert(ResultRelInfoHolder *rri_holder,
								 const ResultPartsStorage *rps_storage)
{
	PartitionFilterState   *pfstate;
	List				   *returning_list;
	ResultRelInfo		   *child_rri,
						   *parent_rri;
	Index					parent_rt_idx;
	TupleTableSlot		   *result_slot;

	/* We don't need to do anything ff there's no map */
	if (!rri_holder->tuple_map)
		return;

	pfstate = (PartitionFilterState *) rps_storage->init_rri_holder_cb_arg;
	returning_list = pfstate->returning_list;

	/* Exit if there's no RETURNING list */
	if (!returning_list)
		return;

	child_rri = rri_holder->result_rel_info;
	parent_rri = rps_storage->base_rri;
	parent_rt_idx = parent_rri->ri_RangeTableIndex;

	/* Replace parent's varattnos with child's */
	returning_list = (List *)
			fix_returning_list_mutator((Node *) returning_list,
									   list_make2(makeInteger(parent_rt_idx),
												  rri_holder));

	/* Specify tuple slot where will be place projection result in */
#if PG_VERSION_NUM >= 100000
	result_slot = parent_rri->ri_projectReturning->pi_state.resultslot;
#elif PG_VERSION_NUM >= 90500
	result_slot = parent_rri->ri_projectReturning->pi_slot;
#endif

	/* Build new projection info */
	child_rri->ri_projectReturning =
		ExecBuildProjectionInfoCompat(returning_list, pfstate->css.ss.ps.ps_ExprContext,
									  result_slot, NULL /* HACK: no PlanState */,
									  RelationGetDescr(child_rri->ri_RelationDesc));
}

/* Prepare FDW access structs */
static void
prepare_rri_fdw_for_insert(ResultRelInfoHolder *rri_holder,
						   const ResultPartsStorage *rps_storage)
{
	ResultRelInfo  *rri = rri_holder->result_rel_info;
	FdwRoutine	   *fdw_routine = rri->ri_FdwRoutine;
	Oid				partid;
	EState		   *estate;

	estate = rps_storage->estate;

	/* Nothing to do if not FDW */
	if (fdw_routine == NULL)
		return;

	partid = RelationGetRelid(rri->ri_RelationDesc);

	/* Perform some checks according to 'pg_pathman_insert_into_fdw' */
	switch (pg_pathman_insert_into_fdw)
	{
		case PF_FDW_INSERT_DISABLED:
			elog(ERROR, "INSERTs into FDW partitions are disabled");
			break;

		case PF_FDW_INSERT_POSTGRES:
		case PF_FDW_INSERT_ANY_FDW:
			{
				ForeignDataWrapper *fdw;
				ForeignServer	   *fserver;

				/* Check if it's PostgreSQL FDW */
				fserver = GetForeignServer(GetForeignTable(partid)->serverid);
				fdw = GetForeignDataWrapper(fserver->fdwid);

				/* Show message if not postgres_fdw */
				if (strcmp("postgres_fdw", fdw->fdwname) != 0)
					switch (pg_pathman_insert_into_fdw)
					{
						case PF_FDW_INSERT_POSTGRES:
							elog(ERROR,
								 "FDWs other than postgres_fdw are restricted");

						case PF_FDW_INSERT_ANY_FDW:
							elog(WARNING,
								 "unrestricted FDW mode may lead to crashes");
					}
			}
			break;

		default:
			elog(ERROR, "Mode is not implemented yet");
			break;
	}

	if (fdw_routine->PlanForeignModify)
	{
		RangeTblEntry	   *rte;
		Query				query;
		PlanState			pstate,
						   *pstate_ptr;
		ModifyTableState	mtstate;
		PlannedStmt		   *plan;

		/* This is the value we'd like to get */
		List			   *fdw_private;

		TupleDesc			tupdesc;
		int					target_attr,
							i;

		/* Fetch RangeTblEntry for partition */
		rte = rt_fetch(rri->ri_RangeTableIndex, estate->es_range_table);

		/* Fetch tuple descriptor */
		tupdesc = RelationGetDescr(rri->ri_RelationDesc);

		/* Create fake Query node */
		memset((void *) &query, 0, sizeof(Query));
		NodeSetTag(&query, T_Query);

		query.commandType		= CMD_INSERT;
		query.querySource		= QSRC_ORIGINAL;
		query.resultRelation	= 1;
		query.rtable			= list_make1(copyObject(rte));
		query.jointree			= makeNode(FromExpr);

		query.targetList		= NIL;
		query.returningList		= NIL;

		/* Generate 'query.targetList' using 'tupdesc' */
		target_attr = 1;
		for (i = 0; i < tupdesc->natts; i++)
		{
			Form_pg_attribute	attr;
			TargetEntry		   *te;
			Param			   *param;

			attr = TupleDescAttr(tupdesc, i);

			if (attr->attisdropped)
				continue;

			param = makeNode(Param);
			param->paramkind	= PARAM_EXTERN;
			param->paramid		= target_attr;
			param->paramtype	= attr->atttypid;
			param->paramtypmod	= attr->atttypmod;
			param->paramcollid	= attr->attcollation;
			param->location		= -1;

			te = makeTargetEntry((Expr *) param, target_attr,
								 pstrdup(NameStr(attr->attname)),
								 false);

			query.targetList = lappend(query.targetList, te);

			target_attr++;
		}

		/* HACK: plan a fake query for FDW access to be planned as well */
		elog(DEBUG1, "FDW(%u): plan fake query for fdw_private", partid);
#if PG_VERSION_NUM >= 130000
		plan = standard_planner(&query, NULL, 0, NULL);
#else
		plan = standard_planner(&query, 0, NULL);
#endif

		/* HACK: create a fake PlanState */
		memset(&pstate, 0, sizeof(PlanState));
		pstate.plan = plan->planTree;
		pstate_ptr = &pstate;

		/* HACK: create a fake ModifyTableState */
		memset(&mtstate, 0, sizeof(ModifyTableState));
		NodeSetTag(&mtstate, T_ModifyTableState);
		mtstate.ps.state = estate;
		mtstate.operation = CMD_INSERT;
		mtstate.mt_plans = &pstate_ptr;
		mtstate.mt_nplans = 1;
		mtstate.mt_whichplan = 0;
		mtstate.resultRelInfo = rri;
#if PG_VERSION_NUM < 110000
		mtstate.mt_onconflict = ONCONFLICT_NONE;
#endif

		/* Extract fdw_private from useless plan */
		elog(DEBUG1, "FDW(%u): extract fdw_private", partid);
		fdw_private = linitial(((ModifyTable *) plan->planTree)->fdwPrivLists);

		/* HACK: call BeginForeignModify on 'rri' */
		elog(DEBUG1, "FDW(%u): call BeginForeignModify on a fake INSERT node", partid);
		fdw_routine->BeginForeignModify(&mtstate, rri, fdw_private, 0, 0);

		/* Report success */
		elog(DEBUG1, "FDW(%u): success", partid);
	}
}

/* Make parent's Vars of returninig list point to child's tuple */
static Node *
fix_returning_list_mutator(Node *node, void *state)
{
	if (node == NULL)
		return NULL;

	if (IsA(node, Var))
	{
		/* Extract packed args */
		List				   *state_args = (List *) state;
		Index					parent_idx = intVal(linitial(state_args));
		ResultRelInfoHolder	   *rri_holder = (ResultRelInfoHolder *) lsecond(state_args);
		Var					   *var;

		/* Copy base fields of Var */
		var = (Var *) palloc(sizeof(Var));
		*var = *(Var *) node;

		/* Make Var point to child's attribute */
		if (var->varno == parent_idx &&
			var->varattno >= 0) /* don't change sysattrs! */
		{
			int		i;
			bool	found_mapping = false;

			/* WHOLEROW reference, change row type */
			if (var->varattno == 0)
			{
				Relation child_rel = rri_holder->result_rel_info->ri_RelationDesc;

				/* Assign var->vartype a TupleDesc's type */
				var->vartype = RelationGetDescr(child_rel)->tdtypeid;

				return (Node *) var;
			}

			/* Map: child_att => parent_att, so we have to run through it */
			for (i = 0; i < rri_holder->tuple_map->outdesc->natts; i++)
			{
				/* Good, 'varattno' of parent is child's 'i+1' */
#if PG_VERSION_NUM >= 130000
				if (var->varattno == rri_holder->tuple_map->attrMap->attnums[i])
#else
				if (var->varattno == rri_holder->tuple_map->attrMap[i])
#endif
				{
					var->varattno = i + 1; /* attnos begin with 1 */
					found_mapping = true;
					break;
				}
			}

			/* Swear if we couldn't find mapping for this attribute */
			if (!found_mapping)
				elog(ERROR, "could not bind attribute %d for returning statement",
							var->varattno);
		}

		return (Node *) var;
	}

	return expression_tree_mutator_compat(node, fix_returning_list_mutator, state);
}


/*
 * -------------------------------------
 *  ExecutorState-related modifications
 * -------------------------------------
 */

/* Append RangeTblEntry 'rte' to estate->es_range_table */
static Index
append_rte_to_estate(EState *estate, RangeTblEntry *rte, Relation child_rel)
{
	estate_mod_data	   *emd_struct = fetch_estate_mod_data(estate);

	/* Copy estate->es_range_table if it's first time expansion */
	if (emd_struct->estate_not_modified)
		estate->es_range_table = list_copy(estate->es_range_table);

	estate->es_range_table = lappend(estate->es_range_table, rte);

	/* Update estate_mod_data */
	emd_struct->estate_not_modified = false;

#if PG_VERSION_NUM >= 120000
	estate->es_range_table_size = list_length(estate->es_range_table);
#endif
#if PG_VERSION_NUM >= 120000 && PG_VERSION_NUM < 130000
	/*
	 * On PG = 12, also add rte to es_range_table_array. This is horribly
	 * inefficient, yes.
	 * In 12 es_range_table_array ptr is not saved anywhere in
	 * core, so it is safe to repalloc.
	 *
	 * In >= 13 (3c92658) es_range_table_array was removed
	 */
	estate->es_range_table_array = (RangeTblEntry **)
		repalloc(estate->es_range_table_array,
				 estate->es_range_table_size * sizeof(RangeTblEntry *));
	estate->es_range_table_array[estate->es_range_table_size - 1] = rte;
#endif

#if PG_VERSION_NUM >= 120000
	/*
	 * Also reallocate es_relations, because es_range_table_size defines its
	 * len. This also ensures ExecEndPlan will close the rel.
	 */
	estate->es_relations = (Relation *)
		repalloc(estate->es_relations, estate->es_range_table_size * sizeof(Relation));
	estate->es_relations[estate->es_range_table_size - 1] = child_rel;
#endif

	return list_length(estate->es_range_table);
}

/* Append ResultRelInfo 'rri' to estate->es_result_relations */
static int
append_rri_to_estate(EState *estate, ResultRelInfo *rri)
{
	estate_mod_data	   *emd_struct = fetch_estate_mod_data(estate);
	int					result_rels_allocated = emd_struct->estate_alloc_result_rels;

	/* Reallocate estate->es_result_relations if needed */
	if (result_rels_allocated <= estate->es_num_result_relations)
	{
		ResultRelInfo *rri_array = estate->es_result_relations;

		/* HACK: we can't repalloc or free previous array (there might be users) */
		result_rels_allocated = result_rels_allocated * ALLOC_EXP + 1;
		estate->es_result_relations = palloc(result_rels_allocated *
												sizeof(ResultRelInfo));
		memcpy(estate->es_result_relations,
			   rri_array,
			   estate->es_num_result_relations * sizeof(ResultRelInfo));
	}

	/*
	 * Append ResultRelInfo to 'es_result_relations' array.
	 * NOTE: this is probably safe since ResultRelInfo
	 * contains nothing but pointers to various structs.
	 */
	estate->es_result_relations[estate->es_num_result_relations] = *rri;

	/* Update estate_mod_data */
	emd_struct->estate_alloc_result_rels = result_rels_allocated;
	emd_struct->estate_not_modified = false;

	return estate->es_num_result_relations++;
}


/*
 * --------------------------------------
 *  Store data in 'estate->es_query_cxt'
 * --------------------------------------
 */

/* Used by fetch_estate_mod_data() to find estate_mod_data */
static void
pf_memcxt_callback(void *arg) { elog(DEBUG1, "EState is destroyed"); }

/* Fetch (or create) a estate_mod_data structure we've hidden inside es_query_cxt */
static estate_mod_data *
fetch_estate_mod_data(EState *estate)
{
	MemoryContext			estate_mcxt = estate->es_query_cxt;
	estate_mod_data		   *emd_struct;
	MemoryContextCallback  *cb = estate_mcxt->reset_cbs;

	/* Go through callback list */
	while (cb != NULL)
	{
		/* This is the dummy callback we're looking for! */
		if (cb->func == pf_memcxt_callback)
			return (estate_mod_data *) cb->arg;

		cb = cb->next;
	}

	/* Have to create a new one */
	emd_struct = MemoryContextAlloc(estate_mcxt, sizeof(estate_mod_data));
	emd_struct->estate_not_modified = true;
	emd_struct->estate_alloc_result_rels = estate->es_num_result_relations;

	cb = MemoryContextAlloc(estate_mcxt, sizeof(MemoryContextCallback));
	cb->func = pf_memcxt_callback;
	cb->arg = emd_struct;

	MemoryContextRegisterResetCallback(estate_mcxt, cb);

	return emd_struct;
}
