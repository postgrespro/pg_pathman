/* ------------------------------------------------------------------------
 *
 * partition_filter.c
 *		Select partition for INSERT operation
 *
 * Copyright (c) 2016, Postgres Professional
 *
 * ------------------------------------------------------------------------
 */

#include "partition_filter.h"
#include "nodes_common.h"
#include "utils.h"
#include "init.h"

#include "utils/guc.h"
#include "utils/memutils.h"
#include "nodes/nodeFuncs.h"
#include "utils/lsyscache.h"


#define ALLOC_EXP	2


bool				pg_pathman_enable_partition_filter = true;

CustomScanMethods	partition_filter_plan_methods;
CustomExecMethods	partition_filter_exec_methods;


static void partition_filter_visitor(Plan *plan, void *context);
static List * pfilter_build_tlist(List *tlist);
static int append_rri_to_estate(EState *estate, ResultRelInfo *rri, int cur_allocated);


void
init_partition_filter_static_data(void)
{
	partition_filter_plan_methods.CustomName 			= "PartitionFilter";
	partition_filter_plan_methods.CreateCustomScanState	= partition_filter_create_scan_state;

	partition_filter_exec_methods.CustomName			= "PartitionFilter";
	partition_filter_exec_methods.BeginCustomScan		= partition_filter_begin;
	partition_filter_exec_methods.ExecCustomScan		= partition_filter_exec;
	partition_filter_exec_methods.EndCustomScan			= partition_filter_end;
	partition_filter_exec_methods.ReScanCustomScan		= partition_filter_rescan;
	partition_filter_exec_methods.MarkPosCustomScan		= NULL;
	partition_filter_exec_methods.RestrPosCustomScan	= NULL;
	partition_filter_exec_methods.ExplainCustomScan		= partition_filter_explain;

	DefineCustomBoolVariable("pg_pathman.enable_partitionfilter",
							 "Enables the planner's use of PartitionFilter custom node.",
							 NULL,
							 &pg_pathman_enable_partition_filter,
							 true,
							 PGC_USERSET,
							 0,
							 NULL,
							 NULL,
							 NULL);
}


/*
 * Add PartitionFilter nodes to the plan tree
 */
void
add_partition_filters(List *rtable, Plan *plan)
{
	if (pg_pathman_enable_partition_filter)
		plan_tree_walker(plan, partition_filter_visitor, rtable);
}

/*
 * This callback adds a new RangeTblEntry once
 * partition is opened for an INSERT.
 */
void
check_acl_for_partition(EState *estate,
						ResultRelInfoHolder *rri_holder,
						void *arg)
{
	RangeTblEntry  *rte;
	Relation		part_rel = rri_holder->result_rel_info->ri_RelationDesc;

	rte = makeNode(RangeTblEntry);

	rte->rtekind = RTE_RELATION;
	rte->relid = rri_holder->partid;
	rte->relkind = part_rel->rd_rel->relkind;
	rte->requiredPerms = ACL_INSERT;

	/* FIXME: Check permissions for partition */
	ExecCheckRTPerms(list_make1(rte), true);

	/* TODO: append RTE to estate->es_range_table */
}


/*
 * Initialize ResultPartsStorage (hash table etc).
 */
void
init_result_parts_storage(ResultPartsStorage *parts_storage,
						  EState *estate,
						  bool speculative_inserts,
						  Size table_entry_size,
						  on_new_rri_holder on_new_rri_holder_cb,
						  void *on_new_rri_holder_cb_arg)
{
	HASHCTL *result_rels_table_config = &parts_storage->result_rels_table_config;

	memset(result_rels_table_config, 0, sizeof(HASHCTL));
	result_rels_table_config->keysize = sizeof(Oid);

	/* Use sizeof(ResultRelInfoHolder) if table_entry_size is 0 */
	if (table_entry_size == ResultPartsStorageStandard)
		result_rels_table_config->entrysize = sizeof(ResultRelInfoHolder);
	else
		result_rels_table_config->entrysize = table_entry_size;

	parts_storage->result_rels_table = hash_create("ResultRelInfo storage", 10,
												   result_rels_table_config,
												   HASH_ELEM | HASH_BLOBS);
	parts_storage->estate = estate;
	parts_storage->es_alloc_result_rels = estate->es_num_result_relations;
	parts_storage->speculative_inserts = speculative_inserts;
	parts_storage->saved_rel_info = NULL;

	parts_storage->on_new_rri_holder_callback = on_new_rri_holder_cb;
	parts_storage->callback_arg = on_new_rri_holder_cb_arg;

	/* Partitions must remain locked till transaction's end */
	parts_storage->head_open_lock_mode = RowExclusiveLock;
	parts_storage->heap_close_lock_mode = NoLock;
}

/*
 * Free ResultPartsStorage (close relations etc).
 */
void
fini_result_parts_storage(ResultPartsStorage *parts_storage, bool close_rels)
{
	/* Close partitions and their indices if asked to */
	if (close_rels)
	{
		HASH_SEQ_STATUS			stat;
		ResultRelInfoHolder	   *rri_holder; /* ResultRelInfo holder */

		hash_seq_init(&stat, parts_storage->result_rels_table);
		while ((rri_holder = (ResultRelInfoHolder *) hash_seq_search(&stat)) != NULL)
		{
			ExecCloseIndices(rri_holder->result_rel_info);

			heap_close(rri_holder->result_rel_info->ri_RelationDesc,
					   parts_storage->heap_close_lock_mode);
		}
	}

	/* Finally destroy hash table */
	hash_destroy(parts_storage->result_rels_table);
}

/*
 * Find a ResultRelInfo for the partition using ResultPartsStorage.
 */
ResultRelInfoHolder *
scan_result_parts_storage(Oid partid, ResultPartsStorage *parts_storage)
{
#define CopyToResultRelInfo(field_name) \
	( part_result_rel_info->field_name = parts_storage->saved_rel_info->field_name )

	ResultRelInfoHolder	   *rri_holder;
	bool					found;

	rri_holder = hash_search(parts_storage->result_rels_table,
							 (const void *) &partid,
							 HASH_ENTER, &found);

	/* If not found, create & cache new ResultRelInfo */
	if (!found)
	{
		ResultRelInfo *part_result_rel_info = makeNode(ResultRelInfo);

		/* Check that 'saved_rel_info' is set */
		if (!parts_storage->saved_rel_info)
			elog(ERROR, "ResultPartsStorage contains no saved_rel_info");

		InitResultRelInfo(part_result_rel_info,
						  heap_open(partid, parts_storage->head_open_lock_mode),
						  parts_storage->saved_rel_info->ri_RangeTableIndex,
						  0); /* TODO: select suitable options */

		ExecOpenIndices(part_result_rel_info, parts_storage->speculative_inserts);

		/* Copy necessary fields from saved ResultRelInfo */
		CopyToResultRelInfo(ri_WithCheckOptions);
		CopyToResultRelInfo(ri_WithCheckOptionExprs);
		CopyToResultRelInfo(ri_junkFilter);
		CopyToResultRelInfo(ri_projectReturning);
		CopyToResultRelInfo(ri_onConflictSetProj);
		CopyToResultRelInfo(ri_onConflictSetWhere);

		/* ri_ConstraintExprs will be initialized by ExecRelCheck() */
		part_result_rel_info->ri_ConstraintExprs = NULL;

		/* Now fill the ResultRelInfo holder */
		rri_holder->partid = partid;
		rri_holder->result_rel_info = part_result_rel_info;

		/* Add ResultRelInfo to storage->es_alloc_result_rels */
		parts_storage->es_alloc_result_rels =
				append_rri_to_estate(parts_storage->estate,
									 part_result_rel_info,
									 parts_storage->es_alloc_result_rels);

		/* Call on_new_rri_holder_callback() if needed */
		if (parts_storage->on_new_rri_holder_callback)
			parts_storage->on_new_rri_holder_callback(parts_storage->estate,
													  rri_holder,
													  parts_storage->callback_arg);
	}

	return rri_holder;
}


/*
 * Find matching partitions for 'value' using PartRelationInfo.
 */
Oid *
find_partitions_for_value(Datum value, const PartRelationInfo *prel,
						  ExprContext *econtext, int *nparts)
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
	temp_const.constvalue = value;
	temp_const.constisnull = false;

	/* ... and some other important data */
	CopyToTempConst(consttype,   atttype);
	CopyToTempConst(consttypmod, atttypmod);
	CopyToTempConst(constcollid, attcollid);
	CopyToTempConst(constlen,    attlen);
	CopyToTempConst(constbyval,  attbyval);

	InitWalkerContext(&wcxt, prel, econtext, true);
	ranges = walk_expr_tree((Expr *) &temp_const, &wcxt)->rangeset;
	return get_partition_oids(ranges, nparts, prel, false);
}


Plan *
make_partition_filter(Plan *subplan, Oid partitioned_table,
					  OnConflictAction conflict_action)
{
	CustomScan *cscan = makeNode(CustomScan);

	cscan->scan.plan.startup_cost = subplan->startup_cost;
	cscan->scan.plan.total_cost = subplan->total_cost;
	cscan->scan.plan.plan_rows = subplan->plan_rows;
	cscan->scan.plan.plan_width = subplan->plan_width;

	cscan->methods = &partition_filter_plan_methods;
	cscan->custom_plans = list_make1(subplan);

	cscan->scan.plan.targetlist = pfilter_build_tlist(subplan->targetlist);

	/* No relation will be scanned */
	cscan->scan.scanrelid = 0;
	cscan->custom_scan_tlist = subplan->targetlist;

	/* Pack partitioned table's Oid and conflict_action */
	cscan->custom_private = list_make2_int(partitioned_table, conflict_action);

	return &cscan->scan.plan;
}

Node *
partition_filter_create_scan_state(CustomScan *node)
{
	PartitionFilterState   *state;

	state = (PartitionFilterState *) palloc0(sizeof(PartitionFilterState));
	NodeSetTag(state, T_CustomScanState);

	state->css.flags = node->flags;
	state->css.methods = &partition_filter_exec_methods;

	/* Extract necessary variables */
	state->subplan = (Plan *) linitial(node->custom_plans);
	state->partitioned_table = linitial_int(node->custom_private);
	state->on_conflict_action = lsecond_int(node->custom_private);

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

	/* It's convenient to store PlanState in 'custom_ps' */
	node->custom_ps = list_make1(ExecInitNode(state->subplan, estate, eflags));

	/* Init ResultRelInfo cache */
	init_result_parts_storage(&state->result_parts, estate,
							  state->on_conflict_action != ONCONFLICT_NONE,
							  ResultPartsStorageStandard,
							  check_acl_for_partition, NULL);

	state->warning_triggered = false;
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

	/* Save original ResultRelInfo */
	if (!state->result_parts.saved_rel_info)
		state->result_parts.saved_rel_info = estate->es_result_relation_info;

	if (!TupIsNull(slot))
	{
		const PartRelationInfo *prel;

		MemoryContext			old_cxt;

		ResultRelInfoHolder	   *result_part_holder;
		Oid						selected_partid;
		int						nparts;
		Oid					   *parts;

		bool					isnull;
		Datum					value;

		/* Fetch PartRelationInfo for this partitioned relation */
		prel = get_pathman_relation_info(state->partitioned_table);
		if (!prel)
		{
			if (!state->warning_triggered)
				elog(WARNING, "Relation \"%s\" is not partitioned, "
							  "PartitionFilter will behave as a normal INSERT",
					 get_rel_name_or_relid(state->partitioned_table));

			return slot;
		}

		/* Extract partitioned column's value (also check types) */
		Assert(slot->tts_tupleDescriptor->
					attrs[prel->attnum - 1]->atttypid == prel->atttype);
		value = slot_getattr(slot, prel->attnum, &isnull);
		if (isnull)
			elog(ERROR, "partitioned column's value should not be NULL");

		/* Switch to per-tuple context */
		old_cxt = MemoryContextSwitchTo(GetPerTupleMemoryContext(estate));

		/* Search for matching partitions */
		parts = find_partitions_for_value(value, prel, econtext, &nparts);

		if (nparts > 1)
			elog(ERROR, "PartitionFilter selected more than one partition");
		else if (nparts == 0)
		{
			/*
			 * If auto partition propagation is enabled then try to create
			 * new partitions for the key
			 */
			if (prel->auto_partition && IsAutoPartitionEnabled())
			{
				selected_partid = create_partitions(state->partitioned_table,
													value, prel->atttype);

				/* get_pathman_relation_info() will refresh this entry */
				invalidate_pathman_relation_info(state->partitioned_table, NULL);
			}
			else
				elog(ERROR,
					 "There is no suitable partition for key '%s'",
					 datum_to_cstring(value, prel->atttype));
		}
		else
			selected_partid = parts[0];

		/* Switch back and clean up per-tuple context */
		MemoryContextSwitchTo(old_cxt);
		ResetExprContext(econtext);

		/* Replace parent table with a suitable partition */
		old_cxt = MemoryContextSwitchTo(estate->es_query_cxt);
		result_part_holder = scan_result_parts_storage(selected_partid,
													   &state->result_parts);
		estate->es_result_relation_info = result_part_holder->result_rel_info;
		MemoryContextSwitchTo(old_cxt);

		return slot;
	}

	return NULL;
}

void
partition_filter_end(CustomScanState *node)
{
	PartitionFilterState   *state = (PartitionFilterState *) node;

	/* Executor will close rels via estate->es_result_relations */
	fini_result_parts_storage(&state->result_parts, false);

	Assert(list_length(node->custom_ps) == 1);
	ExecEndNode((PlanState *) linitial(node->custom_ps));
}

void
partition_filter_rescan(CustomScanState *node)
{
	Assert(list_length(node->custom_ps) == 1);
	ExecReScan((PlanState *) linitial(node->custom_ps));
}

void
partition_filter_explain(CustomScanState *node, List *ancestors, ExplainState *es)
{
	/* Nothing to do here now */
}

static int
append_rri_to_estate(EState *estate, ResultRelInfo *rri, int cur_allocated)
{
	int result_rels_allocated = cur_allocated;

	if (result_rels_allocated <= estate->es_num_result_relations)
	{
		ResultRelInfo *rri_array = estate->es_result_relations;

		result_rels_allocated = result_rels_allocated * ALLOC_EXP + 1;
		estate->es_result_relations = palloc(result_rels_allocated *
												sizeof(ResultRelInfo));
		memcpy(estate->es_result_relations,
			   rri_array,
			   estate->es_num_result_relations * sizeof(ResultRelInfo));
	}

	/* Append ResultRelInfo to 'es_result_relations' array */
	estate->es_result_relations[estate->es_num_result_relations++] = *rri;

	return result_rels_allocated;
}

/*
 * Build partition filter's target list pointing to subplan tuple's elements
 */
static List *
pfilter_build_tlist(List *tlist)
{
	List	   *result_tlist = NIL;
	ListCell   *lc;
	int			i = 1;

	foreach (lc, tlist)
	{
		TargetEntry *tle = (TargetEntry *) lfirst(lc);

		Var *var = makeVar(INDEX_VAR,	/* point to subplan's elements */
						   i,			/* direct attribute mapping */
						   exprType((Node *) tle->expr),
						   exprTypmod((Node *) tle->expr),
						   exprCollation((Node *) tle->expr),
						   0);

		result_tlist = lappend(result_tlist,
							   makeTargetEntry((Expr *) var,
											   i,
											   NULL,
											   tle->resjunk));
		i++; /* next resno */
	}

	return result_tlist;
}

/*
 * Add partition filters to ModifyTable node's children.
 *
 * 'context' should point to the PlannedStmt->rtable.
 */
static void
partition_filter_visitor(Plan *plan, void *context)
{
	List		   *rtable = (List *) context;
	ModifyTable	   *modify_table = (ModifyTable *) plan;
	ListCell	   *lc1,
				   *lc2;

	/* Skip if not ModifyTable with 'INSERT' command */
	if (!IsA(modify_table, ModifyTable) || modify_table->operation != CMD_INSERT)
		return;

	Assert(rtable && IsA(rtable, List));

	forboth (lc1, modify_table->plans, lc2, modify_table->resultRelations)
	{
		Index					rindex = lfirst_int(lc2);
		Oid						relid = getrelid(rindex, rtable);
		const PartRelationInfo *prel = get_pathman_relation_info(relid);

		/* Check that table is partitioned */
		if (prel)
			lfirst(lc1) = make_partition_filter((Plan *) lfirst(lc1),
												relid,
												modify_table->onConflictAction);
	}
}
