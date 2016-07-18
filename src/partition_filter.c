#include "partition_filter.h"
#include "utils.h"
#include "utils/guc.h"
#include "utils/memutils.h"
#include "nodes/nodeFuncs.h"


bool				pg_pathman_enable_partition_filter = true;

CustomScanMethods	partition_filter_plan_methods;
CustomExecMethods	partition_filter_exec_methods;


static List * pfilter_build_tlist(List *tlist);
static ResultRelInfo * getResultRelInfo(Oid partid, PartitionFilterState *state);

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
	cscan->custom_private = list_make2_int(partitioned_table,
										   conflict_action);

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
	state->onConflictAction = lsecond_int(node->custom_private);

	/* Check boundaries */
	Assert(state->onConflictAction >= ONCONFLICT_NONE ||
		   state->onConflictAction <= ONCONFLICT_UPDATE);

	/* Prepare dummy Const node */
	NodeSetTag(&state->temp_const, T_Const);
	state->temp_const.location = -1;

	return (Node *) state;
}

void
partition_filter_begin(CustomScanState *node, EState *estate, int eflags)
{
	PartitionFilterState   *state = (PartitionFilterState *) node;

	HTAB	   *result_rels_table;
	HASHCTL	   *result_rels_table_config = &state->result_rels_table_config;

	node->custom_ps = list_make1(ExecInitNode(state->subplan, estate, eflags));
	state->prel = get_pathman_relation_info(state->partitioned_table, NULL);
	state->savedRelInfo = NULL;

	memset(result_rels_table_config, 0, sizeof(HASHCTL));
	result_rels_table_config->keysize = sizeof(Oid);
	result_rels_table_config->entrysize = sizeof(ResultRelInfoHandle);

	result_rels_table = hash_create("ResultRelInfo storage", 10,
									result_rels_table_config,
									HASH_ELEM | HASH_BLOBS);

	state->result_rels_table = result_rels_table;
}

TupleTableSlot *
partition_filter_exec(CustomScanState *node)
{
#define CopyToTempConst(const_field, attr_field) \
	( state->temp_const.const_field = \
		slot->tts_tupleDescriptor->attrs[attnum - 1]->attr_field )

	PartitionFilterState   *state = (PartitionFilterState *) node;

	ExprContext			   *econtext = node->ss.ps.ps_ExprContext;
	EState				   *estate = node->ss.ps.state;
	PlanState			   *child_ps = (PlanState *) linitial(node->custom_ps);
	TupleTableSlot		   *slot;

	slot = ExecProcNode(child_ps);

	/* Save original ResultRelInfo */
	if (!state->savedRelInfo)
		state->savedRelInfo = estate->es_result_relation_info;

	if (!TupIsNull(slot))
	{
		MemoryContext   old_cxt;

		List		   *ranges;
		int				nparts;
		Oid			   *parts;
		Oid				selected_partid;

		bool			isnull;
		AttrNumber		attnum = state->prel->attnum;
		Datum			value = slot_getattr(slot, attnum, &isnull);

		/* Fill const with value ... */
		state->temp_const.constvalue = value;
		state->temp_const.constisnull = isnull;

		/* ... and some other important data */
		CopyToTempConst(consttype,   atttypid);
		CopyToTempConst(consttypmod, atttypmod);
		CopyToTempConst(constcollid, attcollation);
		CopyToTempConst(constlen,    attlen);
		CopyToTempConst(constbyval,  attbyval);

		InitWalkerContextCustomNode(&state->wcxt, state->prel, econtext,
									CurrentMemoryContext, true,
									&state->wcxt_cached);

		/* Switch to per-tuple context */
		old_cxt = MemoryContextSwitchTo(econtext->ecxt_per_tuple_memory);

		ranges = walk_expr_tree((Expr *) &state->temp_const, &state->wcxt)->rangeset;
		parts = get_partition_oids(ranges, &nparts, state->prel);


		if (nparts > 1)
			elog(ERROR, "PartitionFilter selected more than one partition");
		else if (nparts == 0)
		{
			selected_partid = create_partitions_bg_worker(state->partitioned_table,
														  state->temp_const.constvalue,
														  state->temp_const.consttype);

			/* Now we have to refresh state->wcxt->ranges manually */
			refresh_walker_context_ranges(&state->wcxt);
		}
		else
			selected_partid = parts[0];

		/* Switch back and clean up per-tuple context */
		MemoryContextSwitchTo(old_cxt);
		ResetExprContext(econtext);

		/* Replace main table with suitable partition */
		estate->es_result_relation_info = getResultRelInfo(selected_partid,
														   state);

		return slot;
	}

	return NULL;
}

void
partition_filter_end(CustomScanState *node)
{
	PartitionFilterState   *state = (PartitionFilterState *) node;

	HASH_SEQ_STATUS			stat;
	ResultRelInfoHandle	   *rri_handle;

	hash_seq_init(&stat, state->result_rels_table);
	while ((rri_handle = (ResultRelInfoHandle *) hash_seq_search(&stat)) != NULL)
	{
		ExecCloseIndices(rri_handle->resultRelInfo);
		heap_close(rri_handle->resultRelInfo->ri_RelationDesc,
				   RowExclusiveLock);
	}
	hash_destroy(state->result_rels_table);

	Assert(list_length(node->custom_ps) == 1);
	ExecEndNode((PlanState *) linitial(node->custom_ps));

	clear_walker_context(&state->wcxt);
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


static ResultRelInfo *
getResultRelInfo(Oid partid, PartitionFilterState *state)
{
#define CopyToResultRelInfo(field_name) \
	( resultRelInfo->field_name = state->savedRelInfo->field_name )

#define ResizeTriggerField(field_name, field_type) \
	do { \
		if (resultRelInfo->field_name) \
			pfree(resultRelInfo->field_name); \
		resultRelInfo->field_name = (field_type *) \
			palloc0(resultRelInfo->ri_TrigDesc->numtriggers * sizeof(field_type)); \
	} while (0)

	ResultRelInfoHandle	   *resultRelInfoHandle;
	bool					found;

	resultRelInfoHandle = hash_search(state->result_rels_table,
									  (const void *) &partid,
									  HASH_ENTER, &found);

	if (!found)
	{
		bool			grown_up;
		ResultRelInfo  *resultRelInfo = (ResultRelInfo *) palloc(sizeof(ResultRelInfo));

		InitResultRelInfo(resultRelInfo,
						  heap_open(partid, RowExclusiveLock),
						  0,
						  state->css.ss.ps.state->es_instrument);

		ExecOpenIndices(resultRelInfo, state->onConflictAction != ONCONFLICT_NONE);

		resultRelInfo->ri_TrigDesc = append_trigger_descs(resultRelInfo->ri_TrigDesc,
														  state->savedRelInfo->ri_TrigDesc,
														  &grown_up);
		if (grown_up)
		{
			ResizeTriggerField(ri_TrigFunctions, FmgrInfo);
			ResizeTriggerField(ri_TrigWhenExprs, List *);

			if (resultRelInfo->ri_TrigInstrument)
			{
				pfree(resultRelInfo->ri_TrigInstrument);

				resultRelInfo->ri_TrigInstrument =
					InstrAlloc(resultRelInfo->ri_TrigDesc->numtriggers,
							   state->css.ss.ps.state->es_instrument);
			}
		}

		/* Copy necessary fields from saved ResultRelInfo */
		CopyToResultRelInfo(ri_WithCheckOptions);
		CopyToResultRelInfo(ri_WithCheckOptionExprs);
		CopyToResultRelInfo(ri_junkFilter);
		CopyToResultRelInfo(ri_projectReturning);
		CopyToResultRelInfo(ri_onConflictSetProj);
		CopyToResultRelInfo(ri_onConflictSetWhere);

		/* ri_ConstraintExprs will be initialized by ExecRelCheck() */
		resultRelInfo->ri_ConstraintExprs = NULL;

		resultRelInfoHandle->partid = partid;
		resultRelInfoHandle->resultRelInfo = resultRelInfo;
	}

	return resultRelInfoHandle->resultRelInfo;
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
 * Add partition filters to ModifyTable node's children
 *
 * 'context' should point to the PlannedStmt->rtable
 */
static void
partition_filter_visitor(Plan *plan, void *context)
{
	List		   *rtable = (List *) context;
	ModifyTable	   *modify_table = (ModifyTable *) plan;
	ListCell	   *lc1,
				   *lc2;

	Assert(rtable && IsA(rtable, List));

	/* Skip if not ModifyTable with 'INSERT' command */
	if (!IsA(modify_table, ModifyTable) || modify_table->operation != CMD_INSERT)
		return;

	forboth (lc1, modify_table->plans, lc2, modify_table->resultRelations)
	{
		Index				rindex = lfirst_int(lc2);
		Oid					relid = getrelid(rindex, rtable);
		PartRelationInfo   *prel = get_pathman_relation_info(relid, NULL);

		/* Check that table is partitioned */
		if (prel)
			lfirst(lc1) = make_partition_filter((Plan *) lfirst(lc1),
												relid,
												modify_table->onConflictAction);
	}
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
