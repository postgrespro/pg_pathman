#include "partition_filter.h"
#include "utils/guc.h"
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
make_partition_filter_plan(Plan *subplan, Oid partitioned_table,
						   OnConflictAction	conflict_action)
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
	PartitionFilterState   *state = palloc0(sizeof(PartitionFilterState));

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

	EState				   *estate = node->ss.ps.state;
	PlanState			   *child_ps = (PlanState *) linitial(node->custom_ps);
	TupleTableSlot		   *slot;

	slot = ExecProcNode(child_ps);

	if (!TupIsNull(slot))
	{
		WalkerContext	wcxt;
		List		   *ranges;
		int				nparts;
		Oid			   *parts;

		bool			isnull;
		AttrNumber		attnum = state->prel->attnum;
		Datum			value = slot_getattr(slot, attnum, &isnull);

		state->temp_const.constvalue = value;
		state->temp_const.constisnull = isnull;

		CopyToTempConst(consttype,   atttypid);
		CopyToTempConst(consttypmod, atttypmod);
		CopyToTempConst(constcollid, attcollation);
		CopyToTempConst(constlen,    attlen);
		CopyToTempConst(constbyval,  attbyval);

		wcxt.prel = state->prel;
		wcxt.econtext = NULL;
		wcxt.hasLeast = false;
		wcxt.hasGreatest = false;

		ranges = walk_expr_tree((Expr *) &state->temp_const, &wcxt)->rangeset;
		parts = get_partition_oids(ranges, &nparts, state->prel);
		Assert(nparts == 1); /* there has to be only 1 partition */

		estate->es_result_relation_info = getResultRelInfo(parts[0], state);

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
	ResultRelInfoHandle	   *resultRelInfoHandle;
	bool					found;

	resultRelInfoHandle = hash_search(state->result_rels_table,
									  (const void *) &partid,
									  HASH_ENTER, &found);

	if (!found)
	{
		ResultRelInfo *resultRelInfo = (ResultRelInfo *) palloc(sizeof(ResultRelInfo));
		InitResultRelInfo(resultRelInfo,
						  heap_open(partid, RowExclusiveLock),
						  0,
						  state->css.ss.ps.state->es_instrument);

		ExecOpenIndices(resultRelInfo, state->onConflictAction != ONCONFLICT_NONE);

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

/* Add proxy PartitionFilter nodes to subplans of ModifyTable node */
void
add_partition_filters(List *rtable, ModifyTable *modify_table)
{
	ListCell *lc1,
			 *lc2;

	Assert(IsA(modify_table, ModifyTable));

	if (!pg_pathman_enable_partition_filter)
		return;

	forboth (lc1, modify_table->plans, lc2, modify_table->resultRelations)
	{
		Index				rindex = lfirst_int(lc2);
		Oid					relid = getrelid(rindex, rtable);
		PartRelationInfo   *prel = get_pathman_relation_info(relid, NULL);

		if (prel)
			lfirst(lc1) = make_partition_filter_plan((Plan *) lfirst(lc1),
													 relid,
													 modify_table->onConflictAction);
	}
}
