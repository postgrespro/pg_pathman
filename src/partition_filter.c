#include "partition_filter.h"
#include "utils/guc.h"


bool				pg_pathman_enable_partition_filter = true;

CustomScanMethods	partition_filter_plan_methods;
CustomExecMethods	partition_filter_exec_methods;



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
create_partition_filter_plan(Plan *subplan, PartRelationInfo *prel)
{
	CustomScan *cscan = makeNode(CustomScan);

	cscan->scan.plan.startup_cost = subplan->startup_cost;
	cscan->scan.plan.total_cost = subplan->total_cost;
	cscan->scan.plan.plan_rows = subplan->plan_rows;
	cscan->scan.plan.plan_width = subplan->plan_width;

	cscan->scan.plan.qual = NIL;

	cscan->custom_plans = list_make1(subplan);

	cscan->scan.plan.targetlist = subplan->targetlist;

	/* No relation will be scanned */
	cscan->scan.scanrelid = 0;
	cscan->custom_scan_tlist = subplan->targetlist;

	cscan->methods = &partition_filter_plan_methods;

	return &cscan->scan.plan;
}

Node *
partition_filter_create_scan_state(CustomScan *node)
{
	PartitionFilterState *state = palloc0(sizeof(PartitionFilterState));

	NodeSetTag(state, T_CustomScanState);

	state->css.flags = node->flags;
	state->css.methods = &partition_filter_exec_methods;

	state->subplan = (Plan *) linitial(node->custom_plans);

	return (Node *) state;
}

void
partition_filter_begin(CustomScanState *node, EState *estate, int eflags)
{
	PartitionFilterState *state = (PartitionFilterState *) node;

	node->custom_ps = list_make1(ExecInitNode(state->subplan, estate, eflags));

	state->firstStart = true;
}

TupleTableSlot *
partition_filter_exec(CustomScanState *node)
{
	PartitionFilterState *state = (PartitionFilterState *) node;

	EState			   *estate = node->ss.ps.state;
	PlanState		   *child_ps = (PlanState *) linitial(node->custom_ps);
	TupleTableSlot	   *slot;

	if (state->firstStart)
		state->savedRelInfo = estate->es_result_relation_info;

	slot = ExecProcNode(child_ps);

	if (!TupIsNull(slot))
	{
		/* estate->es_result_relation_info = NULL; */

		return slot;
	}

	return NULL;
}

void
partition_filter_end(CustomScanState *node)
{
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
		PartRelationInfo   *prel = get_pathman_relation_info(getrelid(rindex, rtable),
															 NULL);
		if (prel)
			lfirst(lc1) = create_partition_filter_plan((Plan *) lfirst(lc1), prel);
	}
}
