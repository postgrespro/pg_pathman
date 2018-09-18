#include "postgres.h"

#include "partition_overseer.h"
#include "partition_filter.h"
#include "partition_router.h"

CustomScanMethods	partition_overseer_plan_methods;
CustomExecMethods	partition_overseer_exec_methods;

void
init_partition_overseer_static_data(void)
{
	partition_overseer_plan_methods.CustomName 			= OVERSEER_NODE_NAME;
	partition_overseer_plan_methods.CreateCustomScanState = partition_overseer_create_scan_state;

	partition_overseer_exec_methods.CustomName			= OVERSEER_NODE_NAME;
	partition_overseer_exec_methods.BeginCustomScan		= partition_overseer_begin;
	partition_overseer_exec_methods.ExecCustomScan		= partition_overseer_exec;
	partition_overseer_exec_methods.EndCustomScan		= partition_overseer_end;
	partition_overseer_exec_methods.ReScanCustomScan	= partition_overseer_rescan;
	partition_overseer_exec_methods.MarkPosCustomScan	= NULL;
	partition_overseer_exec_methods.RestrPosCustomScan	= NULL;
	partition_overseer_exec_methods.ExplainCustomScan	= partition_overseer_explain;

	RegisterCustomScanMethods(&partition_overseer_plan_methods);
}

Plan *
make_partition_overseer(Plan *subplan)
{
	CustomScan *cscan = makeNode(CustomScan);

	/* Copy costs etc */
	cscan->scan.plan.startup_cost	= subplan->startup_cost;
	cscan->scan.plan.total_cost		= subplan->total_cost;
	cscan->scan.plan.plan_rows		= subplan->plan_rows;
	cscan->scan.plan.plan_width		= subplan->plan_width;

	/* Setup methods, child plan and param number for EPQ */
	cscan->methods = &partition_overseer_plan_methods;
	cscan->custom_plans = list_make1(subplan);
	cscan->custom_private = NIL;

	/* No physical relation will be scanned */
	cscan->scan.scanrelid = 0;

	/* Build an appropriate target list */
	cscan->scan.plan.targetlist = pfilter_build_tlist(subplan);
	cscan->custom_scan_tlist = subplan->targetlist;

	return &cscan->scan.plan;
}


Node *
partition_overseer_create_scan_state(CustomScan *node)
{
	CustomScanState *state = palloc0(sizeof(CustomScanState));
	NodeSetTag(state, T_CustomScanState);

	state->flags = node->flags;
	state->methods = &partition_overseer_exec_methods;

	return (Node *) state;
}

void
partition_overseer_begin(CustomScanState *node,
						 EState *estate,
						 int eflags)
{
	CustomScan *css = (CustomScan *) node->ss.ps.plan;
	Plan	   *plan = linitial(css->custom_plans);

	/* It's convenient to store PlanState in 'custom_ps' */
	node->custom_ps = list_make1(ExecInitNode(plan, estate, eflags));
}

TupleTableSlot *
partition_overseer_exec(CustomScanState *node)
{
	PlanState *state = linitial(node->custom_ps);
	return partition_router_run_modify_table(state);
}

void
partition_overseer_end(CustomScanState *node)
{
	Assert(list_length(node->custom_ps) == 1);
	ExecEndNode((PlanState *) linitial(node->custom_ps));
}

void
partition_overseer_rescan(CustomScanState *node)
{
	elog(ERROR, "partition_overseer_rescan is not implemented");
}

void
partition_overseer_explain(CustomScanState *node,
						   List *ancestors,
						   ExplainState *es)
{
	/* nothing to do */
}
