#include "postgres.h"

#include "partition_filter.h"
#include "partition_overseer.h"
#include "partition_router.h"
#include "planner_tree_modification.h"

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

static void
set_mt_state_for_router(PlanState *state, void *context)
{
#if PG_VERSION_NUM < 140000
	int					i;
#endif
	ModifyTableState   *mt_state = (ModifyTableState *) state;

	if (!IsA(state, ModifyTableState))
		return;

#if PG_VERSION_NUM >= 140000
	/* Fields "mt_plans", "mt_nplans" removed in 86dc90056dfd */
	{
		CustomScanState *pf_state = (CustomScanState *) outerPlanState(mt_state);
#else
	for (i = 0; i < mt_state->mt_nplans; i++)
	{
		CustomScanState *pf_state = (CustomScanState *) mt_state->mt_plans[i];
#endif
		/* Check if this is a PartitionFilter + PartitionRouter combo */
		if (IsPartitionFilterState(pf_state))
		{
			PartitionRouterState *pr_state = linitial(pf_state->custom_ps);
			if (IsPartitionRouterState(pr_state))
			{
				/* HACK: point to ModifyTable in PartitionRouter */
				pr_state->mt_state = mt_state;
			}
		}
	}
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

	/* Save ModifyTableState in PartitionRouterState structs */
	state_tree_visitor((PlanState *) linitial(node->custom_ps),
						set_mt_state_for_router,
						NULL);
}

TupleTableSlot *
partition_overseer_exec(CustomScanState *node)
{
	ModifyTableState *mt_state = linitial(node->custom_ps);

	TupleTableSlot	   *slot;
	int					mt_plans_old,
						mt_plans_new;

	/* Get initial signal */
#if PG_VERSION_NUM >= 140000 /* field "mt_nplans" removed in 86dc90056dfd */
	mt_plans_old = mt_state->mt_nrels;
#else
	mt_plans_old = mt_state->mt_nplans;
#endif

restart:
	/* Run ModifyTable */
	slot = ExecProcNode((PlanState *) mt_state);

	/* Get current signal */
#if PG_VERSION_NUM >= 140000 /* field "mt_nplans" removed in 86dc90056dfd */
	mt_plans_new = MTHackField(mt_state, mt_nrels);
#else
	mt_plans_new = MTHackField(mt_state, mt_nplans);
#endif

	/* Did PartitionRouter ask us to restart? */
	if (mt_plans_new != mt_plans_old)
	{
		/* Signal points to current plan */
#if PG_VERSION_NUM < 140000
		int state_idx = -mt_plans_new;
#endif

		/* HACK: partially restore ModifyTable's state */
		MTHackField(mt_state, mt_done) = false;
#if PG_VERSION_NUM >= 140000
		/* Fields "mt_nplans", "mt_whichplan" removed in 86dc90056dfd */
		MTHackField(mt_state, mt_nrels) = mt_plans_old;
#else
		MTHackField(mt_state, mt_nplans) = mt_plans_old;
		MTHackField(mt_state, mt_whichplan) = state_idx;
#endif

		/* Rerun ModifyTable */
		goto restart;
	}

	return slot;
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
	/* Nothing to do here now */
}
