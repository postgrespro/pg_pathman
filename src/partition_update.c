/* ------------------------------------------------------------------------
 *
 * partition_update.c
 *		Insert row to right partition in UPDATE operation
 *
 * Copyright (c) 2017, Postgres Professional
 *
 * ------------------------------------------------------------------------
 */

#include "partition_filter.h"
#include "partition_update.h"

#include "utils/guc.h"

bool				pg_pathman_enable_partition_update = true;

CustomScanMethods	partition_update_plan_methods;
CustomExecMethods	partition_update_exec_methods;


void
init_partition_update_static_data(void)
{
	partition_update_plan_methods.CustomName 			= "PartitionUpdate";
	partition_update_plan_methods.CreateCustomScanState	= partition_update_create_scan_state;

	partition_update_exec_methods.CustomName			= "PartitionUpdate";
	partition_update_exec_methods.BeginCustomScan		= partition_update_begin;
	partition_update_exec_methods.ExecCustomScan		= partition_update_exec;
	partition_update_exec_methods.EndCustomScan			= partition_update_end;
	partition_update_exec_methods.ReScanCustomScan		= partition_update_rescan;
	partition_update_exec_methods.MarkPosCustomScan		= NULL;
	partition_update_exec_methods.RestrPosCustomScan	= NULL;
	partition_update_exec_methods.ExplainCustomScan		= partition_update_explain;

	DefineCustomBoolVariable("pg_pathman.enable_partitionupdate",
							 "Enables the planner's use of PartitionUpdate custom node.",
							 NULL,
							 &pg_pathman_enable_partition_update,
							 true,
							 PGC_USERSET,
							 0,
							 NULL,
							 NULL,
							 NULL);
}


Plan *
make_partition_update(Plan *subplan,
					  Oid parent_relid,
					  List *returning_list)

{
	Plan		*pfilter;
	CustomScan	*cscan = makeNode(CustomScan);

	/* Copy costs etc */
	cscan->scan.plan.startup_cost = subplan->startup_cost;
	cscan->scan.plan.total_cost = subplan->total_cost;
	cscan->scan.plan.plan_rows = subplan->plan_rows;
	cscan->scan.plan.plan_width = subplan->plan_width;

	/* Setup methods and child plan */
	cscan->methods = &partition_update_plan_methods;
	pfilter = make_partition_filter(subplan, parent_relid, ONCONFLICT_NONE,
									returning_list);
	cscan->custom_plans = list_make1(pfilter);

	/* No physical relation will be scanned */
	cscan->scan.scanrelid = 0;
	cscan->custom_scan_tlist = subplan->targetlist;
	cscan->custom_private = NULL;

	return &cscan->scan.plan;
}

Node *
partition_update_create_scan_state(CustomScan *node)
{
	PartitionUpdateState   *state;

	state = (PartitionUpdateState *) palloc0(sizeof(PartitionUpdateState));
	NodeSetTag(state, T_CustomScanState);

	state->css.flags = node->flags;
	state->css.methods = &partition_update_exec_methods;

	/* Extract necessary variables */
	state->subplan = (Plan *) linitial(node->custom_plans);
	return (Node *) state;
}

void
partition_update_begin(CustomScanState *node, EState *estate, int eflags)
{
	PartitionUpdateState   *state = (PartitionUpdateState *) node;

	/* Initialize PartitionFilter child node */
	node->custom_ps = list_make1(ExecInitNode(state->subplan, estate, eflags));
}

TupleTableSlot *
partition_update_exec(CustomScanState *node)
{
	PartitionFilterState	*state = (PartitionFilterState *) node;
	PlanState				*child_ps = (PlanState *) linitial(node->custom_ps);
	EState					*estate = node->ss.ps.state;
	TupleTableSlot			*slot;
	ResultRelInfo			*saved_rel_info;

	/* save original ResultRelInfo */
	saved_rel_info = estate->es_result_relation_info;

	slot = ExecProcNode(child_ps);
	if (!TupIsNull(slot))
	{
		/* we got the slot that can be inserted to child partition */
		return slot;
	}

	return NULL;
}

void
partition_update_end(CustomScanState *node)
{
	PartitionUpdateState   *state = (PartitionUpdateState *) node;

	Assert(list_length(node->custom_ps) == 1);
	ExecEndNode((PlanState *) linitial(node->custom_ps));
}

void
partition_update_rescan(CustomScanState *node)
{
	Assert(list_length(node->custom_ps) == 1);
	ExecReScan((PlanState *) linitial(node->custom_ps));
}

void
partition_update_explain(CustomScanState *node, List *ancestors, ExplainState *es)
{
	/* Nothing to do here now */
}
