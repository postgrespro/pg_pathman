/* ------------------------------------------------------------------------
 *
 * runtimeappend.c
 *		RuntimeAppend node's function definitions and global variables
 *
 * Copyright (c) 2016, Postgres Professional
 *
 * ------------------------------------------------------------------------
 */

#include "compat/pg_compat.h"

#include "runtime_append.h"

#include "utils/guc.h"


bool				pg_pathman_enable_runtimeappend = true;

CustomPathMethods	runtimeappend_path_methods;
CustomScanMethods	runtimeappend_plan_methods;
CustomExecMethods	runtimeappend_exec_methods;


void
init_runtime_append_static_data(void)
{
	runtimeappend_path_methods.CustomName				= RUNTIME_APPEND_NODE_NAME;
	runtimeappend_path_methods.PlanCustomPath			= create_runtime_append_plan;

	runtimeappend_plan_methods.CustomName 				= RUNTIME_APPEND_NODE_NAME;
	runtimeappend_plan_methods.CreateCustomScanState	= runtime_append_create_scan_state;

	runtimeappend_exec_methods.CustomName				= RUNTIME_APPEND_NODE_NAME;
	runtimeappend_exec_methods.BeginCustomScan			= runtime_append_begin;
	runtimeappend_exec_methods.ExecCustomScan			= runtime_append_exec;
	runtimeappend_exec_methods.EndCustomScan			= runtime_append_end;
	runtimeappend_exec_methods.ReScanCustomScan			= runtime_append_rescan;
	runtimeappend_exec_methods.MarkPosCustomScan		= NULL;
	runtimeappend_exec_methods.RestrPosCustomScan		= NULL;
	runtimeappend_exec_methods.ExplainCustomScan		= runtime_append_explain;

	DefineCustomBoolVariable("pg_pathman.enable_runtimeappend",
							 "Enables the planner's use of " RUNTIME_APPEND_NODE_NAME " custom node.",
							 NULL,
							 &pg_pathman_enable_runtimeappend,
							 true,
							 PGC_USERSET,
							 0,
							 NULL,
							 NULL,
							 NULL);

	RegisterCustomScanMethods(&runtimeappend_plan_methods);
}

Path *
create_runtime_append_path(PlannerInfo *root,
						   AppendPath *inner_append,
						   ParamPathInfo *param_info,
						   double sel)
{
	return create_append_path_common(root, inner_append,
									 param_info,
									 &runtimeappend_path_methods,
									 sizeof(RuntimeAppendPath),
									 sel);
}

Plan *
create_runtime_append_plan(PlannerInfo *root, RelOptInfo *rel,
						   CustomPath *best_path, List *tlist,
						   List *clauses, List *custom_plans)
{
	return create_append_plan_common(root, rel,
									 best_path, tlist,
									 clauses, custom_plans,
									 &runtimeappend_plan_methods);
}

Node *
runtime_append_create_scan_state(CustomScan *node)
{
	return create_append_scan_state_common(node,
										   &runtimeappend_exec_methods,
										   sizeof(RuntimeAppendState));
}

void
runtime_append_begin(CustomScanState *node, EState *estate, int eflags)
{
	begin_append_common(node, estate, eflags);
}

static void
fetch_next_tuple(CustomScanState *node)
{
	RuntimeAppendState	   *scan_state = (RuntimeAppendState *) node;

	while (scan_state->running_idx < scan_state->ncur_plans)
	{
		ChildScanCommon		child = scan_state->cur_plans[scan_state->running_idx];
		PlanState		   *state = child->content.plan_state;

		for (;;)
		{
			TupleTableSlot *slot = ExecProcNode(state);

			if (TupIsNull(slot))
				break;

			scan_state->slot = slot;
			return;
		}

		scan_state->running_idx++;
	}

	scan_state->slot = NULL;
}

TupleTableSlot *
runtime_append_exec(CustomScanState *node)
{
	return exec_append_common(node, fetch_next_tuple);
}

void
runtime_append_end(CustomScanState *node)
{
	end_append_common(node);
}

void
runtime_append_rescan(CustomScanState *node)
{
	rescan_append_common(node);
}

void
runtime_append_explain(CustomScanState *node, List *ancestors, ExplainState *es)
{
	RuntimeAppendState *scan_state = (RuntimeAppendState *) node;

	explain_append_common(node, ancestors, es,
						  scan_state->children_table,
						  scan_state->custom_exprs);
}
