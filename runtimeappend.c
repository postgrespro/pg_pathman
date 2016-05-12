/* ------------------------------------------------------------------------
 *
 * runtimeappend.c
 *		RuntimeAppend node's function definitions and global variables
 *
 * Copyright (c) 2016, Postgres Professional
 *
 * ------------------------------------------------------------------------
 */
#include "postgres.h"
#include "utils/memutils.h"
#include "runtimeappend.h"
#include "pathman.h"


bool				pg_pathman_enable_runtimeappend = true;

CustomPathMethods	runtimeappend_path_methods;
CustomScanMethods	runtimeappend_plan_methods;
CustomExecMethods	runtimeappend_exec_methods;


Path *
create_runtimeappend_path(PlannerInfo *root,
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
create_runtimeappend_plan(PlannerInfo *root, RelOptInfo *rel,
						  CustomPath *best_path, List *tlist,
						  List *clauses, List *custom_plans)
{
	return create_append_plan_common(root, rel,
									 best_path, tlist,
									 clauses, custom_plans,
									 &runtimeappend_plan_methods);
}

Node *
runtimeappend_create_scan_state(CustomScan *node)
{
	return create_append_scan_state_common(node,
										   &runtimeappend_exec_methods,
										   sizeof(RuntimeAppendState));
}

void
runtimeappend_begin(CustomScanState *node, EState *estate, int eflags)
{
	begin_append_common(node, estate, eflags);
}

static void
fetch_next_tuple(CustomScanState *node)
{
	RuntimeAppendState	   *scan_state = (RuntimeAppendState *) node;
	TupleTableSlot		   *slot = NULL;

	while (scan_state->running_idx < scan_state->ncur_plans)
	{
		ChildScanCommon		child = scan_state->cur_plans[scan_state->running_idx];
		PlanState		   *state = child->content.plan_state;
		bool				quals;

		for (;;)
		{
			slot = ExecProcNode(state);

			if (TupIsNull(slot))
				break;

			node->ss.ps.ps_ExprContext->ecxt_scantuple = slot;
			quals = ExecQual(scan_state->custom_expr_states,
							 node->ss.ps.ps_ExprContext, false);

			ResetExprContext(node->ss.ps.ps_ExprContext);

			if (quals)
			{
				scan_state->slot = slot;
				return;
			}
		}

		scan_state->running_idx++;
	}

	scan_state->slot = slot;
	return;
}

TupleTableSlot *
runtimeappend_exec(CustomScanState *node)
{
	return exec_append_common(node, fetch_next_tuple);
}

void
runtimeappend_end(CustomScanState *node)
{
	end_append_common(node);
}

void
runtimeappend_rescan(CustomScanState *node)
{
	rescan_append_common(node);
}

void
runtimeappend_explain(CustomScanState *node, List *ancestors, ExplainState *es)
{
	RuntimeAppendState *scan_state = (RuntimeAppendState *) node;

	explain_append_common(node, scan_state->children_table, es);
}
