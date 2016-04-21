/* ------------------------------------------------------------------------
 *
 * pickyappend.c
 *		PickyAppend node's function definitions and global variables
 *
 * Copyright (c) 2016, Postgres Professional
 *
 * ------------------------------------------------------------------------
 */
#include "postgres.h"
#include "pickyappend.h"
#include "pathman.h"


bool				pg_pathman_enable_pickyappend = true;

CustomPathMethods	pickyappend_path_methods;
CustomScanMethods	pickyappend_plan_methods;
CustomExecMethods	pickyappend_exec_methods;


Path *
create_pickyappend_path(PlannerInfo *root,
						AppendPath *inner_append,
						ParamPathInfo *param_info,
						List *picky_clauses)
{
	return create_append_path_common(root, inner_append,
									 param_info, picky_clauses,
									 &pickyappend_path_methods);
}

Plan *
create_pickyappend_plan(PlannerInfo *root, RelOptInfo *rel,
						CustomPath *best_path, List *tlist,
						List *clauses, List *custom_plans)
{
	return create_append_plan_common(root, rel,
									 best_path, tlist,
									 clauses, custom_plans,
									 &pickyappend_plan_methods);
}

Node *
pickyappend_create_scan_state(CustomScan *node)
{
	return create_append_scan_state_common(node,
										   &pickyappend_exec_methods,
										   sizeof(PickyAppendState));
}

void
pickyappend_begin(CustomScanState *node, EState *estate, int eflags)
{
	begin_append_common(node, estate, eflags);
}

TupleTableSlot *
pickyappend_exec(CustomScanState *node)
{
	PickyAppendState   *scan_state = (PickyAppendState *) node;

	while (scan_state->running_idx < scan_state->ncur_plans)
	{
		ChildScanCommon		child = scan_state->cur_plans[scan_state->running_idx];
		PlanState		   *state = child->content.plan_state;
		TupleTableSlot	   *slot = NULL;
		bool				quals;

		for (;;)
		{
			slot = ExecProcNode(state);

			if (TupIsNull(slot))
				break;

			node->ss.ps.ps_ExprContext->ecxt_scantuple = slot;
			quals = ExecQual(scan_state->custom_expr_states,
							 node->ss.ps.ps_ExprContext, false);

			if (quals)
				return slot;
		}

		scan_state->running_idx++;
	}

	return NULL;
}

void
pickyappend_end(CustomScanState *node)
{
	end_append_common(node);
}

void
pickyappend_rescan(CustomScanState *node)
{
	rescan_append_common(node);
}

void
pickyppend_explain(CustomScanState *node, List *ancestors, ExplainState *es)
{
	PickyAppendState   *scan_state = (PickyAppendState *) node;

	explain_append_common(node, scan_state->children_table, es);
}
