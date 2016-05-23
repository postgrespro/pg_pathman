/* ------------------------------------------------------------------------
 *
 * nodes_common.h
 *		Common function prototypes and structs for custom nodes
 *
 * Copyright (c) 2016, Postgres Professional
 *
 * ------------------------------------------------------------------------
 */
#ifndef NODES_COMMON_H
#define NODES_COMMON_H

#include "commands/explain.h"
#include "pathman.h"


typedef struct
{
	Oid			relid;				/* partition relid */

	enum
	{
		CHILD_PATH = 0,
		CHILD_PLAN,
		CHILD_PLAN_STATE
	}		content_type;

	union
	{
		Path	   *path;
		Plan	   *plan;
		PlanState  *plan_state;
	}			content;

	int		original_order;			/* for sorting in EXPLAIN */
} ChildScanCommonData;

typedef ChildScanCommonData *ChildScanCommon;

/*
 * Destroy exhausted plan states
 */
inline static void
clear_plan_states(CustomScanState *scan_state)
{
	ListCell *state_cell;

	foreach (state_cell, scan_state->custom_ps)
	{
		ExecEndNode((PlanState *) lfirst(state_cell));
	}
}

Path * create_append_path_common(PlannerInfo *root,
								 AppendPath *inner_append,
								 ParamPathInfo *param_info,
								 CustomPathMethods *path_methods,
								 uint32 size,
								 double sel);

Plan * create_append_plan_common(PlannerInfo *root, RelOptInfo *rel,
								 CustomPath *best_path, List *tlist,
								 List *clauses, List *custom_plans,
								 CustomScanMethods *scan_methods);

Node * create_append_scan_state_common(CustomScan *node,
									   CustomExecMethods *exec_methods,
									   uint32 size);

void begin_append_common(CustomScanState *node, EState *estate, int eflags);

TupleTableSlot * exec_append_common(CustomScanState *node,
									void (*fetch_next_tuple) (CustomScanState *node));

void end_append_common(CustomScanState *node);

void rescan_append_common(CustomScanState *node);

void explain_append_common(CustomScanState *node,
						   HTAB *children_table,
						   ExplainState *es);

#endif
