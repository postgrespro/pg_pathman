#ifndef NODES_COMMON_H
#define NODES_COMMON_H

#include "commands/explain.h"
#include "pathman.h"


/*
 * Element of the plan_state_table
 */
typedef struct
{
	Oid			relid;				/* partition relid (key) */
	PlanState  *ps;					/* reusable plan state */
} PreservedPlanState;

typedef struct
{
	Oid		relid;					/* partition relid */

	union
	{
		Path	   *path;
		Plan	   *plan;
		PlanState  *plan_state;
	}		content;

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

void
explain_common(CustomScanState *node, HTAB *children_table, ExplainState *es);

#endif
