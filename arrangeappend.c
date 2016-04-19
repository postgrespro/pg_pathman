#include "postgres.h"
#include "arrangeappend.h"
#include "pickyappend.h"

#include "pathman.h"

#include "optimizer/clauses.h"
#include "optimizer/cost.h"
#include "optimizer/restrictinfo.h"
#include "optimizer/planmain.h"
#include "optimizer/tlist.h"
#include "optimizer/var.h"

#include "lib/binaryheap.h"


bool				pg_pathman_enable_arrangeappend = true;

CustomPathMethods	arrangeappend_path_methods;
CustomScanMethods	arrangeappend_plan_methods;
CustomExecMethods	arrangeappend_exec_methods;


/*
 * We have one slot for each item in the heap array.  We use SlotNumber
 * to store slot indexes.  This doesn't actually provide any formal
 * type-safety, but it makes the code more self-documenting.
 */
typedef int32 SlotNumber;

/*
 * Compare the tuples in the two given slots.
 */
static int32
heap_compare_slots(Datum a, Datum b, void *arg)
{
	ArrangeAppendState *node = (ArrangeAppendState *) arg;
	SlotNumber			slot1 = DatumGetInt32(a);
	SlotNumber			slot2 = DatumGetInt32(b);

	TupleTableSlot	   *s1 = node->ms_slots[slot1];
	TupleTableSlot	   *s2 = node->ms_slots[slot2];
	int			nkey;

	Assert(!TupIsNull(s1));
	Assert(!TupIsNull(s2));

	for (nkey = 0; nkey < node->ms_nkeys; nkey++)
	{
		SortSupport sortKey = node->ms_sortkeys + nkey;
		AttrNumber	attno = sortKey->ssup_attno;
		Datum		datum1,
					datum2;
		bool		isNull1,
					isNull2;
		int			compare;

		datum1 = slot_getattr(s1, attno, &isNull1);
		datum2 = slot_getattr(s2, attno, &isNull2);

		compare = ApplySortComparator(datum1, isNull1,
									  datum2, isNull2,
									  sortKey);
		if (compare != 0)
			return -compare;
	}
	return 0;
}

Path *
create_arrangeappend_path(PlannerInfo *root,
						AppendPath *inner_append,
						ParamPathInfo *param_info,
						List *picky_clauses)
{
	return create_append_path_common(root, inner_append,
									 param_info, picky_clauses,
									 &arrangeappend_path_methods);
}

Plan *
create_arrangeappend_plan(PlannerInfo *root, RelOptInfo *rel,
						CustomPath *best_path, List *tlist,
						List *clauses, List *custom_plans)
{
	return create_append_plan_common(root, rel,
									 best_path, tlist,
									 clauses, custom_plans,
									 &arrangeappend_plan_methods);
}

Node *
arrangeappend_create_scan_state(CustomScan *node)
{
	return create_append_scan_state_common(node,
										   &arrangeappend_exec_methods,
										   sizeof(ArrangeAppendState));
}

void
arrangeappend_begin(CustomScanState *node, EState *estate, int eflags)
{
	ArrangeAppendState   *scan_state = (ArrangeAppendState *) node;

	begin_append_common(node, estate, eflags);
}

TupleTableSlot *
arrangeappend_exec(CustomScanState *node)
{
	ArrangeAppendState   *scan_state = (ArrangeAppendState *) node;

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
arrangeappend_end(CustomScanState *node)
{
	ArrangeAppendState   *scan_state = (ArrangeAppendState *) node;

	end_append_common(node);
}

void
arrangeappend_rescan(CustomScanState *node)
{
	ArrangeAppendState *scan_state = (ArrangeAppendState *) node;
	int					nplans = scan_state->picky_base.ncur_plans;
	int					i;

	rescan_append_common(node);

	scan_state->ms_slots = (TupleTableSlot **) palloc0(sizeof(TupleTableSlot *) * nplans);
	scan_state->ms_heap = binaryheap_allocate(nplans, heap_compare_slots, scan_state);

		/*
	 * initialize sort-key information
	 */
	scan_state->ms_nkeys = scan_state->numCols;
	scan_state->ms_sortkeys = palloc0(sizeof(SortSupportData) * scan_state->numCols);

	for (i = 0; i < scan_state->numCols; i++)
	{
		SortSupport sortKey = scan_state->ms_sortkeys + i;

		sortKey->ssup_cxt = CurrentMemoryContext;
		sortKey->ssup_collation = scan_state->collations[i];
		sortKey->ssup_nulls_first = scan_state->nullsFirst[i];
		sortKey->ssup_attno = scan_state->sortColIdx[i];

		/*
		 * It isn't feasible to perform abbreviated key conversion, since
		 * tuples are pulled into mergestate's binary heap as needed.  It
		 * would likely be counter-productive to convert tuples into an
		 * abbreviated representation as they're pulled up, so opt out of that
		 * additional optimization entirely.
		 */
		sortKey->abbreviate = false;

		PrepareSortSupportFromOrderingOp(scan_state->sortOperators[i], sortKey);
	}
}

void
pickyppend_explain(CustomScanState *node, List *ancestors, ExplainState *es)
{
	ArrangeAppendState   *scan_state = (ArrangeAppendState *) node;

	explain_append_common(node, scan_state->picky_base.children_table, es);
}
