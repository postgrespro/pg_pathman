#ifndef ARRANGEAPPEND_H
#define ARRANGEAPPEND_H

#include "postgres.h"
#include "pickyappend.h"
#include "pathman.h"


typedef struct
{
	CustomPath			cpath;
	Oid					relid;		/* relid of the partitioned table */

	ChildScanCommon	   *children;	/* all available plans */
	int					nchildren;
} ArrangeAppendPath;

typedef struct
{
	PickyAppendState	picky_base;

	int					numCols;		/* number of sort-key columns */
	AttrNumber		   *sortColIdx;		/* their indexes in the target list */
	Oid				   *sortOperators;	/* OIDs of operators to sort them by */
	Oid				   *collations;		/* OIDs of collations */
	bool			   *nullsFirst;		/* NULLS FIRST/LAST directions */

	int					ms_nkeys;
	SortSupport			ms_sortkeys;
	TupleTableSlot	  **ms_slots;
	struct binaryheap  *ms_heap;
} ArrangeAppendState;


extern bool							pg_pathman_enable_arrangeappend;

extern CustomPathMethods			arrangeappend_path_methods;
extern CustomScanMethods			arrangeappend_plan_methods;
extern CustomExecMethods			arrangeappend_exec_methods;

Plan * create_arrangeappend_plan(PlannerInfo *root, RelOptInfo *rel,
								 CustomPath *best_path, List *tlist,
								 List *clauses, List *custom_plans);

Node * arrangeappend_create_scan_state(CustomScan *node);

void arrangeappend_begin(CustomScanState *node, EState *estate, int eflags);

TupleTableSlot * arrangeappend_exec(CustomScanState *node);

void arrangeappend_end(CustomScanState *node);

void arrangeappend_rescan(CustomScanState *node);

void arrangeappend_explain(CustomScanState *node, List *ancestors, ExplainState *es);

#endif
