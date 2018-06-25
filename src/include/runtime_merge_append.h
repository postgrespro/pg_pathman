/* ------------------------------------------------------------------------
 *
 * runtime_merge_append.h
 *		RuntimeMergeAppend node's function prototypes and structures
 *
 * Copyright (c) 2016, Postgres Professional
 * Portions Copyright (c) 1996-2015, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * ------------------------------------------------------------------------
 */

#ifndef RUNTIME_MERGE_APPEND_H
#define RUNTIME_MERGE_APPEND_H


#include "runtime_append.h"
#include "pathman.h"

#include "postgres.h"


#define RUNTIME_MERGE_APPEND_NODE_NAME "RuntimeMergeAppend"


typedef struct
{
	RuntimeAppendPath	rpath;

	double				limit_tuples;
} RuntimeMergeAppendPath;

typedef struct
{
	RuntimeAppendState	rstate;

	int					numCols;		/* number of sort-key columns */
	AttrNumber		   *sortColIdx;		/* their indexes in the target list */
	Oid				   *sortOperators;	/* OIDs of operators to sort them by */
	Oid				   *collations;		/* OIDs of collations */
	bool			   *nullsFirst;		/* NULLS FIRST/LAST directions */

	int					ms_nkeys;
	SortSupport			ms_sortkeys;
	TupleTableSlot	  **ms_slots;
	struct binaryheap  *ms_heap;
	bool				ms_initialized;
} RuntimeMergeAppendState;


extern bool					pg_pathman_enable_runtime_merge_append;

extern CustomPathMethods	runtime_merge_append_path_methods;
extern CustomScanMethods	runtime_merge_append_plan_methods;
extern CustomExecMethods	runtime_merge_append_exec_methods;


void init_runtime_merge_append_static_data(void);

Path * create_runtime_merge_append_path(PlannerInfo *root,
										AppendPath *inner_append,
										ParamPathInfo *param_info,
										double sel);

Plan * create_runtime_merge_append_plan(PlannerInfo *root, RelOptInfo *rel,
										CustomPath *best_path, List *tlist,
										List *clauses, List *custom_plans);

Node * runtime_merge_append_create_scan_state(CustomScan *node);

void runtime_merge_append_begin(CustomScanState *node,
								EState *estate,
								int eflags);

TupleTableSlot * runtime_merge_append_exec(CustomScanState *node);

void runtime_merge_append_end(CustomScanState *node);

void runtime_merge_append_rescan(CustomScanState *node);

void runtime_merge_append_explain(CustomScanState *node,
								  List *ancestors,
								  ExplainState *es);


#endif /* RUNTIME_MERGE_APPEND_H */
