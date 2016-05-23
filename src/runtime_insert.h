#ifndef RUNTIME_INSERT_H
#define RUNTIME_INSERT_H

#include "postgres.h"
#include "optimizer/paths.h"
#include "optimizer/pathnode.h"

#include "pathman.h"
#include "nodes_common.h"


typedef struct
{
	CustomPath cpath;
} RuntimeInsertPath;

typedef struct
{
	CustomScanState css;
} RuntimeInsertState;

extern bool					pg_pathman_enable_runtime_insert;

extern CustomScanMethods	runtime_insert_plan_methods;
extern CustomExecMethods	runtime_insert_exec_methods;

Path * create_runtimeinsert_path(PlannerInfo *root, AppendPath *inner_append,
								 ParamPathInfo *param_info,
								 double sel);

Plan * create_runtimeinsert_plan(PlannerInfo *root, RelOptInfo *rel,
								 CustomPath *best_path, List *tlist,
								 List *clauses, List *custom_plans);

Node * runtimeinsert_create_scan_state(CustomScan *node);

void runtimeinsert_begin(CustomScanState *node, EState *estate, int eflags);

TupleTableSlot * runtimeappend_exec(CustomScanState *node);

void runtimeinsert_end(CustomScanState *node);

void runtimeinsert_rescan(CustomScanState *node);

void runtimeinsert_explain(CustomScanState *node, List *ancestors, ExplainState *es);

#endif
