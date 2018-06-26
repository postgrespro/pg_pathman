/* ------------------------------------------------------------------------
 *
 * runtimeappend.h
 *		RuntimeAppend node's function prototypes and structures
 *
 * Copyright (c) 2016, Postgres Professional
 *
 * ------------------------------------------------------------------------
 */

#ifndef RUNTIME_APPEND_H
#define RUNTIME_APPEND_H


#include "pathman.h"
#include "nodes_common.h"

#include "postgres.h"
#include "optimizer/paths.h"
#include "optimizer/pathnode.h"
#include "commands/explain.h"


#define RUNTIME_APPEND_NODE_NAME "RuntimeAppend"


typedef struct
{
	CustomPath			cpath;
	Oid					relid;			/* relid of the partitioned table */

	ChildScanCommon	   *children;		/* all available plans */
	int					nchildren;
} RuntimeAppendPath;

typedef struct
{
	CustomScanState		css;
	Oid					relid;		/* relid of the partitioned table */

	/* Restrictions to be checked during ReScan and Exec */
	List			   *custom_exprs;

	/* Refined clauses for partition pruning */
	List			   *canon_custom_exprs;

	/* Copy of partitioning expression and dispatch info */
	Node			   *prel_expr;
	PartRelationInfo   *prel;

	/* All available plans \ plan states */
	HTAB			   *children_table;
	HASHCTL				children_table_config;

	/* Currently selected plans \ plan states */
	ChildScanCommon	   *cur_plans;
	int					ncur_plans;

	/* Should we include parent table? Cached for prepared statements */
	bool				enable_parent;

	/* Index of the selected plan state */
	int					running_idx;

	/* Last saved tuple (for SRF projections) */
	TupleTableSlot	   *slot;
} RuntimeAppendState;


extern bool					pg_pathman_enable_runtimeappend;

extern CustomPathMethods	runtimeappend_path_methods;
extern CustomScanMethods	runtimeappend_plan_methods;
extern CustomExecMethods	runtimeappend_exec_methods;


void init_runtime_append_static_data(void);

Path * create_runtime_append_path(PlannerInfo *root,
								  AppendPath *inner_append,
								  ParamPathInfo *param_info,
								  double sel);

Plan * create_runtime_append_plan(PlannerInfo *root, RelOptInfo *rel,
								  CustomPath *best_path, List *tlist,
								  List *clauses, List *custom_plans);

Node * runtime_append_create_scan_state(CustomScan *node);

void runtime_append_begin(CustomScanState *node,
						  EState *estate,
						  int eflags);

TupleTableSlot * runtime_append_exec(CustomScanState *node);

void runtime_append_end(CustomScanState *node);

void runtime_append_rescan(CustomScanState *node);

void runtime_append_explain(CustomScanState *node,
							List *ancestors,
							ExplainState *es);


#endif /* RUNTIME_APPEND_H */
