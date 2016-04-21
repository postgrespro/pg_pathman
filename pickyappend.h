/* ------------------------------------------------------------------------
 *
 * pickyappend.h
 *		PickyAppend node's function prototypes and structures
 *
 * Copyright (c) 2016, Postgres Professional
 *
 * ------------------------------------------------------------------------
 */
#ifndef PICKYAPPEND_H
#define PICKYAPPEND_H

#include "postgres.h"
#include "optimizer/paths.h"
#include "optimizer/pathnode.h"
#include "commands/explain.h"

#include "pathman.h"
#include "nodes_common.h"


typedef struct
{
	CustomPath			cpath;
	Oid					relid;		/* relid of the partitioned table */

	ChildScanCommon	   *children;	/* all available plans */
	int					nchildren;
} PickyAppendPath;

typedef struct
{
	CustomScanState		css;
	Oid					relid;		/* relid of the partitioned table */
	PartRelationInfo   *prel;

	/* Restrictions to be checked during ReScan and Exec */
	List			   *custom_exprs;
	List			   *custom_expr_states;

	/* All available plans */
	HTAB			   *children_table;
	HASHCTL				children_table_config;

	/* Contains reusable PlanStates */
	HTAB			   *plan_state_table;
	HASHCTL				plan_state_table_config;

	/* Currently selected plans \ plan states */
	ChildScanCommon	   *cur_plans;
	int					ncur_plans;

	/* Index of the selected plan state */
	int					running_idx;
} PickyAppendState;

extern bool					pg_pathman_enable_pickyappend;

extern CustomPathMethods	pickyappend_path_methods;
extern CustomScanMethods	pickyappend_plan_methods;
extern CustomExecMethods	pickyappend_exec_methods;

Path * create_pickyappend_path(PlannerInfo *root, AppendPath *inner_append,
							   ParamPathInfo *param_info, List *picky_clauses,
							   double sel);

Plan * create_pickyappend_plan(PlannerInfo *root, RelOptInfo *rel,
							   CustomPath *best_path, List *tlist,
							   List *clauses, List *custom_plans);

Node * pickyappend_create_scan_state(CustomScan *node);

void pickyappend_begin(CustomScanState *node, EState *estate, int eflags);

TupleTableSlot * pickyappend_exec(CustomScanState *node);

void pickyappend_end(CustomScanState *node);

void pickyappend_rescan(CustomScanState *node);

void pickyppend_explain(CustomScanState *node, List *ancestors, ExplainState *es);

#endif
