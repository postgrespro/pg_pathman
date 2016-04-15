#include "postgres.h"
#include "optimizer/paths.h"
#include "optimizer/pathnode.h"

#include "commands/explain.h"

#include "pathman.h"


extern bool pg_pathman_enable_pickyappend;

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

	/* Currently selected plans \ plan states */
	ChildScanCommon	   *cur_plans;
	int					ncur_plans;

	/* Index of the selected plan state */
	int					running_idx;

	/* Contains reusable PlanStates */
	HTAB			   *plan_state_table;
	HASHCTL				plan_state_table_config;
} PickyAppendState;

extern set_join_pathlist_hook_type	set_join_pathlist_next;

extern CustomPathMethods			pickyappend_path_methods;
extern CustomScanMethods			pickyappend_plan_methods;
extern CustomExecMethods			pickyappend_exec_methods;

void pathman_join_pathlist_hook(PlannerInfo *root, RelOptInfo *joinrel, RelOptInfo *outerrel,
								RelOptInfo *innerrel, JoinType jointype, JoinPathExtraData *extra);

Plan * create_pickyappend_plan(PlannerInfo *root, RelOptInfo *rel, CustomPath *best_path,
							   List *tlist, List *clauses, List *custom_plans);

Node * pickyappend_create_scan_state(CustomScan *node);

void pickyappend_begin(CustomScanState *node, EState *estate, int eflags);

TupleTableSlot * pickyappend_exec(CustomScanState *node);

void pickyappend_end(CustomScanState *node);

void pickyappend_rescan(CustomScanState *node);

void pickyppend_explain(CustomScanState *node, List *ancestors, ExplainState *es);
