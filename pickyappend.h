#include "postgres.h"
#include "optimizer/paths.h"
#include "optimizer/pathnode.h"

#include "commands/explain.h"

#include "pathman.h"

typedef struct
{
	Oid		relid;

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
	}		content;
} ChildScanCommonData;

typedef ChildScanCommonData *ChildScanCommon;

typedef struct
{
	CustomPath			cpath;
	Oid					relid;

	ChildScanCommon	   *children;
	int					nchildren;
} PickyAppendPath;

typedef struct
{
	CustomScanState		css;
	Oid					relid;
	PartRelationInfo   *prel;
	List			   *custom_exprs;

	ChildScanCommon	   *chilren;
	int					nchildren;
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
