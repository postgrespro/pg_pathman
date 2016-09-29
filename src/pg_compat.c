/* ------------------------------------------------------------------------
 *
 * pg_compat.c
 *		Compatibility tools
 *
 * Copyright (c) 2016, Postgres Professional
 *
 * ------------------------------------------------------------------------
 */

#include "pg_compat.h"

#include "optimizer/pathnode.h"
#include "port.h"
#include "utils.h"

#include <math.h>


void
set_append_rel_size_compat(PlannerInfo *root, RelOptInfo *rel,
						   Index rti, RangeTblEntry *rte)
{
	double		parent_rows = 0;
	double		parent_size = 0;
	ListCell   *l;

	foreach(l, root->append_rel_list)
	{
		AppendRelInfo  *appinfo = (AppendRelInfo *) lfirst(l);
		Index			childRTindex,
						parentRTindex = rti;
		RelOptInfo	   *childrel;

		/* append_rel_list contains all append rels; ignore others */
		if (appinfo->parent_relid != parentRTindex)
			continue;

		childRTindex = appinfo->child_relid;

		childrel = find_base_rel(root, childRTindex);
		Assert(childrel->reloptkind == RELOPT_OTHER_MEMBER_REL);

		/*
		 * Accumulate size information from each live child.
		 */
		Assert(childrel->rows > 0);

		parent_rows += childrel->rows;
#if PG_VERSION_NUM >= 90600
		parent_size += childrel->reltarget->width * childrel->rows;
#else
		parent_size += childrel->width * childrel->rows;
#endif
	}

	rel->rows = parent_rows;
#if PG_VERSION_NUM >= 90600
	rel->reltarget->width = rint(parent_size / parent_rows);
#else
	rel->width = rint(parent_size / parent_rows);
#endif
	rel->tuples = parent_rows;
}

extern
void copy_targetlist_compat(RelOptInfo *dest, RelOptInfo *rel)
{
	ListCell	   *lc;

#if PG_VERSION_NUM >= 90600
	dest->reltarget->exprs = NIL;
	foreach(lc, rel->reltarget->exprs)
#else
	dest->reltargetlist = NIL;
	foreach(lc, rel->reltargetlist)
#endif
	{
		Node *new_target;
		Node *node;

		node = (Node *) lfirst(lc);
		new_target = copyObject(node);
		change_varnos(new_target, rel->relid, dest->relid);
#if PG_VERSION_NUM >= 90600
		dest->reltarget->exprs = lappend(dest->reltarget->exprs, new_target);
#else
		dest->reltargetlist = lappend(dest->reltargetlist, new_target);
#endif
	}
}

#if PG_VERSION_NUM >= 90600
/*
 * make_result
 *	  Build a Result plan node
 */
Result *
make_result(List *tlist,
			Node *resconstantqual,
			Plan *subplan)
{
	Result	   *node = makeNode(Result);
	Plan	   *plan = &node->plan;

	plan->targetlist = tlist;
	plan->qual = NIL;
	plan->lefttree = subplan;
	plan->righttree = NULL;
	node->resconstantqual = resconstantqual;

	return node;
}
#endif
