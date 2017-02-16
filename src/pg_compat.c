/* ------------------------------------------------------------------------
 *
 * pg_compat.c
 *		Compatibility tools
 *
 * Copyright (c) 2016, Postgres Professional
 *
 * Portions Copyright (c) 1996-2016, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * ------------------------------------------------------------------------
 */

#include "pg_compat.h"

#include "catalog/pg_proc.h"
#include "foreign/fdwapi.h"
#include "optimizer/clauses.h"
#include "optimizer/pathnode.h"
#include "optimizer/prep.h"
#include "port.h"
#include "utils.h"
#include "utils/lsyscache.h"

#include <math.h>


/* Common code */
void
set_append_rel_size_compat(PlannerInfo *root, RelOptInfo *rel, Index rti)
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

	/* Set 'rows' for append relation */
	rel->rows = parent_rows;

#if PG_VERSION_NUM >= 90600
	rel->reltarget->width = rint(parent_size / parent_rows);
#else
	rel->width = rint(parent_size / parent_rows);
#endif

	rel->tuples = parent_rows;
}


/*
 * ----------
 *  Variants
 * ----------
 */

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


/*
 * If this relation could possibly be scanned from within a worker, then set
 * its consider_parallel flag.
 */
void
set_rel_consider_parallel(PlannerInfo *root, RelOptInfo *rel,
						  RangeTblEntry *rte)
{
	/*
	 * The flag has previously been initialized to false, so we can just
	 * return if it becomes clear that we can't safely set it.
	 */
	Assert(!rel->consider_parallel);

	/* Don't call this if parallelism is disallowed for the entire query. */
	Assert(root->glob->parallelModeOK);

	/* This should only be called for baserels and appendrel children. */
	Assert(rel->reloptkind == RELOPT_BASEREL ||
		   rel->reloptkind == RELOPT_OTHER_MEMBER_REL);

	/* Assorted checks based on rtekind. */
	switch (rte->rtekind)
	{
		case RTE_RELATION:

			/*
			 * Currently, parallel workers can't access the leader's temporary
			 * tables.  We could possibly relax this if the wrote all of its
			 * local buffers at the start of the query and made no changes
			 * thereafter (maybe we could allow hint bit changes), and if we
			 * taught the workers to read them.  Writing a large number of
			 * temporary buffers could be expensive, though, and we don't have
			 * the rest of the necessary infrastructure right now anyway.  So
			 * for now, bail out if we see a temporary table.
			 */
			if (get_rel_persistence(rte->relid) == RELPERSISTENCE_TEMP)
				return;

			/*
			 * Table sampling can be pushed down to workers if the sample
			 * function and its arguments are safe.
			 */
			if (rte->tablesample != NULL)
			{
				Oid			proparallel = func_parallel(rte->tablesample->tsmhandler);

				if (proparallel != PROPARALLEL_SAFE)
					return;
				if (has_parallel_hazard((Node *) rte->tablesample->args,
										false))
					return;
			}

			/*
			 * Ask FDWs whether they can support performing a ForeignScan
			 * within a worker.  Most often, the answer will be no.  For
			 * example, if the nature of the FDW is such that it opens a TCP
			 * connection with a remote server, each parallel worker would end
			 * up with a separate connection, and these connections might not
			 * be appropriately coordinated between workers and the leader.
			 */
			if (rte->relkind == RELKIND_FOREIGN_TABLE)
			{
				Assert(rel->fdwroutine);
				if (!rel->fdwroutine->IsForeignScanParallelSafe)
					return;
				if (!rel->fdwroutine->IsForeignScanParallelSafe(root, rel, rte))
					return;
			}

			/*
			 * There are additional considerations for appendrels, which we'll
			 * deal with in set_append_rel_size and set_append_rel_pathlist.
			 * For now, just set consider_parallel based on the rel's own
			 * quals and targetlist.
			 */
			break;

		case RTE_SUBQUERY:

			/*
			 * There's no intrinsic problem with scanning a subquery-in-FROM
			 * (as distinct from a SubPlan or InitPlan) in a parallel worker.
			 * If the subquery doesn't happen to have any parallel-safe paths,
			 * then flagging it as consider_parallel won't change anything,
			 * but that's true for plain tables, too.  We must set
			 * consider_parallel based on the rel's own quals and targetlist,
			 * so that if a subquery path is parallel-safe but the quals and
			 * projection we're sticking onto it are not, we correctly mark
			 * the SubqueryScanPath as not parallel-safe.  (Note that
			 * set_subquery_pathlist() might push some of these quals down
			 * into the subquery itself, but that doesn't change anything.)
			 */
			break;

		case RTE_JOIN:
			/* Shouldn't happen; we're only considering baserels here. */
			Assert(false);
			return;

		case RTE_FUNCTION:
			/* Check for parallel-restricted functions. */
			if (has_parallel_hazard((Node *) rte->functions, false))
				return;
			break;

		case RTE_VALUES:
			/* Check for parallel-restricted functions. */
			if (has_parallel_hazard((Node *) rte->values_lists, false))
				return;
			break;

		case RTE_CTE:

			/*
			 * CTE tuplestores aren't shared among parallel workers, so we
			 * force all CTE scans to happen in the leader.  Also, populating
			 * the CTE would require executing a subplan that's not available
			 * in the worker, might be parallel-restricted, and must get
			 * executed only once.
			 */
			return;
	}

	/*
	 * If there's anything in baserestrictinfo that's parallel-restricted, we
	 * give up on parallelizing access to this relation.  We could consider
	 * instead postponing application of the restricted quals until we're
	 * above all the parallelism in the plan tree, but it's not clear that
	 * that would be a win in very many cases, and it might be tricky to make
	 * outer join clauses work correctly.  It would likely break equivalence
	 * classes, too.
	 */
	if (has_parallel_hazard((Node *) rel->baserestrictinfo, false))
		return;

	/*
	 * Likewise, if the relation's outputs are not parallel-safe, give up.
	 * (Usually, they're just Vars, but sometimes they're not.)
	 */
	if (has_parallel_hazard((Node *) rel->reltarget->exprs, false))
		return;

	/* We have a winner. */
	rel->consider_parallel = true;
}


/*
 * create_plain_partial_paths
 *	  Build partial access paths for parallel scan of a plain relation
 */
void
create_plain_partial_paths(PlannerInfo *root, RelOptInfo *rel)
{
	int			parallel_workers;

	/*
	 * If the user has set the parallel_workers reloption, use that; otherwise
	 * select a default number of workers.
	 */
	if (rel->rel_parallel_workers != -1)
		parallel_workers = rel->rel_parallel_workers;
	else
	{
		int			parallel_threshold;

		/*
		 * If this relation is too small to be worth a parallel scan, just
		 * return without doing anything ... unless it's an inheritance child.
		 * In that case, we want to generate a parallel path here anyway.  It
		 * might not be worthwhile just for this relation, but when combined
		 * with all of its inheritance siblings it may well pay off.
		 */
		if (rel->pages < (BlockNumber) min_parallel_relation_size &&
			rel->reloptkind == RELOPT_BASEREL)
			return;

		/*
		 * Select the number of workers based on the log of the size of the
		 * relation.  This probably needs to be a good deal more
		 * sophisticated, but we need something here for now.  Note that the
		 * upper limit of the min_parallel_relation_size GUC is chosen to
		 * prevent overflow here.
		 */
		parallel_workers = 1;
		parallel_threshold = Max(min_parallel_relation_size, 1);
		while (rel->pages >= (BlockNumber) (parallel_threshold * 3))
		{
			parallel_workers++;
			parallel_threshold *= 3;
			if (parallel_threshold > INT_MAX / 3)
				break;			/* avoid overflow */
		}
	}

	/*
	 * In no case use more than max_parallel_workers_per_gather workers.
	 */
	parallel_workers = Min(parallel_workers, max_parallel_workers_per_gather);

	/* If any limit was set to zero, the user doesn't want a parallel scan. */
	if (parallel_workers <= 0)
		return;

	/* Add an unordered partial path based on a parallel sequential scan. */
	add_partial_path(rel, create_seqscan_path(root, rel, NULL, parallel_workers));
}


#else /* PG_VERSION_NUM >= 90500 */


/*
 * set_dummy_rel_pathlist
 *	  Build a dummy path for a relation that's been excluded by constraints
 *
 * Rather than inventing a special "dummy" path type, we represent this as an
 * AppendPath with no members (see also IS_DUMMY_PATH/IS_DUMMY_REL macros).
 */
void
set_dummy_rel_pathlist(RelOptInfo *rel)
{
	/* Set dummy size estimates --- we leave attr_widths[] as zeroes */
	rel->rows = 0;
	rel->width = 0;

	/* Discard any pre-existing paths; no further need for them */
	rel->pathlist = NIL;

	add_path(rel, (Path *) create_append_path(rel, NIL, NULL));

	/*
	 * We set the cheapest path immediately, to ensure that IS_DUMMY_REL()
	 * will recognize the relation as dummy if anyone asks.  This is redundant
	 * when we're called from set_rel_size(), but not when called from
	 * elsewhere, and doing it twice is harmless anyway.
	 */
	set_cheapest(rel);
}


#endif /* PG_VERSION_NUM >= 90600 */
