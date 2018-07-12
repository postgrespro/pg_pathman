/* ------------------------------------------------------------------------
 *
 * pg_compat.c
 *		Compatibility tools for PostgreSQL API
 *
 * Copyright (c) 2016, Postgres Professional
 *
 * Portions Copyright (c) 1996-2016, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * ------------------------------------------------------------------------
 */

#include "compat/pg_compat.h"

#include "utils.h"

#include "access/htup_details.h"
#include "catalog/pg_class.h"
#include "catalog/pg_proc.h"
#include "foreign/fdwapi.h"
#include "optimizer/clauses.h"
#include "optimizer/pathnode.h"
#include "optimizer/prep.h"
#include "parser/parse_utilcmd.h"
#include "port.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"

#include <math.h>


/*
 * ----------
 *  Variants
 * ----------
 */


/*
 * create_plain_partial_paths
 *	  Build partial access paths for parallel scan of a plain relation
 */
#if PG_VERSION_NUM >= 100000
void
create_plain_partial_paths(PlannerInfo *root, RelOptInfo *rel)
{
	int			parallel_workers;

	/* no more than max_parallel_workers_per_gather since 11 */
	parallel_workers = compute_parallel_worker_compat(rel, rel->pages, -1);

	/* If any limit was set to zero, the user doesn't want a parallel scan. */
	if (parallel_workers <= 0)
		return;

	/* Add an unordered partial path based on a parallel sequential scan. */
	add_partial_path(rel, create_seqscan_path(root, rel, NULL, parallel_workers));
}
#elif PG_VERSION_NUM >= 90600
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
#endif


/*
 * ExecEvalExpr
 *
 * global variables for macro wrapper evaluation
 */
#if PG_VERSION_NUM >= 90500 && PG_VERSION_NUM < 100000
Datum exprResult;
ExprDoneCond isDone;
#endif


/*
 * get_all_actual_clauses
 */
#if PG_VERSION_NUM >= 100000
List *
get_all_actual_clauses(List *restrictinfo_list)
{
	List	   *result = NIL;
	ListCell   *l;

	foreach(l, restrictinfo_list)
	{
		RestrictInfo *rinfo = (RestrictInfo *) lfirst(l);

		Assert(IsA(rinfo, RestrictInfo));

		result = lappend(result, rinfo->clause);
	}
	return result;
}
#endif


/*
 * make_restrictinfos_from_actual_clauses
 */
#if PG_VERSION_NUM >= 100000
#include "optimizer/restrictinfo.h"
#include "optimizer/var.h"

List *
make_restrictinfos_from_actual_clauses(PlannerInfo *root,
									   List *clause_list)
{
	List	   *result = NIL;
	ListCell   *l;

	foreach(l, clause_list)
	{
		Expr	   *clause = (Expr *) lfirst(l);
		bool		pseudoconstant;
		RestrictInfo *rinfo;

		/*
		 * It's pseudoconstant if it contains no Vars and no volatile
		 * functions.  We probably can't see any sublinks here, so
		 * contain_var_clause() would likely be enough, but for safety use
		 * contain_vars_of_level() instead.
		 */
		pseudoconstant =
			!contain_vars_of_level((Node *) clause, 0) &&
			!contain_volatile_functions((Node *) clause);
		if (pseudoconstant)
		{
			/* tell createplan.c to check for gating quals */
			root->hasPseudoConstantQuals = true;
		}

		rinfo = make_restrictinfo(clause,
								  true,
								  false,
								  pseudoconstant,
								  root->qual_security_level,
								  NULL,
								  NULL,
								  NULL);
		result = lappend(result, rinfo);
	}
	return result;
}
#endif


/*
 * make_result
 *	  Build a Result plan node
 */
#if PG_VERSION_NUM >= 90600
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


/*
 * Examine contents of MemoryContext.
 */
#if PG_VERSION_NUM >= 90600
void
McxtStatsInternal(MemoryContext context, int level,
				  bool examine_children,
				  MemoryContextCounters *totals)
{
	MemoryContextCounters	local_totals;
	MemoryContext			child;

	AssertArg(MemoryContextIsValid(context));

	/* Examine the context itself */
#if PG_VERSION_NUM >= 110000
	(*context->methods->stats) (context, NULL, NULL, totals);
#else
	(*context->methods->stats) (context, level, false, totals);
#endif

	memset(&local_totals, 0, sizeof(local_totals));

	if (!examine_children)
		return;

	/* Examine children */
	for (child = context->firstchild;
		 child != NULL;
		 child = child->nextchild)
	{

		McxtStatsInternal(child, level + 1,
						  examine_children,
						  &local_totals);
	}

	/* Save children stats */
	totals->nblocks += local_totals.nblocks;
	totals->freechunks += local_totals.freechunks;
	totals->totalspace += local_totals.totalspace;
	totals->freespace += local_totals.freespace;
}
#endif


/*
 * oid_cmp
 *
 * qsort comparison function for Oids;
 * needed for find_inheritance_children_array() function
 */
#if PG_VERSION_NUM >= 90500 && PG_VERSION_NUM < 100000
int
oid_cmp(const void *p1, const void *p2)
{
	Oid			v1 = *((const Oid *) p1);
	Oid			v2 = *((const Oid *) p2);

	if (v1 < v2)
		return -1;

	if (v1 > v2)
		return 1;

	return 0;
}
#endif


/*
 * set_dummy_rel_pathlist
 *	  Build a dummy path for a relation that's been excluded by constraints
 *
 * Rather than inventing a special "dummy" path type, we represent this as an
 * AppendPath with no members (see also IS_DUMMY_PATH/IS_DUMMY_REL macros).
 */
#if PG_VERSION_NUM >= 90500 && PG_VERSION_NUM < 90600
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
#endif


#if PG_VERSION_NUM >= 90600
/*
 * If this relation could possibly be scanned from within a worker, then set
 * its consider_parallel flag.
 */
void
set_rel_consider_parallel(PlannerInfo *root, RelOptInfo *rel,
						  RangeTblEntry *rte)
{
#if PG_VERSION_NUM >= 100000
#define is_parallel_safe_compat(root, exprs) is_parallel_safe((root), (exprs))
#elif PG_VERSION_NUM >= 90500
#define is_parallel_safe_compat(root, exprs) \
		(!has_parallel_hazard((exprs), false))
#endif

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
				char		proparallel = func_parallel(rte->tablesample->tsmhandler);

				if (proparallel != PROPARALLEL_SAFE)
					return;
				if (!is_parallel_safe_compat(
							root, (Node *) rte->tablesample->args))
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
			if (!is_parallel_safe_compat(root, (Node *) rte->functions))
				return;
			break;

#if PG_VERSION_NUM >= 100000
		case RTE_TABLEFUNC:
			/* not parallel safe */
			return;
#endif

		case RTE_VALUES:
			/* Check for parallel-restricted functions. */
			if (!is_parallel_safe_compat(root, (Node *) rte->values_lists))
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

#if PG_VERSION_NUM >= 100000
		case RTE_NAMEDTUPLESTORE:
			/*
			 * tuplestore cannot be shared, at least without more
			 * infrastructure to support that.
			 */
			return;
#endif
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
	if (!is_parallel_safe_compat(root, (Node *) rel->baserestrictinfo))
		return;

	/*
	 * Likewise, if the relation's outputs are not parallel-safe, give up.
	 * (Usually, they're just Vars, but sometimes they're not.)
	 */
	if (!is_parallel_safe_compat(root, (Node *) rel->reltarget->exprs))
		return;

	/* We have a winner. */
	rel->consider_parallel = true;
}
#endif


/*
 * Returns the relpersistence associated with a given relation.
 *
 * NOTE: this function is implemented in 9.6
 */
#if PG_VERSION_NUM >= 90500 && PG_VERSION_NUM < 90600
char
get_rel_persistence(Oid relid)
{
	HeapTuple		tp;
	Form_pg_class	reltup;
	char 			result;

	tp = SearchSysCache1(RELOID, ObjectIdGetDatum(relid));
	if (!HeapTupleIsValid(tp))
		elog(ERROR, "cache lookup failed for relation %u", relid);

	reltup = (Form_pg_class) GETSTRUCT(tp);
	result = reltup->relpersistence;
	ReleaseSysCache(tp);

	return result;
}
#endif

#if (PG_VERSION_NUM >= 90500 && PG_VERSION_NUM <= 90505) || \
	(PG_VERSION_NUM >= 90600 && PG_VERSION_NUM <= 90601)
/*
 * Return a palloc'd bare attribute map for tuple conversion, matching input
 * and output columns by name.  (Dropped columns are ignored in both input and
 * output.)  This is normally a subroutine for convert_tuples_by_name, but can
 * be used standalone.
 */
AttrNumber *
convert_tuples_by_name_map(TupleDesc indesc,
						   TupleDesc outdesc,
						   const char *msg)
{
	AttrNumber *attrMap;
	int			n;
	int			i;

	n = outdesc->natts;
	attrMap = (AttrNumber *) palloc0(n * sizeof(AttrNumber));
	for (i = 0; i < n; i++)
	{
		Form_pg_attribute att = TupleDescAttr(outdesc, i);
		char	   *attname;
		Oid			atttypid;
		int32		atttypmod;
		int			j;

		if (att->attisdropped)
			continue;			/* attrMap[i] is already 0 */
		attname = NameStr(att->attname);
		atttypid = att->atttypid;
		atttypmod = att->atttypmod;
		for (j = 0; j < indesc->natts; j++)
		{
			att = TupleDescAttr(indesc, j);
			if (att->attisdropped)
				continue;
			if (strcmp(attname, NameStr(att->attname)) == 0)
			{
				/* Found it, check type */
				if (atttypid != att->atttypid || atttypmod != att->atttypmod)
					ereport(ERROR,
							(errcode(ERRCODE_DATATYPE_MISMATCH),
							 errmsg_internal("%s", _(msg)),
							 errdetail("Attribute \"%s\" of type %s does not match corresponding attribute of type %s.",
									   attname,
									   format_type_be(outdesc->tdtypeid),
									   format_type_be(indesc->tdtypeid))));
				attrMap[i] = (AttrNumber) (j + 1);
				break;
			}
		}
		if (attrMap[i] == 0)
			ereport(ERROR,
					(errcode(ERRCODE_DATATYPE_MISMATCH),
					 errmsg_internal("%s", _(msg)),
					 errdetail("Attribute \"%s\" of type %s does not exist in type %s.",
							   attname,
							   format_type_be(outdesc->tdtypeid),
							   format_type_be(indesc->tdtypeid))));
	}

	return attrMap;
}
#endif

/*
 * -------------
 *  Common code
 * -------------
 */

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
		Assert(childrel->rows >= 0);
		parent_rows += childrel->rows;

#if PG_VERSION_NUM >= 90600
		parent_size += childrel->reltarget->width * childrel->rows;
#else
		parent_size += childrel->width * childrel->rows;
#endif
	}

	/* Set 'rows' for append relation */
	rel->rows = parent_rows;

	if (parent_rows == 0)
		parent_rows = 1;

#if PG_VERSION_NUM >= 90600
	rel->reltarget->width = rint(parent_size / parent_rows);
#else
	rel->width = rint(parent_size / parent_rows);
#endif

	rel->tuples = parent_rows;
}
