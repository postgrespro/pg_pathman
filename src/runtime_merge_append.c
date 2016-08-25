/* ------------------------------------------------------------------------
 *
 * runtime_merge_append.c
 *		RuntimeMergeAppend node's function definitions and global variables
 *
 * Copyright (c) 2016, Postgres Professional
 *
 * ------------------------------------------------------------------------
 */
#include "postgres.h"
#include "runtime_merge_append.h"

#include "pathman.h"

#include "catalog/pg_collation.h"
#include "miscadmin.h"
#include "nodes/nodeFuncs.h"
#include "optimizer/clauses.h"
#include "optimizer/cost.h"
#include "optimizer/restrictinfo.h"
#include "optimizer/planmain.h"
#include "optimizer/tlist.h"
#include "optimizer/var.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/ruleutils.h"

#include "lib/binaryheap.h"


bool				pg_pathman_enable_runtime_merge_append = true;

CustomPathMethods	runtime_merge_append_path_methods;
CustomScanMethods	runtime_merge_append_plan_methods;
CustomExecMethods	runtime_merge_append_exec_methods;

typedef struct
{
	int			numCols;
	AttrNumber *sortColIdx;
	Oid		   *sortOperators;
	Oid		   *collations;
	bool	   *nullsFirst;
} MergeAppendGuts;

static Plan * prepare_sort_from_pathkeys(PlannerInfo *root, Plan *lefttree, List *pathkeys,
										 Relids relids, const AttrNumber *reqColIdx,
										 bool adjust_tlist_in_place, int *p_numsortkeys,
										 AttrNumber **p_sortColIdx, Oid **p_sortOperators,
										 Oid **p_collations, bool **p_nullsFirst);

static Sort * make_sort(PlannerInfo *root, Plan *lefttree, int numCols,
						AttrNumber *sortColIdx, Oid *sortOperators,
						Oid *collations, bool *nullsFirst,
						double limit_tuples);

static void copy_plan_costsize(Plan *dest, Plan *src);

static void show_sort_group_keys(PlanState *planstate, const char *qlabel,
								 int nkeys, AttrNumber *keycols,
								 Oid *sortOperators, Oid *collations, bool *nullsFirst,
								 List *ancestors, ExplainState *es);

/*
 * We have one slot for each item in the heap array.  We use SlotNumber
 * to store slot indexes.  This doesn't actually provide any formal
 * type-safety, but it makes the code more self-documenting.
 */
typedef int32 SlotNumber;

/*
 * Compare the tuples in the two given slots.
 */
static int32
heap_compare_slots(Datum a, Datum b, void *arg)
{
	RuntimeMergeAppendState	   *node = (RuntimeMergeAppendState *) arg;
	SlotNumber					slot1 = DatumGetInt32(a);
	SlotNumber				 	slot2 = DatumGetInt32(b);

	TupleTableSlot			   *s1 = node->ms_slots[slot1];
	TupleTableSlot			   *s2 = node->ms_slots[slot2];
	int			nkey;

	Assert(!TupIsNull(s1));
	Assert(!TupIsNull(s2));

	for (nkey = 0; nkey < node->ms_nkeys; nkey++)
	{
		SortSupport sortKey = node->ms_sortkeys + nkey;
		AttrNumber	attno = sortKey->ssup_attno;
		Datum		datum1,
					datum2;
		bool		isNull1,
					isNull2;
		int			compare;

		datum1 = slot_getattr(s1, attno, &isNull1);
		datum2 = slot_getattr(s2, attno, &isNull2);

		compare = ApplySortComparator(datum1, isNull1,
									  datum2, isNull2,
									  sortKey);
		if (compare != 0)
			return -compare;
	}
	return 0;
}

static void
pack_runtimemergeappend_private(CustomScan *cscan, MergeAppendGuts *mag)
{
	List   *runtimemergeappend_private = NIL;
	List   *sortColIdx    = NIL,
		   *sortOperators = NIL,
		   *collations    = NIL,
		   *nullsFirst    = NIL;
	int		i;

	for (i = 0; i < mag->numCols; i++)
	{
		sortColIdx    = lappend_int(sortColIdx, mag->sortColIdx[i]);
		sortOperators = lappend_oid(sortOperators, mag->sortOperators[i]);
		collations    = lappend_oid(collations, mag->collations[i]);
		nullsFirst    = lappend_int(nullsFirst, mag->nullsFirst[i]);
	}

	runtimemergeappend_private = list_make2(makeInteger(mag->numCols),
											list_make4(sortColIdx,
													   sortOperators,
													   collations,
													   nullsFirst));

	/* Append RuntimeMergeAppend's data to the 'custom_private' */
	cscan->custom_private = lappend(cscan->custom_private,
									runtimemergeappend_private);
}

static void
unpack_runtimemergeappend_private(RuntimeMergeAppendState *scan_state,
								  CustomScan *cscan)
{
#define FillStateField(name, type, method) \
	do \
	{ \
		ListCell   *lc; \
		int			i = 0; \
		Assert(scan_state->numCols == list_length(name)); \
		scan_state->name = palloc0(scan_state->numCols * sizeof(type)); \
		foreach (lc, name) \
			scan_state->name[i++] = method(lc); \
	} \
	while (0)

	List   *runtimemergeappend_private = NIL;
	List   *sortColIdx,
		   *sortOperators,
		   *collations,
		   *nullsFirst;

	/*
	 * RuntimeMergeAppend node's private data is stored in
	 * second element of the 'custom_private' list, right
	 * after the RuntimeAppend node's private data (2nd)
	 */
	runtimemergeappend_private = lsecond(cscan->custom_private);
	scan_state->numCols = intVal(linitial(runtimemergeappend_private));

	sortColIdx    = linitial(lsecond(runtimemergeappend_private));
	sortOperators = lsecond(lsecond(runtimemergeappend_private));
	collations    = lthird(lsecond(runtimemergeappend_private));
	nullsFirst    = lfourth(lsecond(runtimemergeappend_private));

	FillStateField(sortColIdx,    AttrNumber, lfirst_int);
	FillStateField(sortOperators, Oid,        lfirst_oid);
	FillStateField(collations,    Oid,        lfirst_oid);
	FillStateField(nullsFirst,    bool,       lfirst_int);
}

Path *
create_runtimemergeappend_path(PlannerInfo *root,
							   AppendPath *inner_append,
							   ParamPathInfo *param_info,
							   double sel)
{
	RelOptInfo *rel = inner_append->path.parent;
	Path	   *path;
	double		limit_tuples;

	path = create_append_path_common(root, inner_append,
									 param_info,
									 &runtime_merge_append_path_methods,
									 sizeof(RuntimeMergeAppendPath),
									 sel);

	if (bms_equal(rel->relids, root->all_baserels))
		limit_tuples = root->limit_tuples;
	else
		limit_tuples = -1.0;

	((RuntimeMergeAppendPath *) path)->limit_tuples = limit_tuples;

	return path;
}

Plan *
create_runtimemergeappend_plan(PlannerInfo *root, RelOptInfo *rel,
							   CustomPath *best_path, List *tlist,
							   List *clauses, List *custom_plans)
{
	CustomScan	   *node;
	Plan		   *plan;
	List		   *pathkeys = best_path->path.pathkeys;
	double			limit_tuples = ((RuntimeMergeAppendPath *) best_path)->limit_tuples;

	MergeAppendGuts mag;

	ListCell	   *path_cell;
	ListCell	   *plan_cell;

	plan = create_append_plan_common(root, rel,
									 best_path, tlist,
									 clauses, custom_plans,
									 &runtime_merge_append_plan_methods);

	node = (CustomScan *) plan;

	(void) prepare_sort_from_pathkeys(root, plan, pathkeys,
									  best_path->path.parent->relids,
									  NULL,
									  true,
									  &mag.numCols,
									  &mag.sortColIdx,
									  &mag.sortOperators,
									  &mag.collations,
									  &mag.nullsFirst);

	/*
	 * Now prepare the child plans.  We must apply prepare_sort_from_pathkeys
	 * even to subplans that don't need an explicit sort, to make sure they
	 * are returning the same sort key columns the MergeAppend expects.
	 */
	forboth(path_cell, best_path->custom_paths, plan_cell, custom_plans)
	{
		Path	   *subpath = (Path *) lfirst(path_cell);
		Plan	   *subplan = (Plan *) lfirst(plan_cell);

		int			numsortkeys;
		AttrNumber *sortColIdx;
		Oid		   *sortOperators;
		Oid		   *collations;
		bool	   *nullsFirst;

		/* Compute sort column info, and adjust subplan's tlist as needed */
		subplan = prepare_sort_from_pathkeys(root, subplan, pathkeys,
											 subpath->parent->relids,
											 mag.sortColIdx,
											 false,
											 &numsortkeys,
											 &sortColIdx,
											 &sortOperators,
											 &collations,
											 &nullsFirst);

		/*
		 * Check that we got the same sort key information.  We just Assert
		 * that the sortops match, since those depend only on the pathkeys;
		 * but it seems like a good idea to check the sort column numbers
		 * explicitly, to ensure the tlists really do match up.
		 */
		Assert(numsortkeys == mag.numCols);
		if (memcmp(sortColIdx, mag.sortColIdx,
				   numsortkeys * sizeof(AttrNumber)) != 0)
			elog(ERROR, "RuntimeMergeAppend child's targetlist doesn't match RuntimeMergeAppend");
		Assert(memcmp(sortOperators, mag.sortOperators,
					  numsortkeys * sizeof(Oid)) == 0);
		Assert(memcmp(collations, mag.collations,
					  numsortkeys * sizeof(Oid)) == 0);
		Assert(memcmp(nullsFirst, mag.nullsFirst,
					  numsortkeys * sizeof(bool)) == 0);

		/* Now, insert a Sort node if subplan isn't sufficiently ordered */
		if (!pathkeys_contained_in(pathkeys, subpath->pathkeys))
			subplan = (Plan *) make_sort(root, subplan, numsortkeys,
										 sortColIdx, sortOperators,
										 collations, nullsFirst,
										 limit_tuples);

		/* Replace subpath with subplan */
		lfirst(plan_cell) = subplan;
	}

	pack_runtimemergeappend_private(node, &mag);

	return plan;
}

Node *
runtimemergeappend_create_scan_state(CustomScan *node)
{
	Node *state;
	state = create_append_scan_state_common(node,
											&runtime_merge_append_exec_methods,
											sizeof(RuntimeMergeAppendState));

	unpack_runtimemergeappend_private((RuntimeMergeAppendState *) state, node);

	return state;
}

void
runtimemergeappend_begin(CustomScanState *node, EState *estate, int eflags)
{
	begin_append_common(node, estate, eflags);
}

static void
fetch_next_tuple(CustomScanState *node)
{
	RuntimeMergeAppendState	   *scan_state = (RuntimeMergeAppendState *) node;
	RuntimeAppendState		   *rstate = &scan_state->rstate;
	PlanState				   *ps;
	int							i;

	if (!scan_state->ms_initialized)
	{
		for (i = 0; i < scan_state->rstate.ncur_plans; i++)
		{
			ChildScanCommon		child = scan_state->rstate.cur_plans[i];
			PlanState		   *ps = child->content.plan_state;

			Assert(child->content_type == CHILD_PLAN_STATE);

			scan_state->ms_slots[i] = ExecProcNode(ps);
			if (!TupIsNull(scan_state->ms_slots[i]))
				binaryheap_add_unordered(scan_state->ms_heap, Int32GetDatum(i));
		}
		binaryheap_build(scan_state->ms_heap);
		scan_state->ms_initialized = true;
	}
	else
	{
		i = DatumGetInt32(binaryheap_first(scan_state->ms_heap));
		ps = scan_state->rstate.cur_plans[i]->content.plan_state;

		for (;;)
		{
			bool quals;

			scan_state->ms_slots[i] = ExecProcNode(ps);

			if (TupIsNull(scan_state->ms_slots[i]))
			{
				(void) binaryheap_remove_first(scan_state->ms_heap);
				break;
			}

			node->ss.ps.ps_ExprContext->ecxt_scantuple = scan_state->ms_slots[i];
			quals = ExecQual(rstate->custom_expr_states,
							 node->ss.ps.ps_ExprContext, false);

			ResetExprContext(node->ss.ps.ps_ExprContext);

			if (quals)
			{
				binaryheap_replace_first(scan_state->ms_heap, Int32GetDatum(i));
				break;
			}
		}
	}

	if (binaryheap_empty(scan_state->ms_heap))
	{
		/* All the subplans are exhausted, and so is the heap */
		rstate->slot = NULL;
		return;
	}
	else
	{
		i = DatumGetInt32(binaryheap_first(scan_state->ms_heap));
		rstate->slot = scan_state->ms_slots[i];
		return;
	}
}

TupleTableSlot *
runtimemergeappend_exec(CustomScanState *node)
{
	return exec_append_common(node, fetch_next_tuple);
}

void
runtimemergeappend_end(CustomScanState *node)
{
	RuntimeMergeAppendState	   *scan_state = (RuntimeMergeAppendState *) node;

	end_append_common(node);

	if (scan_state->ms_heap)
		binaryheap_free(scan_state->ms_heap);
}

void
runtimemergeappend_rescan(CustomScanState *node)
{
	RuntimeMergeAppendState	   *scan_state = (RuntimeMergeAppendState *) node;
	int							nplans;
	int							i;

	rescan_append_common(node);

	nplans = scan_state->rstate.ncur_plans;

	scan_state->ms_slots = (TupleTableSlot **) palloc0(sizeof(TupleTableSlot *) * nplans);
	scan_state->ms_heap = binaryheap_allocate(nplans, heap_compare_slots, scan_state);

	/*
	 * initialize sort-key information
	 */
	scan_state->ms_nkeys = scan_state->numCols;
	scan_state->ms_sortkeys = palloc0(sizeof(SortSupportData) * scan_state->numCols);

	for (i = 0; i < scan_state->numCols; i++)
	{
		SortSupport sortKey = scan_state->ms_sortkeys + i;

		sortKey->ssup_cxt = CurrentMemoryContext;
		sortKey->ssup_collation = scan_state->collations[i];
		sortKey->ssup_nulls_first = scan_state->nullsFirst[i];
		sortKey->ssup_attno = scan_state->sortColIdx[i];

		/*
		 * It isn't feasible to perform abbreviated key conversion, since
		 * tuples are pulled into mergestate's binary heap as needed.  It
		 * would likely be counter-productive to convert tuples into an
		 * abbreviated representation as they're pulled up, so opt out of that
		 * additional optimization entirely.
		 */
		sortKey->abbreviate = false;

		PrepareSortSupportFromOrderingOp(scan_state->sortOperators[i], sortKey);
	}

	binaryheap_reset(scan_state->ms_heap);
	scan_state->ms_initialized = false;
}

void
runtimemergeappend_explain(CustomScanState *node, List *ancestors, ExplainState *es)
{
	RuntimeMergeAppendState *scan_state = (RuntimeMergeAppendState *) node;

	explain_append_common(node, scan_state->rstate.children_table, es);

	/* We should print sort keys as well */
	show_sort_group_keys((PlanState *) &node->ss.ps, "Sort Key",
						 scan_state->numCols, scan_state->sortColIdx,
						 scan_state->sortOperators, scan_state->collations,
						 scan_state->nullsFirst, ancestors, es);
}


/*
 * Copied from createplan.c
 */

static void
copy_plan_costsize(Plan *dest, Plan *src)
{
	if (src)
	{
		dest->startup_cost = src->startup_cost;
		dest->total_cost = src->total_cost;
		dest->plan_rows = src->plan_rows;
		dest->plan_width = src->plan_width;
	}
	else
	{
		dest->startup_cost = 0;
		dest->total_cost = 0;
		dest->plan_rows = 0;
		dest->plan_width = 0;
	}
}

/* Copied from createplan.c */
static Sort *
make_sort(PlannerInfo *root, Plan *lefttree, int numCols,
		  AttrNumber *sortColIdx, Oid *sortOperators,
		  Oid *collations, bool *nullsFirst,
		  double limit_tuples)
{
	Sort	   *node = makeNode(Sort);
	Plan	   *plan = &node->plan;
	Path		sort_path;		/* dummy for result of cost_sort */

	copy_plan_costsize(plan, lefttree); /* only care about copying size */
	cost_sort(&sort_path, root, NIL,
			  lefttree->total_cost,
			  lefttree->plan_rows,
			  lefttree->plan_width,
			  0.0,
			  work_mem,
			  limit_tuples);
	plan->startup_cost = sort_path.startup_cost;
	plan->total_cost = sort_path.total_cost;
	plan->targetlist = lefttree->targetlist;
	plan->qual = NIL;
	plan->lefttree = lefttree;
	plan->righttree = NULL;
	node->numCols = numCols;
	node->sortColIdx = sortColIdx;
	node->sortOperators = sortOperators;
	node->collations = collations;
	node->nullsFirst = nullsFirst;

	return node;
}

static EquivalenceMember *
find_ec_member_for_tle(EquivalenceClass *ec,
					   TargetEntry *tle,
					   Relids relids)
{
	Expr	   *tlexpr;
	ListCell   *lc;

	/* We ignore binary-compatible relabeling on both ends */
	tlexpr = tle->expr;
	while (tlexpr && IsA(tlexpr, RelabelType))
		tlexpr = ((RelabelType *) tlexpr)->arg;

	foreach(lc, ec->ec_members)
	{
		EquivalenceMember *em = (EquivalenceMember *) lfirst(lc);
		Expr	   *emexpr;

		/*
		 * We shouldn't be trying to sort by an equivalence class that
		 * contains a constant, so no need to consider such cases any further.
		 */
		if (em->em_is_const)
			continue;

		/*
		 * Ignore child members unless they match the rel being sorted.
		 */
		if (em->em_is_child &&
			!bms_equal(em->em_relids, relids))
			continue;

		/* Match if same expression (after stripping relabel) */
		emexpr = em->em_expr;
		while (emexpr && IsA(emexpr, RelabelType))
			emexpr = ((RelabelType *) emexpr)->arg;

		if (equal(emexpr, tlexpr))
			return em;
	}

	return NULL;
}

static Plan *
prepare_sort_from_pathkeys(PlannerInfo *root, Plan *lefttree, List *pathkeys,
						   Relids relids,
						   const AttrNumber *reqColIdx,
						   bool adjust_tlist_in_place,
						   int *p_numsortkeys,
						   AttrNumber **p_sortColIdx,
						   Oid **p_sortOperators,
						   Oid **p_collations,
						   bool **p_nullsFirst)
{
	List	   *tlist = lefttree->targetlist;
	ListCell   *i;
	int			numsortkeys;
	AttrNumber *sortColIdx;
	Oid		   *sortOperators;
	Oid		   *collations;
	bool	   *nullsFirst;

	/*
	 * We will need at most list_length(pathkeys) sort columns; possibly less
	 */
	numsortkeys = list_length(pathkeys);
	sortColIdx = (AttrNumber *) palloc(numsortkeys * sizeof(AttrNumber));
	sortOperators = (Oid *) palloc(numsortkeys * sizeof(Oid));
	collations = (Oid *) palloc(numsortkeys * sizeof(Oid));
	nullsFirst = (bool *) palloc(numsortkeys * sizeof(bool));

	numsortkeys = 0;

	foreach(i, pathkeys)
	{
		PathKey    *pathkey = (PathKey *) lfirst(i);
		EquivalenceClass *ec = pathkey->pk_eclass;
		EquivalenceMember *em;
		TargetEntry *tle = NULL;
		Oid			pk_datatype = InvalidOid;
		Oid			sortop;
		ListCell   *j;

		if (ec->ec_has_volatile)
		{
			/*
			 * If the pathkey's EquivalenceClass is volatile, then it must
			 * have come from an ORDER BY clause, and we have to match it to
			 * that same targetlist entry.
			 */
			if (ec->ec_sortref == 0)	/* can't happen */
				elog(ERROR, "volatile EquivalenceClass has no sortref");
			tle = get_sortgroupref_tle(ec->ec_sortref, tlist);
			Assert(tle);
			Assert(list_length(ec->ec_members) == 1);
			pk_datatype = ((EquivalenceMember *) linitial(ec->ec_members))->em_datatype;
		}
		else if (reqColIdx != NULL)
		{
			/*
			 * If we are given a sort column number to match, only consider
			 * the single TLE at that position.  It's possible that there is
			 * no such TLE, in which case fall through and generate a resjunk
			 * targetentry (we assume this must have happened in the parent
			 * plan as well).  If there is a TLE but it doesn't match the
			 * pathkey's EC, we do the same, which is probably the wrong thing
			 * but we'll leave it to caller to complain about the mismatch.
			 */
			tle = get_tle_by_resno(tlist, reqColIdx[numsortkeys]);
			if (tle)
			{
				em = find_ec_member_for_tle(ec, tle, relids);
				if (em)
				{
					/* found expr at right place in tlist */
					pk_datatype = em->em_datatype;
				}
				else
					tle = NULL;
			}
		}
		else
		{
			/*
			 * Otherwise, we can sort by any non-constant expression listed in
			 * the pathkey's EquivalenceClass.  For now, we take the first
			 * tlist item found in the EC. If there's no match, we'll generate
			 * a resjunk entry using the first EC member that is an expression
			 * in the input's vars.  (The non-const restriction only matters
			 * if the EC is below_outer_join; but if it isn't, it won't
			 * contain consts anyway, else we'd have discarded the pathkey as
			 * redundant.)
			 *
			 * XXX if we have a choice, is there any way of figuring out which
			 * might be cheapest to execute?  (For example, int4lt is likely
			 * much cheaper to execute than numericlt, but both might appear
			 * in the same equivalence class...)  Not clear that we ever will
			 * have an interesting choice in practice, so it may not matter.
			 */
			foreach(j, tlist)
			{
				tle = (TargetEntry *) lfirst(j);
				em = find_ec_member_for_tle(ec, tle, relids);
				if (em)
				{
					/* found expr already in tlist */
					pk_datatype = em->em_datatype;
					break;
				}
				tle = NULL;
			}
		}

		if (!tle)
		{
			/*
			 * No matching tlist item; look for a computable expression. Note
			 * that we treat Aggrefs as if they were variables; this is
			 * necessary when attempting to sort the output from an Agg node
			 * for use in a WindowFunc (since grouping_planner will have
			 * treated the Aggrefs as variables, too).
			 */
			Expr	   *sortexpr = NULL;

			foreach(j, ec->ec_members)
			{
				EquivalenceMember *em = (EquivalenceMember *) lfirst(j);
				List	   *exprvars;
				ListCell   *k;

				/*
				 * We shouldn't be trying to sort by an equivalence class that
				 * contains a constant, so no need to consider such cases any
				 * further.
				 */
				if (em->em_is_const)
					continue;

				/*
				 * Ignore child members unless they match the rel being
				 * sorted.
				 */
				if (em->em_is_child &&
					!bms_equal(em->em_relids, relids))
					continue;

				sortexpr = em->em_expr;
				exprvars = pull_var_clause((Node *) sortexpr,
										   PVC_INCLUDE_AGGREGATES,
										   PVC_INCLUDE_PLACEHOLDERS);
				foreach(k, exprvars)
				{
					if (!tlist_member_ignore_relabel(lfirst(k), tlist))
						break;
				}
				list_free(exprvars);
				if (!k)
				{
					pk_datatype = em->em_datatype;
					break;		/* found usable expression */
				}
			}
			if (!j)
				elog(ERROR, "could not find pathkey item to sort");

			/*
			 * Do we need to insert a Result node?
			 */
			if (!adjust_tlist_in_place &&
				!is_projection_capable_plan(lefttree))
			{
				/* copy needed so we don't modify input's tlist below */
				tlist = copyObject(tlist);
				lefttree = (Plan *) make_result(root, tlist, NULL,
												lefttree);
			}

			/* Don't bother testing is_projection_capable_plan again */
			adjust_tlist_in_place = true;

			/*
			 * Add resjunk entry to input's tlist
			 */
			tle = makeTargetEntry(sortexpr,
								  list_length(tlist) + 1,
								  NULL,
								  true);
			tlist = lappend(tlist, tle);
			lefttree->targetlist = tlist;		/* just in case NIL before */
		}

		/*
		 * Look up the correct sort operator from the PathKey's slightly
		 * abstracted representation.
		 */
		sortop = get_opfamily_member(pathkey->pk_opfamily,
									 pk_datatype,
									 pk_datatype,
									 pathkey->pk_strategy);
		if (!OidIsValid(sortop))	/* should not happen */
			elog(ERROR, "could not find member %d(%u,%u) of opfamily %u",
				 pathkey->pk_strategy, pk_datatype, pk_datatype,
				 pathkey->pk_opfamily);

		/* Add the column to the sort arrays */
		sortColIdx[numsortkeys] = tle->resno;
		sortOperators[numsortkeys] = sortop;
		collations[numsortkeys] = ec->ec_collation;
		nullsFirst[numsortkeys] = pathkey->pk_nulls_first;
		numsortkeys++;
	}

	/* Return results */
	*p_numsortkeys = numsortkeys;
	*p_sortColIdx = sortColIdx;
	*p_sortOperators = sortOperators;
	*p_collations = collations;
	*p_nullsFirst = nullsFirst;

	return lefttree;
}

/*
 * Append nondefault characteristics of the sort ordering of a column to buf
 * (collation, direction, NULLS FIRST/LAST)
 */
static void
show_sortorder_options(StringInfo buf, Node *sortexpr,
					   Oid sortOperator, Oid collation, bool nullsFirst)
{
	Oid			sortcoltype = exprType(sortexpr);
	bool		reverse = false;
	TypeCacheEntry *typentry;

	typentry = lookup_type_cache(sortcoltype,
								 TYPECACHE_LT_OPR | TYPECACHE_GT_OPR);

	/*
	 * Print COLLATE if it's not default.  There are some cases where this is
	 * redundant, eg if expression is a column whose declared collation is
	 * that collation, but it's hard to distinguish that here.
	 */
	if (OidIsValid(collation) && collation != DEFAULT_COLLATION_OID)
	{
		char	   *collname = get_collation_name(collation);

		if (collname == NULL)
			elog(ERROR, "cache lookup failed for collation %u", collation);
		appendStringInfo(buf, " COLLATE %s", quote_identifier(collname));
	}

	/* Print direction if not ASC, or USING if non-default sort operator */
	if (sortOperator == typentry->gt_opr)
	{
		appendStringInfoString(buf, " DESC");
		reverse = true;
	}
	else if (sortOperator != typentry->lt_opr)
	{
		char	   *opname = get_opname(sortOperator);

		if (opname == NULL)
			elog(ERROR, "cache lookup failed for operator %u", sortOperator);
		appendStringInfo(buf, " USING %s", opname);
		/* Determine whether operator would be considered ASC or DESC */
		(void) get_equality_op_for_ordering_op(sortOperator, &reverse);
	}

	/* Add NULLS FIRST/LAST only if it wouldn't be default */
	if (nullsFirst && !reverse)
	{
		appendStringInfoString(buf, " NULLS FIRST");
	}
	else if (!nullsFirst && reverse)
	{
		appendStringInfoString(buf, " NULLS LAST");
	}
}

/*
 * Common code to show sort/group keys, which are represented in plan nodes
 * as arrays of targetlist indexes.  If it's a sort key rather than a group
 * key, also pass sort operators/collations/nullsFirst arrays.
 */
static void
show_sort_group_keys(PlanState *planstate, const char *qlabel,
					 int nkeys, AttrNumber *keycols,
					 Oid *sortOperators, Oid *collations, bool *nullsFirst,
					 List *ancestors, ExplainState *es)
{
	Plan	   *plan = planstate->plan;
	List	   *context;
	List	   *result = NIL;
	StringInfoData sortkeybuf;
	bool		useprefix;
	int			keyno;

	if (nkeys <= 0)
		return;

	initStringInfo(&sortkeybuf);

	/* Set up deparsing context */
	context = set_deparse_context_planstate(es->deparse_cxt,
											(Node *) planstate,
											ancestors);
	useprefix = (list_length(es->rtable) > 1 || es->verbose);

	for (keyno = 0; keyno < nkeys; keyno++)
	{
		/* find key expression in tlist */
		AttrNumber	keyresno = keycols[keyno];
		TargetEntry *target = get_tle_by_resno(plan->targetlist,
											   keyresno);
		char	   *exprstr;

		if (!target)
			elog(ERROR, "no tlist entry for key %d", keyresno);
		/* Deparse the expression, showing any top-level cast */
		exprstr = deparse_expression((Node *) target->expr, context,
									 useprefix, true);
		resetStringInfo(&sortkeybuf);
		appendStringInfoString(&sortkeybuf, exprstr);
		/* Append sort order information, if relevant */
		if (sortOperators != NULL)
			show_sortorder_options(&sortkeybuf,
								   (Node *) target->expr,
								   sortOperators[keyno],
								   collations[keyno],
								   nullsFirst[keyno]);
		/* Emit one property-list item per sort key */
		result = lappend(result, pstrdup(sortkeybuf.data));
	}

	ExplainPropertyList(qlabel, result, es);
}
