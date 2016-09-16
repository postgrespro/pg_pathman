/* ------------------------------------------------------------------------
 *
 * nodes_common.c
 *		Common code for custom nodes
 *
 * Copyright (c) 2016, Postgres Professional
 *
 * ------------------------------------------------------------------------
 */

#include "nodes_common.h"
#include "runtimeappend.h"
#include "utils.h"

#include "access/sysattr.h"
#include "optimizer/restrictinfo.h"
#include "optimizer/var.h"
#include "utils/memutils.h"


/* Allocation settings */
#define INITIAL_ALLOC_NUM	10
#define ALLOC_EXP			2


/* Compare plans by 'original_order' */
static int
cmp_child_scan_common_by_orig_order(const void *ap,
									const void *bp)
{
	ChildScanCommon a = *(ChildScanCommon *) ap;
	ChildScanCommon b = *(ChildScanCommon *) bp;

	if (a->original_order > b->original_order)
		return 1;
	else if (a->original_order < b->original_order)
		return -1;
	else
		return 0;
}

static void
transform_plans_into_states(RuntimeAppendState *scan_state,
							ChildScanCommon *selected_plans, int n,
							EState *estate)
{
	int i;

	for (i = 0; i < n; i++)
	{
		ChildScanCommon		child = selected_plans[i];
		PlanState		   *ps;

		/* Create new node since this plan hasn't been used yet */
		if (child->content_type != CHILD_PLAN_STATE)
		{
			Assert(child->content_type == CHILD_PLAN); /* no paths allowed */

			ps = ExecInitNode(child->content.plan, estate, 0);
			child->content.plan_state = ps;
			child->content_type = CHILD_PLAN_STATE; /* update content type */

			/* Explain and clear_plan_states rely on this list */
			scan_state->css.custom_ps = lappend(scan_state->css.custom_ps, ps);
		}
		else
			ps = child->content.plan_state;

		/* Node with params will be ReScanned */
		if (scan_state->css.ss.ps.chgParam)
			UpdateChangedParamSet(ps, scan_state->css.ss.ps.chgParam);

		/*
		 * We should ReScan this node manually since
		 * ExecProcNode won't do this for us in this case.
		 */
		if (bms_is_empty(ps->chgParam))
			ExecReScan(ps);

		child->content.plan_state = ps;
	}
}

static ChildScanCommon *
select_required_plans(HTAB *children_table, Oid *parts, int nparts, int *nres)
{
	uint32				allocated = INITIAL_ALLOC_NUM,
						used = 0;
	ChildScanCommon	   *result;
	int					i;

	result = (ChildScanCommon *) palloc(allocated * sizeof(ChildScanCommon));

	for (i = 0; i < nparts; i++)
	{
		ChildScanCommon child = hash_search(children_table,
											(const void *) &parts[i],
											HASH_FIND, NULL);
		if (!child)
			continue; /* no plan for this partition */

		if (allocated <= used)
		{
			allocated = allocated * ALLOC_EXP + 1;
			result = repalloc(result, allocated * sizeof(ChildScanCommon));
		}

		result[used++] = child;
	}

	*nres = used;
	return result;
}

/* Replace Vars' varnos with the value provided by 'parent' */
static List *
replace_tlist_varnos(List *child_tlist, RelOptInfo *parent)
{
	ListCell   *lc;
	List	   *result = NIL;
	int			i = 1; /* resnos begin with 1 */

	foreach (lc, child_tlist)
	{
		Var *var = (Var *) ((TargetEntry *) lfirst(lc))->expr;
		Var *newvar = (Var *) palloc(sizeof(Var));

		Assert(IsA(var, Var));

		*newvar = *var;
		newvar->varno = parent->relid;
		newvar->varnoold = parent->relid;

		result = lappend(result, makeTargetEntry((Expr *) newvar,
												 i++, /* item's index */
												 NULL, false));
	}

	return result;
}

/* Append partition attribute in case it's not present in target list */
static List *
append_part_attr_to_tlist(List *tlist, Index relno, const PartRelationInfo *prel)
{
	ListCell   *lc;
	bool		part_attr_found = false;

	foreach (lc, tlist)
	{
		TargetEntry *te = (TargetEntry *) lfirst(lc);
		Var			*var = (Var *) te->expr;

		if (IsA(var, Var) && var->varoattno == prel->attnum)
			part_attr_found = true;
	}

	if (!part_attr_found)
	{
		Var	   *newvar = makeVar(relno,
								 prel->attnum,
								 prel->atttype,
								 prel->atttypmod,
								 prel->attcollid,
								 0);

		Index	last_item = list_length(tlist) + 1;

		tlist = lappend(tlist, makeTargetEntry((Expr *) newvar,
											   last_item,
											   NULL, false));
	}

	return tlist;
}

static void
pack_runtimeappend_private(CustomScan *cscan, RuntimeAppendPath *path,
						   bool enable_parent)
{
	ChildScanCommon    *children = path->children;
	int					nchildren = path->nchildren;
	List			   *custom_private = NIL,
					   *custom_oids = NIL;
	int					i;

	for (i = 0; i < nchildren; i++)
	{
		/* We've already filled 'custom_paths' in create_runtimeappend_path */
		custom_oids = lappend_oid(custom_oids, children[i]->relid);
		pfree(children[i]);
	}

	/* Save parent & partition Oids and a flag as first element of 'custom_private' */
	custom_private = lappend(custom_private,
							 list_make3(list_make1_oid(path->relid),
										custom_oids, /* list of Oids */
										list_make1_int(enable_parent)));

	/* Store freshly built 'custom_private' */
	cscan->custom_private = custom_private;
}

static void
unpack_runtimeappend_private(RuntimeAppendState *scan_state, CustomScan *cscan)
{
	ListCell   *oid_cell,
			   *plan_cell;
	List	   *runtimeappend_private = linitial(cscan->custom_private),
			   *custom_oids;		/* Oids of partitions */
	int			custom_oids_count;	/* number of partitions */

	HTAB	   *children_table;
	HASHCTL	   *children_table_config = &scan_state->children_table_config;
	int			i;

	/* Extract Oids list from packed data */
	custom_oids = (List *) lsecond(runtimeappend_private);
	custom_oids_count = list_length(custom_oids);

	memset(children_table_config, 0, sizeof(HASHCTL));
	children_table_config->keysize = sizeof(Oid);
	children_table_config->entrysize = sizeof(ChildScanCommonData);

	children_table = hash_create("RuntimeAppend plan storage",
								 custom_oids_count,
								 children_table_config,
								 HASH_ELEM | HASH_BLOBS);

	i = 0;
	forboth (oid_cell, custom_oids, plan_cell, cscan->custom_plans)
	{
		bool				child_found;
		Oid					cur_oid = lfirst_oid(oid_cell);

		ChildScanCommon		child = hash_search(children_table,
												(const void *) &cur_oid,
												HASH_ENTER, &child_found);

		Assert(!child_found); /* there should be no collisions */

		child->content_type = CHILD_PLAN;
		child->content.plan = (Plan *) lfirst(plan_cell);
		child->original_order = i++; /* will be used in EXPLAIN */
	}

	/* Finally fill 'scan_state' with unpacked elements */
	scan_state->children_table = children_table;
	scan_state->relid = linitial_oid(linitial(runtimeappend_private));
	scan_state->enable_parent = (bool) linitial_int(lthird(runtimeappend_private));
}

/*
 * Filter all available clauses and extract relevant ones.
 */
List *
get_partitioned_attr_clauses(List *restrictinfo_list,
							 const PartRelationInfo *prel,
							 Index partitioned_rel)
{
#define AdjustAttno(attno) \
	( (AttrNumber) (attno + FirstLowInvalidHeapAttributeNumber) )

	List	   *result = NIL;
	ListCell   *l;

	foreach(l, restrictinfo_list)
	{
		RestrictInfo   *rinfo = (RestrictInfo *) lfirst(l);
		Bitmapset	   *varattnos = NULL;
		int				part_attno;

		Assert(IsA(rinfo, RestrictInfo));
		pull_varattnos((Node *) rinfo->clause, partitioned_rel, &varattnos);

		if (bms_get_singleton_member(varattnos, &part_attno) &&
			AdjustAttno(part_attno) == prel->attnum)
		{
			result = lappend(result, rinfo->clause);
		}
	}
	return result;
}


/* Transform partition ranges into plain array of partition Oids */
Oid *
get_partition_oids(List *ranges, int *n, const PartRelationInfo *prel,
				   bool include_parent)
{
	ListCell   *range_cell;
	uint32		allocated = INITIAL_ALLOC_NUM,
				used = 0;
	Oid		   *result = (Oid *) palloc(allocated * sizeof(Oid));
	Oid		   *children = PrelGetChildrenArray(prel);

	/* If required, add parent to result */
	Assert(INITIAL_ALLOC_NUM >= 1);
	if (include_parent)
		result[used++] = PrelParentRelid(prel);

	/* Deal with selected partitions */
	foreach (range_cell, ranges)
	{
		uint32	i;
		uint32	a = lfirst_irange(range_cell).ir_lower,
				b = lfirst_irange(range_cell).ir_upper;

		for (i = a; i <= b; i++)
		{
			if (allocated <= used)
			{
				allocated = allocated * ALLOC_EXP + 1;
				result = repalloc(result, allocated * sizeof(Oid));
			}

			Assert(i < PrelChildrenCount(prel));
			result[used++] = children[i];
		}
	}

	*n = used;
	return result;
}

Path *
create_append_path_common(PlannerInfo *root,
						  AppendPath *inner_append,
						  ParamPathInfo *param_info,
						  CustomPathMethods *path_methods,
						  uint32 size,
						  double sel)
{
	RelOptInfo		   *innerrel = inner_append->path.parent;
	ListCell		   *lc;
	int					i;

	RangeTblEntry	   *inner_entry = root->simple_rte_array[innerrel->relid];

	RuntimeAppendPath  *result;

	result = (RuntimeAppendPath *) palloc0(size);
	NodeSetTag(result, T_CustomPath);

	result->cpath.path.pathtype = T_CustomScan;
	result->cpath.path.parent = innerrel;
	result->cpath.path.param_info = param_info;
	result->cpath.path.pathkeys = inner_append->path.pathkeys;
#if PG_VERSION_NUM >= 90600
	result->cpath.path.pathtarget = inner_append->path.pathtarget;
#endif
	result->cpath.path.rows = inner_append->path.rows * sel;
	result->cpath.flags = 0;
	result->cpath.methods = path_methods;

	result->cpath.path.startup_cost = 0.0;
	result->cpath.path.total_cost = 0.0;

	Assert(inner_entry->relid != 0);
	result->relid = inner_entry->relid;

	result->nchildren = list_length(inner_append->subpaths);
	result->children = (ChildScanCommon *)
			palloc(result->nchildren * sizeof(ChildScanCommon));
	i = 0;
	foreach (lc, inner_append->subpaths)
	{
		Path			   *path = lfirst(lc);
		Index				relindex = path->parent->relid;
		ChildScanCommon		child;

		child = (ChildScanCommon) palloc(sizeof(ChildScanCommonData));

		result->cpath.path.startup_cost += path->startup_cost;
		result->cpath.path.total_cost += path->total_cost;

		child->content_type = CHILD_PATH;
		child->content.path = path;
		child->relid = root->simple_rte_array[relindex]->relid;
		Assert(child->relid != InvalidOid);

		result->cpath.custom_paths = lappend(result->cpath.custom_paths,
											 child->content.path);
		result->children[i] = child;

		i++;
	}

	result->cpath.path.startup_cost *= sel;
	result->cpath.path.total_cost *= sel;

	return &result->cpath.path;
}

Plan *
create_append_plan_common(PlannerInfo *root, RelOptInfo *rel,
						  CustomPath *best_path, List *tlist,
						  List *clauses, List *custom_plans,
						  CustomScanMethods *scan_methods)
{
	RuntimeAppendPath	   *rpath = (RuntimeAppendPath *) best_path;
	const PartRelationInfo *prel;
	CustomScan			   *cscan;

	prel = get_pathman_relation_info(rpath->relid);
	Assert(prel);

	cscan = makeNode(CustomScan);
	cscan->custom_scan_tlist = NIL; /* initial value (empty list) */
	cscan->scan.plan.targetlist = NIL;

	if (custom_plans)
	{
		ListCell   *lc1,
				   *lc2;

		forboth (lc1, rpath->cpath.custom_paths, lc2, custom_plans)
		{
			Plan		   *child_plan = (Plan *) lfirst(lc2);
			RelOptInfo 	   *child_rel = ((Path *) lfirst(lc1))->parent;

			/* Replace rel's tlist with a matching one */
			if (!cscan->scan.plan.targetlist)
				tlist = replace_tlist_varnos(child_plan->targetlist, rel);

			/* Add partition attribute if necessary (for ExecQual()) */
			child_plan->targetlist = append_part_attr_to_tlist(child_plan->targetlist,
															   child_rel->relid,
															   prel);

			/* Now make custom_scan_tlist match child plans' targetlists */
			if (!cscan->custom_scan_tlist)
				cscan->custom_scan_tlist = replace_tlist_varnos(child_plan->targetlist,
																rel);
		}
	}

	cscan->scan.plan.qual = NIL;
	cscan->scan.plan.targetlist = tlist;

	/* Since we're not scanning any real table directly */
	cscan->scan.scanrelid = 0;

	cscan->custom_exprs = get_partitioned_attr_clauses(clauses, prel, rel->relid);
	cscan->custom_plans = custom_plans;
	cscan->methods = scan_methods;

	/* Cache 'prel->enable_parent' as well */
	pack_runtimeappend_private(cscan, rpath, prel->enable_parent);

	return &cscan->scan.plan;
}

Node *
create_append_scan_state_common(CustomScan *node,
								CustomExecMethods *exec_methods,
								uint32 size)
{
	RuntimeAppendState *scan_state;

	scan_state = (RuntimeAppendState *) palloc0(size);
	NodeSetTag(scan_state, T_CustomScanState);

	scan_state->css.flags = node->flags;
	scan_state->css.methods = exec_methods;
	scan_state->custom_exprs = node->custom_exprs;

	unpack_runtimeappend_private(scan_state, node);

	scan_state->cur_plans = NULL;
	scan_state->ncur_plans = 0;
	scan_state->running_idx = 0;

	return (Node *) scan_state;
}

void
begin_append_common(CustomScanState *node, EState *estate, int eflags)
{
	RuntimeAppendState *scan_state = (RuntimeAppendState *) node;

	scan_state->custom_expr_states =
		(List *) ExecInitExpr((Expr *) scan_state->custom_exprs,
							  (PlanState *) scan_state);

	node->ss.ps.ps_TupFromTlist = false;
}

TupleTableSlot *
exec_append_common(CustomScanState *node,
				   void (*fetch_next_tuple) (CustomScanState *node))
{
	RuntimeAppendState	   *scan_state = (RuntimeAppendState *) node;

	/* ReScan if no plans are selected */
	if (scan_state->ncur_plans == 0)
		ExecReScan(&node->ss.ps);

	for (;;)
	{
		/* Fetch next tuple if we're done with Projections */
		if (!node->ss.ps.ps_TupFromTlist)
		{
			fetch_next_tuple(node); /* use specific callback */

			if (TupIsNull(scan_state->slot))
				return NULL;
		}

		if (node->ss.ps.ps_ProjInfo)
		{
			ExprDoneCond	isDone;
			TupleTableSlot *result;

			ResetExprContext(node->ss.ps.ps_ExprContext);

			node->ss.ps.ps_ProjInfo->pi_exprContext->ecxt_scantuple = scan_state->slot;
			result = ExecProject(node->ss.ps.ps_ProjInfo, &isDone);

			if (isDone != ExprEndResult)
			{
				node->ss.ps.ps_TupFromTlist = (isDone == ExprMultipleResult);

				return result;
			}
			else
				node->ss.ps.ps_TupFromTlist = false;
		}
		else
			return scan_state->slot;
	}
}

void
end_append_common(CustomScanState *node)
{
	RuntimeAppendState *scan_state = (RuntimeAppendState *) node;

	clear_plan_states(&scan_state->css);
	hash_destroy(scan_state->children_table);
}

void
rescan_append_common(CustomScanState *node)
{
	RuntimeAppendState	   *scan_state = (RuntimeAppendState *) node;
	ExprContext			   *econtext = node->ss.ps.ps_ExprContext;
	const PartRelationInfo *prel;
	List				   *ranges;
	ListCell			   *lc;
	WalkerContext			wcxt;
	Oid					   *parts;
	int						nparts;

	prel = get_pathman_relation_info(scan_state->relid);
	Assert(prel);

	/* First we select all available partitions... */
	ranges = list_make1_irange(make_irange(0, PrelLastChild(prel), false));

	InitWalkerContext(&wcxt, prel, econtext, false);
	foreach (lc, scan_state->custom_exprs)
	{
		WrapperNode	   *wn;

		/* ... then we cut off irrelevant ones using the provided clauses */
		wn = walk_expr_tree((Expr *) lfirst(lc), &wcxt);
		ranges = irange_list_intersect(ranges, wn->rangeset);
	}

	/* Get Oids of the required partitions */
	parts = get_partition_oids(ranges, &nparts, prel, scan_state->enable_parent);

	/* Select new plans for this run using 'parts' */
	if (scan_state->cur_plans)
		pfree(scan_state->cur_plans); /* shallow free since cur_plans
									   * belong to children_table  */
	scan_state->cur_plans = select_required_plans(scan_state->children_table,
												  parts, nparts,
												  &scan_state->ncur_plans);
	pfree(parts);

	/* Transform selected plans into executable plan states */
	transform_plans_into_states(scan_state,
								scan_state->cur_plans,
								scan_state->ncur_plans,
								scan_state->css.ss.ps.state);

	scan_state->running_idx = 0;
}

void
explain_append_common(CustomScanState *node, HTAB *children_table, ExplainState *es)
{
	/* Construct excess PlanStates */
	if (!es->analyze)
	{
		uint32				allocated = INITIAL_ALLOC_NUM,
							used = 0;
		ChildScanCommon	   *custom_ps,
							child;
		HASH_SEQ_STATUS		seqstat;
		int					i;

		custom_ps = (ChildScanCommon *) palloc(allocated * sizeof(ChildScanCommon));

		/* There can't be any nodes since we're not scanning anything */
		Assert(!node->custom_ps);

		/* Iterate through node's ChildScanCommon table */
		hash_seq_init(&seqstat, children_table);

		while ((child = (ChildScanCommon) hash_seq_search(&seqstat)))
		{
			if (allocated <= used)
			{
				allocated = allocated * ALLOC_EXP + 1;
				custom_ps = repalloc(custom_ps, allocated * sizeof(ChildScanCommon));
			}

			custom_ps[used++] = child;
		}

		/*
		 * We have to restore the original plan order
		 * which has been lost within the hash table
		 */
		qsort(custom_ps, used, sizeof(ChildScanCommon),
			  cmp_child_scan_common_by_orig_order);

		/*
		 * These PlanStates will be used by EXPLAIN,
		 * end_append_common will destroy them eventually
		 */
		for (i = 0; i < used; i++)
			node->custom_ps = lappend(node->custom_ps,
									  ExecInitNode(custom_ps[i]->content.plan,
												   node->ss.ps.state,
												   0));
	}
}
