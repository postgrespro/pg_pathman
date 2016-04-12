#include "postgres.h"
#include "pickyappend.h"

#include "pathman.h"

#include "optimizer/clauses.h"
#include "optimizer/cost.h"
#include "optimizer/restrictinfo.h"
#include "optimizer/planmain.h"
#include "optimizer/tlist.h"
#include "optimizer/var.h"

set_join_pathlist_hook_type		set_join_pathlist_next = NULL;

CustomPathMethods				pickyappend_path_methods;
CustomScanMethods				pickyappend_plan_methods;
CustomExecMethods				pickyappend_exec_methods;


typedef struct
{
	Oid			relid; /* relid of the corresponding partition */
	PlanState  *ps;
} PreservedPlanState;


static void
clear_plan_states(PickyAppendState *scan_state)
{
	PreservedPlanState *pps;
	HASH_SEQ_STATUS		seqstat;

	hash_seq_init(&seqstat, scan_state->plan_state_table);

	while ((pps = (PreservedPlanState *) hash_seq_search(&seqstat)))
	{
		ExecEndNode(pps->ps);
	}
}

static void
transform_plans_into_states(PickyAppendState *scan_state,
							ChildScanCommon *selected_plans, int n,
							EState *estate)
{
	int i;

	for (i = 0; i < n; i++)
	{
		ChildScanCommon		child = selected_plans[i];
		PreservedPlanState *pps;
		bool				pps_found;

		pps = (PreservedPlanState *) hash_search(scan_state->plan_state_table,
												 (const void *) &child->relid,
												 HASH_ENTER, &pps_found);

		if (!pps_found)
		{
			pps->ps = ExecInitNode(child->content.plan, estate, 0);
			scan_state->css.custom_ps = lappend(scan_state->css.custom_ps, pps->ps);
		}
		else
			ExecReScan(pps->ps);

		child->content_type = CHILD_PLAN_STATE;
		child->content.plan_state = pps->ps;
	}
}

static ChildScanCommon *
select_required_plans(ChildScanCommon *children, int nchildren,
					  Oid *parts, int nparts,
					  int *nres)
{
	ChildScanCommon	   *children_end = children + nchildren;
	Oid				   *parts_end = parts + nparts;
	int					allocated = 10;
	int					used = 0;
	ChildScanCommon	   *result = palloc(10 * sizeof(ChildScanCommon));

	while (children < children_end && parts < parts_end)
	{
		if ((*children)->relid < *parts)
			children++;
		else
		{
			if (!(*parts < (*children)->relid))
			{
				ChildScanCommon child = palloc(sizeof(ChildScanCommonData));

				if (allocated <= used)
				{
					allocated *= 2;
					result = repalloc(result, allocated * sizeof(ChildScanCommon));
				}

				child->content_type = CHILD_PLAN;
				child->content.plan = (*children)->content.plan;
				child->relid = (*children)->relid;

				result[used++] = child;
				children++;
			}
			parts++;
		}
	}

	*nres = used;
	return result;
}

/* qsort comparison function for oids */
static int
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

static Oid *
get_partition_oids(List *ranges, int *n, PartRelationInfo *prel)
{
	ListCell   *range_cell;
	int			allocated = 10;
	int			used = 0;
	Oid		   *result = palloc(allocated * sizeof(Oid));
	Oid		   *children = dsm_array_get_pointer(&prel->children);

	foreach (range_cell, ranges)
	{
		int i;
		int a = irange_lower(lfirst_irange(range_cell));
		int b = irange_upper(lfirst_irange(range_cell));

		for (i = a; i <= b; i++)
		{
			if (allocated <= used)
			{
				allocated *= 2;
				result = repalloc(result, allocated * sizeof(Oid));
			}

			Assert(i < prel->children_count);
			result[used++] = children[i];
		}
	}

	if (used > 1)
		qsort(result, used, sizeof(Oid), oid_cmp);

	*n = used;
	return result;
}

static int
cmp_child_scan_common(const void *a, const void *b)
{
	ChildScanCommon child_a = *(ChildScanCommon *) a;
	ChildScanCommon child_b = *(ChildScanCommon *) b;

	if (child_a->relid > child_b->relid)
		return 1;
	else if (child_a->relid < child_b->relid)
		return -1;
	else
		return 0;
}

static Path *
create_pickyappend_path(PlannerInfo *root,
					    RelOptInfo *joinrel,
					    RelOptInfo *outerrel,
					    RelOptInfo *innerrel,
					    ParamPathInfo *param_info,
					    JoinPathExtraData *extra)
{
	AppendPath	   *inner_append = (AppendPath *) innerrel->cheapest_total_path;
	List		   *joinrestrictclauses = extra->restrictlist;
	List		   *joinclauses;
	List		   *otherclauses;
	ListCell	   *lc;
	int				i;

	RangeTblEntry  *inner_entry = root->simple_rte_array[innerrel->relid];

	PickyAppendPath *result;

	result = palloc0(sizeof(PickyAppendPath));
	NodeSetTag(result, T_CustomPath);

	if (IS_OUTER_JOIN(extra->sjinfo->jointype))
	{
		extract_actual_join_clauses(joinrestrictclauses,
									&joinclauses, &otherclauses);
	}
	else
	{
		/* We can treat all clauses alike for an inner join */
		joinclauses = extract_actual_clauses(joinrestrictclauses, false);
		otherclauses = NIL;
	}

	result->cpath.path.pathtype = T_CustomScan;
	result->cpath.path.parent = innerrel;
	result->cpath.path.param_info = param_info;
	result->cpath.path.pathkeys = NIL;
#if PG_VERSION_NUM >= 90600
	result->cpath.path.pathtarget = inner_append->path.pathtarget;
#endif
	result->cpath.path.rows = inner_append->path.rows;
	result->cpath.flags = 0;
	result->cpath.methods = &pickyappend_path_methods;

	/* TODO: real costs */
	result->cpath.path.startup_cost = 0;
	result->cpath.path.total_cost = 0;

	/* Set 'partitioned column'-related clauses */
	result->cpath.custom_private = joinclauses;
	result->cpath.custom_paths = NIL;

	Assert(inner_entry->relid != 0);
	result->relid = inner_entry->relid;

	result->nchildren = list_length(inner_append->subpaths);
	result->children = palloc(result->nchildren * sizeof(ChildScanCommon));
	i = 0;
	foreach (lc, inner_append->subpaths)
	{
		Path		   *path = lfirst(lc);
		Index			relindex = path->parent->relid;
		ChildScanCommon	child = palloc(sizeof(ChildScanCommonData));

		child->content_type = CHILD_PATH;
		child->content.path = path;
		child->relid = root->simple_rte_array[relindex]->relid;
		Assert(child->relid != InvalidOid);

		result->children[i++] = child;
	}

	qsort(result->children, result->nchildren,
		  sizeof(ChildScanCommon), cmp_child_scan_common);

	/* Fill 'custom_paths' with paths in sort order */
	for (i = 0; i < result->nchildren; i++)
		result->cpath.custom_paths = lappend(result->cpath.custom_paths,
											 result->children[i]->content.path);

	return &result->cpath.path;
}

void
pathman_join_pathlist_hook(PlannerInfo *root,
						   RelOptInfo *joinrel,
						   RelOptInfo *outerrel,
						   RelOptInfo *innerrel,
						   JoinType jointype,
						   JoinPathExtraData *extra)
{
	JoinCostWorkspace	workspace;
	Path			   *outer,
					   *inner;
	Relids				inner_required;
	RangeTblEntry	   *inner_entry = root->simple_rte_array[innerrel->relid];
	PartRelationInfo   *inner_prel;
	NestPath		   *nest_path;
	List			   *pathkeys = NIL;

	if (set_join_pathlist_next)
		set_join_pathlist_next(root, joinrel, outerrel,
							   innerrel, jointype, extra);

	if (jointype == JOIN_FULL)
		return;

	if (innerrel->reloptkind == RELOPT_BASEREL &&
		inner_entry->inh &&
		IsA(innerrel->cheapest_total_path, AppendPath) &&
		(inner_prel = get_pathman_relation_info(inner_entry->relid, NULL)))
	{
		elog(LOG, "adding new path !!!");
		//pprint(innerrel->cheapest_total_path);
	}
	else return;

	outer = outerrel->cheapest_total_path;

	inner_required = bms_union(PATH_REQ_OUTER(innerrel->cheapest_total_path),
							   bms_make_singleton(outerrel->relid));

	inner = create_pickyappend_path(root, joinrel, outerrel, innerrel,
									get_appendrel_parampathinfo(innerrel,
																inner_required),
									extra);

	initial_cost_nestloop(root, &workspace, jointype,
						  outer, inner,
						  extra->sjinfo, &extra->semifactors);

	pathkeys = build_join_pathkeys(root, joinrel, jointype, outer->pathkeys);

	nest_path = create_nestloop_path(root, joinrel, jointype, &workspace,
									 extra->sjinfo, &extra->semifactors,
									 outer, inner, extra->restrictlist,
									 pathkeys,
									 calc_nestloop_required_outer(outer, inner));

	add_path(joinrel, (Path *) nest_path);
}

static void
save_pickyappend_private(CustomScan *cscan, PickyAppendPath *path)
{
	ChildScanCommon    *children = path->children;
	int					nchildren = path->nchildren;
	List			   *custom_private = NIL;
	List			   *custom_oids = NIL;
	int					i;

	for (i = 0; i < nchildren; i++)
	{
		/* We've already filled 'custom_paths' in create_pickyappend_path */
		custom_oids = lappend_oid(custom_oids, children[i]->relid);
		pfree(children[i]);
	}

	custom_private = list_make2(list_make1_oid(path->relid), custom_oids);

	cscan->custom_private = custom_private;
}

static void
unpack_pickyappend_private(PickyAppendState *scan_state, CustomScan *cscan)
{
	List			   *custom_oids = (List *) lsecond(cscan->custom_private);
	int					nchildren = list_length(custom_oids);
	ChildScanCommon    *children = palloc(nchildren * sizeof(ChildScanCommon));
	ListCell		   *cur_oid;
	ListCell		   *cur_plan;
	int					i;

	i = 0;
	forboth (cur_oid, custom_oids, cur_plan, cscan->custom_plans)
	{
		ChildScanCommon child = palloc(sizeof(ChildScanCommonData));

		child->content_type = CHILD_PLAN;
		child->content.plan = (Plan *) lfirst(cur_plan);
		child->relid = lfirst_oid(cur_oid);

		children[i++] = child;
	}

	scan_state->relid = linitial_oid(linitial(cscan->custom_private));
	scan_state->children = children;
	scan_state->nchildren = nchildren;
}

Plan *
create_pickyappend_plan(PlannerInfo *root, RelOptInfo *rel,
						CustomPath *best_path, List *tlist,
						List *clauses, List *custom_plans)
{
	PickyAppendPath    *gpath = (PickyAppendPath *) best_path;
	CustomScan		   *cscan;

	cscan = makeNode(CustomScan);
	cscan->scan.plan.qual = NIL;
	cscan->scan.plan.targetlist = tlist;
	cscan->custom_scan_tlist = tlist;
	cscan->scan.scanrelid = 0;

	cscan->custom_exprs = gpath->cpath.custom_private;
	cscan->custom_plans = custom_plans;

	cscan->methods = &pickyappend_plan_methods;

	save_pickyappend_private(cscan, gpath);

	return &cscan->scan.plan;
}

Node *
pickyappend_create_scan_state(CustomScan *node)
{
	PickyAppendState *scan_state = palloc0(sizeof(PickyAppendState));

	NodeSetTag(scan_state, T_CustomScanState);
	scan_state->css.flags = node->flags;
	scan_state->css.methods = &pickyappend_exec_methods;
	scan_state->custom_exprs = node->custom_exprs;

	unpack_pickyappend_private(scan_state, node);

	scan_state->prel = get_pathman_relation_info(scan_state->relid, NULL);

	scan_state->cur_plans = NULL;
	scan_state->ncur_plans = 0;
	scan_state->running_idx = 0;

	return (Node *) scan_state;
}

void
pickyappend_begin(CustomScanState *node, EState *estate, int eflags)
{
	PickyAppendState   *scan_state = (PickyAppendState *) node;
	HTAB			   *plan_state_table = scan_state->plan_state_table;
	HASHCTL			   *plan_state_table_config = &scan_state->plan_state_table_config;

	memset(plan_state_table_config, 0, sizeof(HASHCTL));
	plan_state_table_config->keysize = sizeof(Oid);
	plan_state_table_config->entrysize = sizeof(PreservedPlanState);

	plan_state_table = hash_create("PlanState storage", 128,
								   plan_state_table_config, HASH_ELEM | HASH_BLOBS);

	scan_state->plan_state_table = plan_state_table;
	scan_state->custom_expr_states = (List *) ExecInitExpr((Expr *) scan_state->custom_exprs,
														   (PlanState *) scan_state);
}

TupleTableSlot *
pickyappend_exec(CustomScanState *node)
{
	PickyAppendState   *scan_state = (PickyAppendState *) node;

	while (scan_state->running_idx < scan_state->ncur_plans)
	{
		ChildScanCommon		child = scan_state->cur_plans[scan_state->running_idx];
		PlanState		   *state = child->content.plan_state;
		TupleTableSlot	   *slot = NULL;
		bool				quals;

		for (;;)
		{
			slot = ExecProcNode(state);

			if (TupIsNull(slot))
				break;

			node->ss.ps.ps_ExprContext->ecxt_scantuple = slot;
			quals = ExecQual(scan_state->custom_expr_states,
							 node->ss.ps.ps_ExprContext, false);

			if (quals)
				return slot;
		}

		scan_state->running_idx++;
	}

	return NULL;
}

void
pickyappend_end(CustomScanState *node)
{
	PickyAppendState   *scan_state = (PickyAppendState *) node;

	clear_plan_states(scan_state);
	hash_destroy(scan_state->plan_state_table);
}

void
pickyappend_rescan(CustomScanState *node)
{
	PickyAppendState   *scan_state = (PickyAppendState *) node;
	ExprContext		   *econtext = node->ss.ps.ps_ExprContext;
	PartRelationInfo   *prel = scan_state->prel;
	List			   *ranges;
	ListCell		   *lc;
	Oid				   *parts;
	int					nparts;

	ranges = list_make1_int(make_irange(0, prel->children_count - 1, false));

	foreach (lc, scan_state->custom_exprs)
	{
		WrapperNode	   *wn;
		WalkerContext	wcxt;

		wcxt.econtext = econtext;
		wn = walk_expr_tree(&wcxt, (Expr *) lfirst(lc), prel);

		ranges = irange_list_intersect(ranges, wn->rangeset);
	}

	parts = get_partition_oids(ranges, &nparts, prel);

	scan_state->cur_plans = select_required_plans(scan_state->children,
												  scan_state->nchildren,
												  parts, nparts,
												  &scan_state->ncur_plans);
	pfree(parts);

	transform_plans_into_states(scan_state,
								scan_state->cur_plans,
								scan_state->ncur_plans,
								scan_state->css.ss.ps.state);

	scan_state->running_idx = 0;

	/* elog(LOG, "nparts: %d, plans: %d", nparts, nplans); */
}

void
pickyppend_explain(CustomScanState *node, List *ancestors, ExplainState *es)
{
}
