#include "postgres.h"
#include "pickyappend.h"
#include "pathman.h"


bool pg_pathman_enable_pickyappend = true;

CustomPathMethods				pickyappend_path_methods;
CustomScanMethods				pickyappend_plan_methods;
CustomExecMethods				pickyappend_exec_methods;


static void
free_child_scan_common_array(ChildScanCommon *cur_plans, int n)
{
	int i;

	if (!cur_plans)
		return;

	/* We shouldn't free inner objects e.g. Plans here */
	for (i = 0; i < n; i++)
		pfree(cur_plans[i]);

	pfree(cur_plans);
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

		/* Create new node since this plan hasn't been used yet */
		if (!pps_found)
		{
			pps->ps = ExecInitNode(child->content.plan, estate, 0);
			/* Explain and clear_plan_states rely on this list */
			scan_state->css.custom_ps = lappend(scan_state->css.custom_ps, pps->ps);
		}

		/* Node with params will be ReScanned */
		if (scan_state->css.ss.ps.chgParam)
			UpdateChangedParamSet(pps->ps, scan_state->css.ss.ps.chgParam);

		/*
		 * We should ReScan this node manually since
		 * ExecProcNode won't do this for us in this case.
		 */
		if (!pps->ps->chgParam)
			ExecReScan(pps->ps);

		child->content.plan_state = pps->ps;
	}
}

static ChildScanCommon *
select_required_plans(HTAB *children_table, Oid *parts, int nparts, int *nres)
{
	int					allocated = 10;
	int					used = 0;
	ChildScanCommon	   *result = palloc(10 * sizeof(ChildScanCommon));
	int					i;

	for (i = 0; i < nparts; i++)
	{
		ChildScanCommon child_copy;
		ChildScanCommon child = hash_search(children_table,
											(const void *) &parts[i],
											HASH_FIND, NULL);
		if (!child)
			continue;

		if (allocated <= used)
		{
			allocated *= 2;
			result = repalloc(result, allocated * sizeof(ChildScanCommon));
		}

		child_copy = palloc(sizeof(ChildScanCommonData));
		memcpy(child_copy, child, sizeof(ChildScanCommonData));

		result[used++] = child_copy;
	}

	*nres = used;
	return result;
}

/* Transform partition ranges into plain array of partition Oids */
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

	*n = used;
	return result;
}

Path *
create_pickyappend_path(PlannerInfo *root,
						AppendPath *inner_append,
						ParamPathInfo *param_info,
						List *picky_clauses)
{
	RelOptInfo *innerrel = inner_append->path.parent;
	ListCell   *lc;
	int			i;

	RangeTblEntry  *inner_entry = root->simple_rte_array[innerrel->relid];

	PickyAppendPath *result;

	result = palloc0(sizeof(PickyAppendPath));
	NodeSetTag(result, T_CustomPath);

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
	result->cpath.custom_private = picky_clauses;
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

		child->content.path = path;
		child->relid = root->simple_rte_array[relindex]->relid;
		Assert(child->relid != InvalidOid);

		result->cpath.custom_paths = lappend(result->cpath.custom_paths,
											 child->content.path);
		result->children[i] = child;

		i++;
	}

	return &result->cpath.path;
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

	/* Save main table and partition relids */
	custom_private = list_make2(list_make1_oid(path->relid), custom_oids);

	cscan->custom_private = custom_private;
}

static void
unpack_pickyappend_private(PickyAppendState *scan_state, CustomScan *cscan)
{
	ListCell   *oid_cell;
	ListCell   *plan_cell;
	List	   *custom_oids = (List *) lsecond(cscan->custom_private);
	int			nchildren = list_length(custom_oids);
	HTAB	   *children_table = scan_state->children_table;
	HASHCTL	   *children_table_config = &scan_state->children_table_config;
	int			i;

	memset(children_table_config, 0, sizeof(HASHCTL));
	children_table_config->keysize = sizeof(Oid);
	children_table_config->entrysize = sizeof(ChildScanCommonData);

	children_table = hash_create("Plan storage", nchildren,
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

		child->content.plan = (Plan *) lfirst(plan_cell);
		child->original_order = i++; /* will be used in EXPLAIN */
	}

	scan_state->children_table = children_table;
	scan_state->relid = linitial_oid(linitial(cscan->custom_private));
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

	/* Fill in relation info using main table's relid */
	scan_state->prel = get_pathman_relation_info(scan_state->relid, NULL);
	Assert(scan_state->prel);

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
								   plan_state_table_config,
								   HASH_ELEM | HASH_BLOBS);

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

	clear_plan_states(&scan_state->css);
	hash_destroy(scan_state->plan_state_table);
	hash_destroy(scan_state->children_table);
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

	/* Get Oids of the required partitions */
	parts = get_partition_oids(ranges, &nparts, prel);

	/* Select new plans for this run using 'parts' */
	free_child_scan_common_array(scan_state->cur_plans, scan_state->ncur_plans);
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
pickyppend_explain(CustomScanState *node, List *ancestors, ExplainState *es)
{
	PickyAppendState   *scan_state = (PickyAppendState *) node;

	explain_common(node, scan_state->children_table, es);
}
