/* ------------------------------------------------------------------------
 *
 * nodes_common.c
 *		Common code for custom nodes
 *
 * Copyright (c) 2016, Postgres Professional
 *
 * ------------------------------------------------------------------------
 */
#include "compat/pg_compat.h"

#include "init.h"
#include "nodes_common.h"
#include "runtime_append.h"
#include "utils.h"

#include "nodes/nodeFuncs.h"
#include "optimizer/clauses.h"
#include "optimizer/tlist.h"
#include "optimizer/var.h"
#include "rewrite/rewriteManip.h"
#include "utils/memutils.h"
#include "utils/ruleutils.h"


/* Allocation settings */
#define INITIAL_ALLOC_NUM	10


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
		ChildScanCommon		child;
		PlanState		   *ps;

		AssertArg(selected_plans);
		child = selected_plans[i];

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
	uint32				allocated,
						used;
	ChildScanCommon	   *result;
	int					i;

	ArrayAlloc(result, allocated, used, INITIAL_ALLOC_NUM);

	for (i = 0; i < nparts; i++)
	{
		ChildScanCommon child = hash_search(children_table,
											(const void *) &parts[i],
											HASH_FIND, NULL);
		if (!child)
			continue; /* no plan for this partition */

		ArrayPush(result, allocated, used, child);
	}

	/* Get rid of useless array */
	if (used == 0)
	{
		pfree(result);
		result = NULL;
	}

	*nres = used;
	return result;
}

/* Adapt child's tlist for parent relation (change varnos and varattnos) */
static List *
build_parent_tlist(List *tlist, AppendRelInfo *appinfo)
{
	List	   *temp_tlist,
			   *pulled_vars;
	ListCell   *lc1,
			   *lc2;

	temp_tlist = copyObject(tlist);
	pulled_vars = pull_vars_of_level((Node *) temp_tlist, 0);

	foreach (lc1, pulled_vars)
	{
		Var		   *tlist_var = (Var *) lfirst(lc1);
		bool		found_column = false;
		AttrNumber	attnum;

		/* Skip system attributes */
		if (tlist_var->varattno < InvalidAttrNumber)
			continue;

		attnum = 0;
		foreach (lc2, appinfo->translated_vars)
		{
			Var *translated_var = (Var *) lfirst(lc2);

			/* Don't forget to inc 'attunum'! */
			attnum++;

			/* Skip dropped columns */
			if (!translated_var)
				continue;

			/* Find this column in list of parent table columns */
			if (translated_var->varattno == tlist_var->varattno)
			{
				tlist_var->varattno = attnum;
				found_column = true; /* successful mapping */
			}
		}

		/* Raise ERROR if mapping failed */
		if (!found_column)
			elog(ERROR,
				 "table \"%s\" has no attribute %d of partition \"%s\"",
				 get_rel_name_or_relid(appinfo->parent_relid),
				 tlist_var->varattno,
				 get_rel_name_or_relid(appinfo->child_relid));
	}

	ChangeVarNodes((Node *) temp_tlist,
				   appinfo->child_relid,
				   appinfo->parent_relid,
				   0);

	return temp_tlist;
}

/* Is tlist 'a' subset of tlist 'b'? (in terms of Vars) */
static bool
tlist_is_var_subset(List *a, List *b)
{
	ListCell *lc;

	foreach (lc, b)
	{
		TargetEntry *te = (TargetEntry *) lfirst(lc);

		if (!IsA(te->expr, Var) && !IsA(te->expr, RelabelType))
			continue;

		if (!tlist_member_ignore_relabel_compat(te->expr, a))
			return true;
	}

	return false;
}

/* Append partition attribute in case it's not present in target list */
static List *
append_part_attr_to_tlist(List *tlist,
						  AppendRelInfo *appinfo,
						  const PartRelationInfo *prel)
{
	ListCell   *lc,
			   *lc_var;
	List	   *vars_not_found = NIL;

	foreach (lc_var, prel->expr_vars)
	{
		bool	part_attr_found		= false;
		Var		*expr_var			= (Var *) lfirst(lc_var),
				*child_var;

		/* Get attribute number of partitioned column (may differ) */
		child_var = (Var *) list_nth(appinfo->translated_vars,
								AttrNumberGetAttrOffset(expr_var->varattno));
		Assert(child_var);

		foreach (lc, tlist)
		{
			TargetEntry	    *te = (TargetEntry *) lfirst(lc);
			Var				*var = (Var *) te->expr;

			if (IsA(var, Var) && var->varattno == child_var->varattno)
			{
				part_attr_found = true;
				break;
			}
		}

		if (!part_attr_found)
			vars_not_found = lappend(vars_not_found, child_var);
	}

	foreach(lc, vars_not_found)
	{
		Index	last_item = list_length(tlist) + 1;
		Var		*newvar = (Var *) palloc(sizeof(Var));

		/* copy Var */
		*newvar = *((Var *) lfirst(lc));

		/* other fields except 'varno' should be correct */
		newvar->varno = appinfo->child_relid;

		tlist = lappend(tlist, makeTargetEntry((Expr *) newvar,
											   last_item,
											   NULL, false));
	}

	list_free(vars_not_found);
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


/* Check that one of arguments of OpExpr is expression */
static bool
clause_contains_prel_expr(Node *node, Node *prel_expr)
{
	if (node == NULL)
		return false;

	if (match_expr_to_operand(prel_expr, node))
			return true;

	return expression_tree_walker(node, clause_contains_prel_expr, prel_expr);
}


/* Prepare CustomScan's custom expression for walk_expr_tree() */
static Node *
canonicalize_custom_exprs_mutator(Node *node, void *cxt)
{
	if (node == NULL)
		return NULL;

	if (IsA(node, Var))
	{
		Var *var = palloc(sizeof(Var));
		*var = *(Var *) node;

		/* Replace original 'varnoold' */
		var->varnoold = INDEX_VAR;

		/* Restore original 'varattno' */
		var->varattno = var->varoattno;

		return (Node *) var;
	}

	return expression_tree_mutator(node, canonicalize_custom_exprs_mutator, NULL);
}

static List *
canonicalize_custom_exprs(List *custom_exps)
{
	return (List *) canonicalize_custom_exprs_mutator((Node *) custom_exps, NULL);
}


/*
 * Filter all available clauses and extract relevant ones.
 */
List *
get_partitioning_clauses(List *restrictinfo_list,
						 const PartRelationInfo *prel,
						 Index partitioned_rel)
{
	List	   *result = NIL;
	ListCell   *l;

	foreach(l, restrictinfo_list)
	{
		RestrictInfo   *rinfo = (RestrictInfo *) lfirst(l);
		Node		   *prel_expr;

		Assert(IsA(rinfo, RestrictInfo));

		prel_expr = PrelExpressionForRelid(prel, partitioned_rel);

		if (clause_contains_prel_expr((Node *) rinfo->clause, prel_expr))
			result = lappend(result, rinfo->clause);
	}
	return result;
}


/* Transform partition ranges into plain array of partition Oids */
Oid *
get_partition_oids(List *ranges, int *n, const PartRelationInfo *prel,
				   bool include_parent)
{
	ListCell   *range_cell;
	uint32		allocated,
				used;
	Oid		   *result;
	Oid		   *children = PrelGetChildrenArray(prel);

	ArrayAlloc(result, allocated, used, INITIAL_ALLOC_NUM);

	/* If required, add parent to result */
	Assert(INITIAL_ALLOC_NUM >= 1);
	if (include_parent)
		result[used++] = PrelParentRelid(prel);

	/* Deal with selected partitions */
	foreach (range_cell, ranges)
	{
		uint32	i;
		uint32	a = irange_lower(lfirst_irange(range_cell)),
				b = irange_upper(lfirst_irange(range_cell));

		for (i = a; i <= b; i++)
		{
			Assert(i < PrelChildrenCount(prel));
			ArrayPush(result, allocated, used, children[i]);
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
		Path			   *path = (Path *) lfirst(lc);
		RelOptInfo		   *childrel = path->parent;
		ChildScanCommon		child;

		/* Do we have parameterization? */
		if (param_info)
		{
			Relids required_outer = param_info->ppi_req_outer;

			/* Rebuild path using new 'required_outer' */
			path = get_cheapest_parameterized_child_path(root, childrel,
														 required_outer);
		}

		/*
		 * We were unable to re-parameterize child path,
		 * which means that we can't use Runtime[Merge]Append,
		 * since its children can't evaluate join quals.
		 */
		if (!path)
		{
			int j;

			for (j = 0; j < i; j++)
				pfree(result->children[j]);
			pfree(result->children);

			list_free_deep(result->cpath.custom_paths);

			pfree(result);

			return NULL; /* notify caller */
		}

		child = (ChildScanCommon) palloc(sizeof(ChildScanCommonData));

		result->cpath.path.startup_cost += path->startup_cost;
		result->cpath.path.total_cost += path->total_cost;

		child->content_type = CHILD_PATH;
		child->content.path = path;
		child->relid = root->simple_rte_array[childrel->relid]->relid;
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
	RuntimeAppendPath  *rpath = (RuntimeAppendPath *) best_path;
	PartRelationInfo   *prel;
	CustomScan		   *cscan;

	prel = get_pathman_relation_info(rpath->relid);
	Assert(prel);

	cscan = makeNode(CustomScan);
	cscan->custom_scan_tlist = NIL; /* initial value (empty list) */

	if (custom_plans)
	{
		ListCell   *lc1,
				   *lc2;
		bool		processed_rel_tlist = false;

		Assert(list_length(rpath->cpath.custom_paths) == list_length(custom_plans));

		forboth (lc1, rpath->cpath.custom_paths, lc2, custom_plans)
		{
			Plan		   *child_plan = (Plan *) lfirst(lc2);
			RelOptInfo	   *child_rel = ((Path *) lfirst(lc1))->parent;
#if PG_VERSION_NUM >= 110000
			AppendRelInfo *appinfo = root->append_rel_array[child_rel->relid];
#else
			AppendRelInfo  *appinfo = find_childrel_appendrelinfo(root, child_rel);
#endif

			/* Replace rel's tlist with a matching one (for ExecQual()) */
			if (!processed_rel_tlist)
			{
				List *temp_tlist = build_parent_tlist(child_plan->targetlist,
													  appinfo);

				/*
				 * HACK: PostgreSQL may return a physical tlist,
				 * which is bad (we may have child IndexOnlyScans).
				 * If we find out that CustomScan's tlist is a
				 * Var-superset of child's tlist, we replace it
				 * with the latter, else we'll have a broken tlist
				 * labeling (Assert).
				 *
				 * NOTE: physical tlist may only be used if we're not
				 * asked to produce tuples of exact format (CP_EXACT_TLIST).
				 */
				if (tlist_is_var_subset(temp_tlist, tlist))
					tlist = temp_tlist;

				/* Done, new target list has been built */
				processed_rel_tlist = true;
			}

			/* Add partition attribute if necessary (for ExecQual()) */
			child_plan->targetlist = append_part_attr_to_tlist(child_plan->targetlist,
															   appinfo, prel);

			/* Now make custom_scan_tlist match child plans' targetlists */
			if (!cscan->custom_scan_tlist)
				cscan->custom_scan_tlist = build_parent_tlist(child_plan->targetlist,
															  appinfo);
		}
	}

	cscan->scan.plan.qual = NIL;
	cscan->scan.plan.targetlist = tlist;

	/* Since we're not scanning any real table directly */
	cscan->scan.scanrelid = 0;

	cscan->custom_exprs = get_partitioning_clauses(clauses, prel, rel->relid);
	cscan->custom_plans = custom_plans;
	cscan->methods = scan_methods;

	/* Cache 'prel->enable_parent' as well */
	pack_runtimeappend_private(cscan, rpath, prel->enable_parent);

	/* Don't forget to close 'prel'! */
	close_pathman_relation_info(prel);

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

#if PG_VERSION_NUM < 100000
	node->ss.ps.ps_TupFromTlist = false;
#endif

	scan_state->prel = get_pathman_relation_info(scan_state->relid);
	Assert(scan_state->prel);

	/* Prepare expression according to set_set_customscan_references() */
	scan_state->prel_expr = PrelExpressionForRelid(scan_state->prel, INDEX_VAR);

	/* Prepare custom expression according to set_set_customscan_references() */
	scan_state->canon_custom_exprs =
			canonicalize_custom_exprs(scan_state->custom_exprs);
}

TupleTableSlot *
exec_append_common(CustomScanState *node,
				   void (*fetch_next_tuple) (CustomScanState *node))
{
	RuntimeAppendState *scan_state = (RuntimeAppendState *) node;
	TupleTableSlot	   *result;

	/* ReScan if no plans are selected */
	if (scan_state->ncur_plans == 0)
		ExecReScan(&node->ss.ps);

#if PG_VERSION_NUM >= 100000
	fetch_next_tuple(node); /* use specific callback */

	if (TupIsNull(scan_state->slot))
		return NULL;

	if (!node->ss.ps.ps_ProjInfo)
		return scan_state->slot;

	/*
	 * Assuming that current projection doesn't involve SRF.
	 * NOTE: Any SFR functions are evaluated in ProjectSet node.
	 */
	ResetExprContext(node->ss.ps.ps_ExprContext);
	node->ss.ps.ps_ProjInfo->pi_exprContext->ecxt_scantuple = scan_state->slot;
	result = ExecProject(node->ss.ps.ps_ProjInfo);

	return result;
#elif PG_VERSION_NUM >= 90500
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

			ResetExprContext(node->ss.ps.ps_ExprContext);

			node->ss.ps.ps_ProjInfo->pi_exprContext->ecxt_scantuple =
				scan_state->slot;
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
#endif
}

void
end_append_common(CustomScanState *node)
{
	RuntimeAppendState *scan_state = (RuntimeAppendState *) node;

	clear_plan_states(&scan_state->css);
	hash_destroy(scan_state->children_table);
	close_pathman_relation_info(scan_state->prel);
}

void
rescan_append_common(CustomScanState *node)
{
	RuntimeAppendState *scan_state = (RuntimeAppendState *) node;
	ExprContext		   *econtext = node->ss.ps.ps_ExprContext;
	PartRelationInfo   *prel = scan_state->prel;
	List			   *ranges;
	ListCell		   *lc;
	WalkerContext		wcxt;
	Oid				   *parts;
	int					nparts;

	/* First we select all available partitions... */
	ranges = list_make1_irange_full(prel, IR_COMPLETE);

	InitWalkerContext(&wcxt, scan_state->prel_expr, prel, econtext);
	foreach (lc, scan_state->canon_custom_exprs)
	{
		WrapperNode *wrap;

		/* ... then we cut off irrelevant ones using the provided clauses */
		wrap = walk_expr_tree((Expr *) lfirst(lc), &wcxt);
		ranges = irange_list_intersection(ranges, wrap->rangeset);
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
explain_append_common(CustomScanState *node,
					  List *ancestors,
					  ExplainState *es,
					  HTAB *children_table,
					  List *custom_exprs)
{
	List *deparse_context;
	char *exprstr;

	/* Set up deparsing context */
	deparse_context = set_deparse_context_planstate(es->deparse_cxt,
													(Node *) node,
													ancestors);

	/* Deparse the expression */
	exprstr = deparse_expression((Node *) make_ands_explicit(custom_exprs),
								 deparse_context, true, false);

	/* And add to es->str */
	ExplainPropertyText("Prune by", exprstr, es);

	/* Construct excess PlanStates */
	if (!es->analyze)
	{
		uint32				allocated,
							used;
		ChildScanCommon	   *custom_ps,
							child;
		HASH_SEQ_STATUS		seqstat;
		int					i;

		ArrayAlloc(custom_ps, allocated, used, INITIAL_ALLOC_NUM);

		/* There can't be any nodes since we're not scanning anything */
		Assert(!node->custom_ps);

		/* Iterate through node's ChildScanCommon table */
		hash_seq_init(&seqstat, children_table);

		while ((child = (ChildScanCommon) hash_seq_search(&seqstat)))
		{
			ArrayPush(custom_ps, allocated, used, child);
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
