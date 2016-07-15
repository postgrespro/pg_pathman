/* ------------------------------------------------------------------------
 *
 * pg_pathman.c
 *		This module sets planner hooks, handles SELECT queries and produces
 *		paths for partitioned tables
 *
 * Copyright (c) 2015-2016, Postgres Professional
 *
 * ------------------------------------------------------------------------
 */
#include "pathman.h"
#include "postgres.h"
#include "fmgr.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "nodes/pg_list.h"
#include "nodes/relation.h"
#include "nodes/primnodes.h"
#include "optimizer/clauses.h"
#include "optimizer/paths.h"
#include "optimizer/pathnode.h"
#include "optimizer/planner.h"
#include "optimizer/restrictinfo.h"
#include "optimizer/cost.h"
#include "parser/analyze.h"
#include "utils/hsearch.h"
#include "utils/tqual.h"
#include "utils/rel.h"
#include "utils/elog.h"
#include "utils/array.h"
#include "utils/date.h"
#include "utils/guc.h"
#include "utils/lsyscache.h"
#include "utils/selfuncs.h"
#include "utils/memutils.h"
#include "access/heapam.h"
#include "access/nbtree.h"
#include "storage/ipc.h"
#include "catalog/pg_operator.h"
#include "catalog/pg_type.h"
#include "foreign/fdwapi.h"
#include "hooks.h"
#include "utils.h"
#include "runtimeappend.h"
#include "runtime_merge_append.h"

PG_MODULE_MAGIC;

List		   *inheritance_disabled_relids = NIL;
List		   *inheritance_enabled_relids = NIL;
bool			pg_pathman_enable;
PathmanState   *pmstate;

/* Original hooks */
static shmem_startup_hook_type shmem_startup_hook_original = NULL;
static post_parse_analyze_hook_type post_parse_analyze_hook_original = NULL;
static planner_hook_type planner_hook_original = NULL;

/* pg module functions */
void _PG_init(void);

/* Hook functions */
static void pathman_shmem_startup(void);
void pathman_post_parse_analysis_hook(ParseState *pstate, Query *query);
static PlannedStmt * pathman_planner_hook(Query *parse, int cursorOptions, ParamListInfo boundParams);

/* Utility functions */
static void handle_modification_query(Query *parse);
static Node *wrapper_make_expression(WrapperNode *wrap, int index, bool *alwaysTrue);
static void disable_inheritance(Query *parse);
static void disable_inheritance_cte(Query *parse);
static void disable_inheritance_subselect(Query *parse);
static bool disable_inheritance_subselect_walker(Node *node, void *context);

/* Expression tree handlers */
static void handle_binary_opexpr(WalkerContext *context, WrapperNode *result, const Node *varnode, const Const *c);
static void handle_binary_opexpr_param(const PartRelationInfo *prel, WrapperNode *result, const Node *varnode);
static WrapperNode *handle_opexpr(const OpExpr *expr, WalkerContext *context);
static WrapperNode *handle_boolexpr(const BoolExpr *expr, WalkerContext *context);
static WrapperNode *handle_arrexpr(const ScalarArrayOpExpr *expr, WalkerContext *context);
static RestrictInfo *rebuild_restrictinfo(Node *clause, RestrictInfo *old_rinfo);
static bool pull_var_param(const WalkerContext *ctx, const OpExpr *expr, Node **var_ptr, Node **param_ptr);

/* copied from allpaths.h */
static void set_plain_rel_size(PlannerInfo *root, RelOptInfo *rel,
				   RangeTblEntry *rte);
static void set_plain_rel_pathlist(PlannerInfo *root, RelOptInfo *rel, RangeTblEntry *rte);
static List *accumulate_append_subpath(List *subpaths, Path *path);
static void generate_mergeappend_paths(PlannerInfo *root, RelOptInfo *rel,
						   List *live_childrels,
						   List *all_child_pathkeys,
						   PathKey *pathkeyAsc,
						   PathKey *pathkeyDesc);
static Path *get_cheapest_parameterized_child_path(PlannerInfo *root, RelOptInfo *rel, Relids required_outer);


/*
 * Compare two Datums with the given comarison function
 *
 * flinfo is a pointer to an instance of FmgrInfo
 * arg1, arg2 are Datum instances
 */
#define check_lt(flinfo, arg1, arg2) \
	((int) FunctionCall2(cmp_func, arg1, arg2) < 0)
#define check_le(flinfo, arg1, arg2) \
	((int) FunctionCall2(cmp_func, arg1, arg2) <= 0)
#define check_eq(flinfo, arg1, arg2) \
	((int) FunctionCall2(cmp_func, arg1, arg2) == 0)
#define check_ge(flinfo, arg1, arg2) \
	((int) FunctionCall2(cmp_func, arg1, arg2) >= 0)
#define check_gt(flinfo, arg1, arg2) \
	((int) FunctionCall2(cmp_func, arg1, arg2) > 0)

#define WcxtHasExprContext(wcxt) ( (wcxt)->econtext )

/* We can transform Param into Const provided that 'econtext' is available */
#define IsConstValue(wcxt, node) \
	( IsA((node), Const) || (WcxtHasExprContext(wcxt) ? IsA((node), Param) : false) )

#define ExtractConst(wcxt, node) \
	( IsA((node), Param) ? extract_const((wcxt), (Param *) (node)) : ((Const *) (node)) )

/*
 * Entry point
 */
void
_PG_init(void)
{
	if (!process_shared_preload_libraries_in_progress)
	{
		elog(ERROR, "Pathman module must be initialized in postmaster. "
					"Put the following line to configuration file: "
					"shared_preload_libraries='pg_pathman'");
		initialization_needed = false;
	}

	/* Request additional shared resources */
	RequestAddinShmemSpace(pathman_memsize());
	RequestAddinLWLocks(3);

	set_rel_pathlist_hook_next = set_rel_pathlist_hook;
	set_rel_pathlist_hook = pathman_rel_pathlist_hook;
	set_join_pathlist_next = set_join_pathlist_hook;
	set_join_pathlist_hook = pathman_join_pathlist_hook;
	shmem_startup_hook_original = shmem_startup_hook;
	shmem_startup_hook = pathman_shmem_startup;
	post_parse_analyze_hook_original = post_parse_analyze_hook;
	post_parse_analyze_hook = pathman_post_parse_analysis_hook;
	planner_hook_original = planner_hook;
	planner_hook = pathman_planner_hook;

	/* RuntimeAppend */
	runtimeappend_path_methods.CustomName				= "RuntimeAppend";
	runtimeappend_path_methods.PlanCustomPath			= create_runtimeappend_plan;

	runtimeappend_plan_methods.CustomName 				= "RuntimeAppend";
	runtimeappend_plan_methods.CreateCustomScanState	= runtimeappend_create_scan_state;

	runtimeappend_exec_methods.CustomName				= "RuntimeAppend";
	runtimeappend_exec_methods.BeginCustomScan			= runtimeappend_begin;
	runtimeappend_exec_methods.ExecCustomScan			= runtimeappend_exec;
	runtimeappend_exec_methods.EndCustomScan			= runtimeappend_end;
	runtimeappend_exec_methods.ReScanCustomScan			= runtimeappend_rescan;
	runtimeappend_exec_methods.MarkPosCustomScan		= NULL;
	runtimeappend_exec_methods.RestrPosCustomScan		= NULL;
	runtimeappend_exec_methods.ExplainCustomScan		= runtimeappend_explain;

	/* RuntimeMergeAppend */
	runtime_merge_append_path_methods.CustomName			= "RuntimeMergeAppend";
	runtime_merge_append_path_methods.PlanCustomPath		= create_runtimemergeappend_plan;

	runtime_merge_append_plan_methods.CustomName 			= "RuntimeMergeAppend";
	runtime_merge_append_plan_methods.CreateCustomScanState	= runtimemergeappend_create_scan_state;

	runtime_merge_append_exec_methods.CustomName			= "RuntimeMergeAppend";
	runtime_merge_append_exec_methods.BeginCustomScan		= runtimemergeappend_begin;
	runtime_merge_append_exec_methods.ExecCustomScan		= runtimemergeappend_exec;
	runtime_merge_append_exec_methods.EndCustomScan			= runtimemergeappend_end;
	runtime_merge_append_exec_methods.ReScanCustomScan		= runtimemergeappend_rescan;
	runtime_merge_append_exec_methods.MarkPosCustomScan		= NULL;
	runtime_merge_append_exec_methods.RestrPosCustomScan	= NULL;
	runtime_merge_append_exec_methods.ExplainCustomScan		= runtimemergeappend_explain;

	DefineCustomBoolVariable("pg_pathman.enable",
							 "Enables pg_pathman's optimizations during the planner stage",
							 NULL,
							 &pg_pathman_enable,
							 true,
							 PGC_USERSET,
							 0,
							 NULL,
							 pg_pathman_enable_assign_hook,
							 NULL);

	DefineCustomBoolVariable("pg_pathman.enable_runtimeappend",
							 "Enables the planner's use of RuntimeAppend custom node.",
							 NULL,
							 &pg_pathman_enable_runtimeappend,
							 true,
							 PGC_USERSET,
							 0,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomBoolVariable("pg_pathman.enable_runtimemergeappend",
							 "Enables the planner's use of RuntimeMergeAppend custom node.",
							 NULL,
							 &pg_pathman_enable_runtime_merge_append,
							 true,
							 PGC_USERSET,
							 0,
							 NULL,
							 NULL,
							 NULL);
}

PartRelationInfo *
get_pathman_relation_info(Oid relid, bool *found)
{
	RelationKey key;

	key.dbid = MyDatabaseId;
	key.relid = relid;
	return hash_search(relations, (const void *) &key, HASH_FIND, found);
}

RangeRelation *
get_pathman_range_relation(Oid relid, bool *found)
{
	RelationKey key;

	key.dbid = MyDatabaseId;
	key.relid = relid;
	return hash_search(range_restrictions, (const void *) &key, HASH_FIND, found);
}

FmgrInfo *
get_cmp_func(Oid type1, Oid type2)
{
	FmgrInfo   *cmp_func;
	Oid			cmp_proc_oid;
	TypeCacheEntry	*tce;

	cmp_func = palloc(sizeof(FmgrInfo));
	tce = lookup_type_cache(type1,
				TYPECACHE_BTREE_OPFAMILY | TYPECACHE_CMP_PROC | TYPECACHE_CMP_PROC_FINFO);
	cmp_proc_oid = get_opfamily_proc(tce->btree_opf,
									 type1,
									 type2,
									 BTORDER_PROC);
	fmgr_info(cmp_proc_oid, cmp_func);
	return cmp_func;
}

/*
 * Post parse analysis hook. It makes sure the config is loaded before executing
 * any statement, including utility commands
 */
void
pathman_post_parse_analysis_hook(ParseState *pstate, Query *query)
{
	if (initialization_needed)
		load_config();

	if (post_parse_analyze_hook_original)
		post_parse_analyze_hook_original(pstate, query);

	inheritance_disabled_relids = NIL;
	inheritance_enabled_relids = NIL;
}

/*
 * Planner hook. It disables inheritance for tables that have been partitioned
 * by pathman to prevent standart PostgreSQL partitioning mechanism from
 * handling that tables.
 */
PlannedStmt *
pathman_planner_hook(Query *parse, int cursorOptions, ParamListInfo boundParams)
{
	PlannedStmt	  *result;

	if (pg_pathman_enable)
	{
		// inheritance_disabled = false;
		switch(parse->commandType)
		{
			case CMD_SELECT:
				disable_inheritance(parse);
				break;
			case CMD_UPDATE:
			case CMD_DELETE:
				disable_inheritance_cte(parse);
				disable_inheritance_subselect(parse);
				handle_modification_query(parse);
				break;
			default:
				break;
		}
	}

	/* Invoke original hook */
	if (planner_hook_original)
		result = planner_hook_original(parse, cursorOptions, boundParams);
	else
		result = standard_planner(parse, cursorOptions, boundParams);

	list_free(inheritance_disabled_relids);
	list_free(inheritance_enabled_relids);
	inheritance_disabled_relids = NIL;
	inheritance_enabled_relids = NIL;

	return result;
}

/*
 * Disables inheritance for partitioned by pathman relations. It must be done to
 * prevent PostgresSQL from full search.
 */
static void
disable_inheritance(Query *parse)
{
	ListCell		   *lc;
	RangeTblEntry	   *rte;
	PartRelationInfo   *prel;
	MemoryContext		oldcontext;
	bool	found;

	/* If query contains CTE (WITH statement) then handle subqueries too */
	disable_inheritance_cte(parse);

	/* If query contains subselects */
	disable_inheritance_subselect(parse);

	foreach(lc, parse->rtable)
	{
		rte = (RangeTblEntry*) lfirst(lc);

		switch(rte->rtekind)
		{
			case RTE_RELATION:
				if (rte->inh)
				{
					/* Look up this relation in pathman relations */
					prel = get_pathman_relation_info(rte->relid, &found);
					if (prel != NULL && found)
					{
						rte->inh = false;
						/*
						 * Sometimes user uses the ONLY statement and in this case
						 * rte->inh is also false. We should differ the case
						 * when user uses ONLY statement from case when we
						 * make rte->inh false intentionally.
						 */
						oldcontext = MemoryContextSwitchTo(TopMemoryContext);
						inheritance_enabled_relids = \
							lappend_oid(inheritance_enabled_relids, rte->relid);
						MemoryContextSwitchTo(oldcontext);

						/*
						 * Check if relation was already found with ONLY modifier. In
						 * this case throw an error because we cannot handle
						 * situations when partitioned table used both with and
						 * without ONLY modifier in SELECT queries
						 */
						if (list_member_oid(inheritance_disabled_relids, rte->relid))
							goto disable_error;

						goto disable_next;
					}
				}

				oldcontext = MemoryContextSwitchTo(TopMemoryContext);
				inheritance_disabled_relids = \
					lappend_oid(inheritance_disabled_relids, rte->relid);
				MemoryContextSwitchTo(oldcontext);

				/* Check if relation was already found withoud ONLY modifier */
				if (list_member_oid(inheritance_enabled_relids, rte->relid))
						goto disable_error;
				break;
			case RTE_SUBQUERY:
				/* Recursively disable inheritance for subqueries */
				disable_inheritance(rte->subquery);
				break;
			default:
				break;
		}

disable_next:
		;
	}

	return;

disable_error:
	elog(ERROR, "It is prohibited to query partitioned tables both "
				"with and without ONLY modifier");
}

static void
disable_inheritance_cte(Query *parse)
{
	ListCell	  *lc;

	foreach(lc, parse->cteList)
	{
		CommonTableExpr *cte = (CommonTableExpr*) lfirst(lc);

		if (IsA(cte->ctequery, Query))
			disable_inheritance((Query *) cte->ctequery);
	}
}

static void
disable_inheritance_subselect(Query *parse)
{
	Node		*quals;

	if (!parse->jointree || !parse->jointree->quals)
		return;

	quals = parse->jointree->quals;
	disable_inheritance_subselect_walker(quals, NULL);
}

static bool
disable_inheritance_subselect_walker(Node *node, void *context)
{
	if (node == NULL)
		return false;

	if (IsA(node, SubLink))
	{
		disable_inheritance((Query *) (((SubLink *) node)->subselect));
		return false;
	}

	return expression_tree_walker(node, disable_inheritance_subselect_walker, (void *) context);
}

/*
 * Checks if query is affects only one partition. If true then substitute
 */
static void
handle_modification_query(Query *parse)
{
	PartRelationInfo   *prel;
	List			   *ranges;
	RangeTblEntry	   *rte;
	WrapperNode		   *wrap;
	Expr			   *expr;
	bool				found;
	WalkerContext		context;

	Assert(parse->commandType == CMD_UPDATE ||
		   parse->commandType == CMD_DELETE);
	Assert(parse->resultRelation > 0);

	rte = rt_fetch(parse->resultRelation, parse->rtable);
	prel = get_pathman_relation_info(rte->relid, &found);

	if (!found)
		return;

	/* Parse syntax tree and extract partition ranges */
	ranges = list_make1_int(make_irange(0, prel->children_count - 1, false));
	expr = (Expr *) eval_const_expressions(NULL, parse->jointree->quals);
	if (!expr)
		return;

	/* Parse syntax tree and extract partition ranges */
	context.prel = prel;
	context.econtext = NULL;
	context.hasLeast = false;
	context.hasGreatest = false;
	wrap = walk_expr_tree(expr, &context);
	finish_least_greatest(wrap, &context);

	ranges = irange_list_intersect(ranges, wrap->rangeset);

	/* If only one partition is affected then substitute parent table with partition */
	if (irange_list_length(ranges) == 1)
	{
		IndexRange irange = (IndexRange) linitial_oid(ranges);
		if (irange_lower(irange) == irange_upper(irange))
		{
			Oid *children = (Oid *) dsm_array_get_pointer(&prel->children);
			rte->relid = children[irange_lower(irange)];
			rte->inh = false;
		}
	}

	return;
}

/*
 * Shared memory startup hook
 */
static void
pathman_shmem_startup(void)
{
	/* Allocate shared memory objects */
	LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);
	init_dsm_config();
	init_shmem_config();
	LWLockRelease(AddinShmemInitLock);

	/* Invoke original hook if needed */
	if (shmem_startup_hook_original != NULL)
		shmem_startup_hook_original();
}

void
set_append_rel_size(PlannerInfo *root, RelOptInfo *rel,
					Index rti, RangeTblEntry *rte)
{
	double		parent_rows = 0;
	double		parent_size = 0;
	ListCell   *l;

	foreach(l, root->append_rel_list)
	{
		AppendRelInfo *appinfo = (AppendRelInfo *) lfirst(l);
		int			childRTindex,
					parentRTindex = rti;
		RelOptInfo *childrel;

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
		parent_size += childrel->width * childrel->rows;
	}

	rel->rows = parent_rows;
	rel->width = rint(parent_size / parent_rows);
	// for (i = 0; i < nattrs; i++)
	// 	rel->attr_widths[i] = rint(parent_attrsizes[i] / parent_rows);
	rel->tuples = parent_rows;
}

/*
 * Creates child relation and adds it to root.
 * Returns child index in simple_rel_array
 */
int
append_child_relation(PlannerInfo *root, RelOptInfo *rel, Index rti,
	RangeTblEntry *rte, int index, Oid childOid, List *wrappers)
{
	RangeTblEntry *childrte;
	RelOptInfo    *childrel;
	Index		childRTindex;
	AppendRelInfo *appinfo;
	Node *node;
	ListCell *lc, *lc2;
	Relation	newrelation;

	newrelation = heap_open(childOid, NoLock);

	/*
	 * Create RangeTblEntry for child relation.
	 * This code partially based on expand_inherited_rtentry() function.
	 */
	childrte = copyObject(rte);
	childrte->relid = childOid;
	childrte->relkind = newrelation->rd_rel->relkind;
	childrte->inh = false;
	childrte->requiredPerms = 0;
	root->parse->rtable = lappend(root->parse->rtable, childrte);
	childRTindex = list_length(root->parse->rtable);
	root->simple_rte_array[childRTindex] = childrte;

	/* Create RelOptInfo */
	childrel = build_simple_rel(root, childRTindex, RELOPT_OTHER_MEMBER_REL);

	/* Copy targetlist */
	childrel->reltargetlist = NIL;
	foreach(lc, rel->reltargetlist)
	{
		Node *new_target;

		node = (Node *) lfirst(lc);
		new_target = copyObject(node);
		change_varnos(new_target, rel->relid, childrel->relid);
		childrel->reltargetlist = lappend(childrel->reltargetlist, new_target);
	}

	/* Copy attr_needed (used in build_joinrel_tlist() function) */
	childrel->attr_needed = rel->attr_needed;

	/* Copy restrictions */
	childrel->baserestrictinfo = NIL;
	forboth(lc, wrappers, lc2, rel->baserestrictinfo)
	{
		bool alwaysTrue;
		WrapperNode *wrap = (WrapperNode *) lfirst(lc);
		Node *new_clause = wrapper_make_expression(wrap, index, &alwaysTrue);
		RestrictInfo *old_rinfo = (RestrictInfo *) lfirst(lc2);

		if (alwaysTrue)
		{
			continue;
		}
		Assert(new_clause);

		if (and_clause((Node *) new_clause))
		{
			ListCell *alc;

			foreach(alc, ((BoolExpr *) new_clause)->args)
			{
				Node *arg = (Node *) lfirst(alc);
				RestrictInfo *new_rinfo = rebuild_restrictinfo(arg, old_rinfo);

				change_varnos((Node *)new_rinfo, rel->relid, childrel->relid);
				childrel->baserestrictinfo = lappend(childrel->baserestrictinfo,
													 new_rinfo);
			}
		}
		else
		{
			RestrictInfo *new_rinfo = rebuild_restrictinfo(new_clause, old_rinfo);

			/* Replace old relids with new ones */
			change_varnos((Node *)new_rinfo, rel->relid, childrel->relid);

			childrel->baserestrictinfo = lappend(childrel->baserestrictinfo,
												 (void *) new_rinfo);
		}
	}

	/* Build an AppendRelInfo for this parent and child */
	appinfo = makeNode(AppendRelInfo);
	appinfo->parent_relid = rti;
	appinfo->child_relid = childRTindex;
	appinfo->parent_reloid = rte->relid;
	root->append_rel_list = lappend(root->append_rel_list, appinfo);
	root->total_table_pages += (double) childrel->pages;

	/* Add equivalence members */
	foreach(lc, root->eq_classes)
	{
		EquivalenceClass *cur_ec = (EquivalenceClass *) lfirst(lc);

		/* Copy equivalence member from parent and make some modifications */
		foreach(lc2, cur_ec->ec_members)
		{
			EquivalenceMember *cur_em = (EquivalenceMember *) lfirst(lc2);
			EquivalenceMember *em;

			if (!bms_is_member(rti, cur_em->em_relids))
				continue;

			em = makeNode(EquivalenceMember);
			em->em_expr = copyObject(cur_em->em_expr);
			change_varnos((Node *) em->em_expr, rti, childRTindex);
			em->em_relids = bms_add_member(NULL, childRTindex);
			em->em_nullable_relids = cur_em->em_nullable_relids;
			em->em_is_const = false;
			em->em_is_child = true;
			em->em_datatype = cur_em->em_datatype;
			cur_ec->ec_members = lappend(cur_ec->ec_members, em);
		}
	}
	childrel->has_eclass_joins = rel->has_eclass_joins;

	/* Recalc parent relation tuples count */
	rel->tuples += childrel->tuples;

	heap_close(newrelation, NoLock);

	return childRTindex;
}

/* Create new restriction based on clause */
static RestrictInfo *
rebuild_restrictinfo(Node *clause, RestrictInfo *old_rinfo)
{
	return make_restrictinfo((Expr *) clause,
							 old_rinfo->is_pushed_down,
							 old_rinfo->outerjoin_delayed,
							 old_rinfo->pseudoconstant,
							 old_rinfo->required_relids,
							 old_rinfo->outer_relids,
							 old_rinfo->nullable_relids);
}

/* Convert wrapper into expression for given index */
static Node *
wrapper_make_expression(WrapperNode *wrap, int index, bool *alwaysTrue)
{
	bool	lossy, found;

	*alwaysTrue = false;
	/*
	 * TODO: use faster algorithm using knowledge that we enumerate indexes
	 * sequntially.
	 */
	found = irange_list_find(wrap->rangeset, index, &lossy);
	/* Return NULL for always true and always false. */
	if (!found)
		return NULL;
	if (!lossy)
	{
		*alwaysTrue = true;
		return NULL;
	}

	if (IsA(wrap->orig, BoolExpr))
	{
		const BoolExpr *expr = (const BoolExpr *) wrap->orig;
		BoolExpr *result;

		if (expr->boolop == OR_EXPR || expr->boolop == AND_EXPR)
		{
			ListCell *lc;
			List *args = NIL;

			foreach (lc, wrap->args)
			{
				Node   *arg;
				bool	childAlwaysTrue;

				arg = wrapper_make_expression((WrapperNode *)lfirst(lc), index, &childAlwaysTrue);
#ifdef USE_ASSERT_CHECKING
				/*
				 * We shouldn't get there for always true clause under OR and
				 * always false clause under AND.
				 */
				if (expr->boolop == OR_EXPR)
					Assert(!childAlwaysTrue);
				if (expr->boolop == AND_EXPR)
					Assert(arg || childAlwaysTrue);
#endif
				if (arg)
					args = lappend(args, arg);
			}

			Assert(list_length(args) >= 1);

			/* Remove redundant OR/AND when child is single. */
			if (list_length(args) == 1)
				return (Node *) linitial(args);

			result = makeNode(BoolExpr);
			result->xpr.type = T_BoolExpr;
			result->args = args;
			result->boolop = expr->boolop;
			result->location = expr->location;
			return (Node *)result;
		}
		else
		{
			return copyObject(wrap->orig);
		}
	}
	else
	{
		return copyObject(wrap->orig);
	}
}

/*
 * Recursive function to walk through conditions tree
 */
WrapperNode *
walk_expr_tree(Expr *expr, WalkerContext *context)
{
	BoolExpr		   *boolexpr;
	OpExpr			   *opexpr;
	ScalarArrayOpExpr  *arrexpr;
	WrapperNode		   *result;

	switch (expr->type)
	{
		/* AND, OR, NOT expressions */
		case T_BoolExpr:
			boolexpr = (BoolExpr *) expr;
			return handle_boolexpr(boolexpr, context);
		/* =, !=, <, > etc. */
		case T_OpExpr:
			opexpr = (OpExpr *) expr;
			return handle_opexpr(opexpr, context);
		/* IN expression */
		case T_ScalarArrayOpExpr:
			arrexpr = (ScalarArrayOpExpr *) expr;
			return handle_arrexpr(arrexpr, context);
		default:
			result = (WrapperNode *)palloc(sizeof(WrapperNode));
			result->orig = (const Node *)expr;
			result->args = NIL;
			result->rangeset = list_make1_irange(make_irange(0, context->prel->children_count - 1, true));
			result->paramsel = 1.0;

			return result;
	}
}

void
finish_least_greatest(WrapperNode *wrap, WalkerContext *context)
{
	if (context->hasLeast && context->hasGreatest)
	{
		switch (context->prel->atttype)
		{
			case INT4OID:
				{
					int		least = DatumGetInt32(context->least),
							greatest = DatumGetInt32(context->greatest);
					List   *rangeset = NIL;

					if (greatest - least + 1 < context->prel->children_count)
					{
						int	value,
							hash;
						for (value = least; value <= greatest; value++)
						{
							hash = make_hash(value, context->prel->children_count);
							rangeset = irange_list_union(rangeset,
								list_make1_irange(make_irange(hash, hash, true)));
						}
						wrap->rangeset = irange_list_intersect(wrap->rangeset,
															   rangeset);
					}
				}
				break;
			default:
				elog(ERROR, "Invalid datatype: %u", context->prel->atttype);
		}
	}
	context->hasLeast = false;
	context->hasGreatest = false;
}

/*
 *	This function determines which partitions should appear in query plan
 */
static void
handle_binary_opexpr(WalkerContext *context, WrapperNode *result,
					 const Node *varnode, const Const *c)
{
	HashRelationKey		key;
	RangeRelation	   *rangerel;
	Datum				value;
	int					i,
						strategy;
	uint32				int_value;
	bool				is_less,
						is_greater;
	FmgrInfo		    cmp_func;
	Oid					cmp_proc_oid;
	Oid					vartype;
	const OpExpr	   *expr = (const OpExpr *)result->orig;
	TypeCacheEntry	   *tce;
	const PartRelationInfo *prel = context->prel;

	Assert(IsA(varnode, Var) || IsA(varnode, RelabelType));

	vartype = !IsA(varnode, RelabelType) ?
			((Var *) varnode)->vartype :
			((RelabelType *) varnode)->resulttype;

	/* Determine operator type */
	tce = lookup_type_cache(vartype,
							TYPECACHE_BTREE_OPFAMILY | TYPECACHE_CMP_PROC | TYPECACHE_CMP_PROC_FINFO);

	strategy = get_op_opfamily_strategy(expr->opno, tce->btree_opf);
	cmp_proc_oid = get_opfamily_proc(tce->btree_opf,
									 vartype,
									 c->consttype,
									 BTORDER_PROC);
	fmgr_info(cmp_proc_oid, &cmp_func);

	switch (prel->parttype)
	{
		case PT_HASH:
			if (strategy == BTEqualStrategyNumber)
			{
				value = OidFunctionCall1(prel->hash_proc, c->constvalue);
				int_value = DatumGetUInt32(value);
				key.hash = make_hash(int_value, prel->children_count);
				result->rangeset = list_make1_irange(make_irange(key.hash, key.hash, true));
				return;
			}
			break;
		case PT_RANGE:
			value = c->constvalue;
			rangerel = get_pathman_range_relation(prel->key.relid, NULL);
			if (rangerel != NULL)
			{
				RangeEntry *re;
				bool		lossy = false;
#ifdef USE_ASSERT_CHECKING
				bool		found = false;
				int			counter = 0;
#endif
				int			startidx = 0,
							cmp_min,
							cmp_max,
							endidx = rangerel->ranges.length - 1;
				RangeEntry *ranges = dsm_array_get_pointer(&rangerel->ranges);
				bool byVal = rangerel->by_val;

				/* Check boundaries */
				if (rangerel->ranges.length == 0)
				{
					result->rangeset = NIL;
					return;
				}
				else
				{
					/* Corner cases */
					cmp_min = FunctionCall2(&cmp_func, value,
											PATHMAN_GET_DATUM(ranges[0].min, byVal)),
					cmp_max = FunctionCall2(&cmp_func, value,
											PATHMAN_GET_DATUM(ranges[rangerel->ranges.length - 1].max, byVal));

					if ((cmp_min < 0 &&
						 (strategy == BTLessEqualStrategyNumber ||
						  strategy == BTEqualStrategyNumber)) ||
						(cmp_min <= 0 && strategy == BTLessStrategyNumber))
					{
						result->rangeset = NIL;
						return;
					}

					if (cmp_max >= 0 && (strategy == BTGreaterEqualStrategyNumber ||
						strategy == BTGreaterStrategyNumber ||
						strategy == BTEqualStrategyNumber))
					{
						result->rangeset = NIL;
						return;
					}

					if ((cmp_min < 0 && strategy == BTGreaterStrategyNumber) ||
						(cmp_min <= 0 && strategy == BTGreaterEqualStrategyNumber))
					{
						result->rangeset = list_make1_irange(make_irange(startidx, endidx, false));
						return;
					}

					if (cmp_max >= 0 && (strategy == BTLessEqualStrategyNumber ||
						strategy == BTLessStrategyNumber))
					{
						result->rangeset = list_make1_irange(make_irange(startidx, endidx, false));
						return;
					}
				}

				/* Binary search */
				while (true)
				{
					i = startidx + (endidx - startidx) / 2;
					Assert(i >= 0 && i < rangerel->ranges.length);
					re = &ranges[i];
					cmp_min = FunctionCall2(&cmp_func, value, PATHMAN_GET_DATUM(re->min, byVal));
					cmp_max = FunctionCall2(&cmp_func, value, PATHMAN_GET_DATUM(re->max, byVal));

					is_less = (cmp_min < 0 || (cmp_min == 0 && strategy == BTLessStrategyNumber));
					is_greater = (cmp_max > 0 || (cmp_max >= 0 && strategy != BTLessStrategyNumber));

					if (!is_less && !is_greater)
					{
						if (strategy == BTGreaterEqualStrategyNumber && cmp_min == 0)
							lossy = false;
						else if (strategy == BTLessStrategyNumber && cmp_max == 0)
							lossy = false;
						else
							lossy = true;
#ifdef USE_ASSERT_CHECKING
						found = true;
#endif
						break;
					}

					/* If we still didn't find partition then it doesn't exist */
					if (startidx >= endidx)
					{
						/* Handle case when we hit the gap between partitions */
						if (strategy != BTEqualStrategyNumber)
						{
							if (strategy == BTLessStrategyNumber ||
								strategy == BTLessEqualStrategyNumber)
							{
								if (is_less && i > 0)
									i--;
							}
							if (strategy == BTGreaterStrategyNumber ||
								strategy == BTGreaterEqualStrategyNumber)
							{
								if (is_greater && i < rangerel->ranges.length - 1)
									i++;
							}
							lossy = false;
							break;
						}
						result->rangeset = NIL;
						return;
					}

					if (is_less)
						endidx = i - 1;
					else if (is_greater)
						startidx = i + 1;

					/* For debug's sake */
					Assert(++counter < 100);
				}

				Assert(found);

				/* Filter partitions */
				switch(strategy)
				{
					case BTLessStrategyNumber:
					case BTLessEqualStrategyNumber:
						if (lossy)
						{
							result->rangeset = list_make1_irange(make_irange(i, i, true));
							if (i > 0)
								result->rangeset = lcons_irange(
									make_irange(0, i - 1, false), result->rangeset);
						}
						else
						{
							result->rangeset = list_make1_irange(
								make_irange(0, i, false));
						}
						return;
					case BTEqualStrategyNumber:
						result->rangeset = list_make1_irange(make_irange(i, i, true));
						return;
					case BTGreaterEqualStrategyNumber:
					case BTGreaterStrategyNumber:
						if (lossy)
						{
							result->rangeset = list_make1_irange(make_irange(i, i, true));
							if (i < prel->children_count - 1)
								result->rangeset = lappend_irange(result->rangeset,
									make_irange(i + 1, prel->children_count - 1, false));
						}
						else
						{
							result->rangeset = list_make1_irange(
								make_irange(i, prel->children_count - 1, false));
						}
						return;
				}
				result->rangeset = list_make1_irange(make_irange(startidx, endidx, true));
				return;
			}
	}

	result->rangeset = list_make1_irange(make_irange(0, prel->children_count - 1, true));
	result->paramsel = 1.0;
}

/*
 *	Estimate selectivity of parametrized quals.
 */
static void
handle_binary_opexpr_param(const PartRelationInfo *prel,
						   WrapperNode *result, const Node *varnode)
{
	const OpExpr	   *expr = (const OpExpr *)result->orig;
	TypeCacheEntry	   *tce;
	int					strategy;
	Oid					vartype;

	Assert(IsA(varnode, Var) || IsA(varnode, RelabelType));

	vartype = !IsA(varnode, RelabelType) ?
			((Var *) varnode)->vartype :
			((RelabelType *) varnode)->resulttype;

	/* Determine operator type */
	tce = lookup_type_cache(vartype, TYPECACHE_BTREE_OPFAMILY);
	strategy = get_op_opfamily_strategy(expr->opno, tce->btree_opf);

	result->rangeset = list_make1_irange(make_irange(0, prel->children_count - 1, true));

	if (strategy == BTEqualStrategyNumber)
	{
		result->paramsel = 1.0 / (double) prel->children_count;
	}
	else if (prel->parttype == PT_RANGE && strategy > 0)
	{
		result->paramsel = DEFAULT_INEQ_SEL;
	}
	else
	{
		result->paramsel = 1.0;
	}
}

/*
 * Calculates hash value
 */
uint32
make_hash(uint32 value, uint32 partitions)
{
	return value % partitions;
}

/*
 * Search for range section. Returns position of the item in array.
 * If item wasn't found then function returns closest position and sets
 * foundPtr to false. If value is outside the range covered by partitions
 * then returns -1.
 */
int
range_binary_search(const RangeRelation *rangerel, FmgrInfo *cmp_func, Datum value, bool *foundPtr)
{
	RangeEntry *ranges = dsm_array_get_pointer(&rangerel->ranges);
	RangeEntry *re;
	bool		byVal = rangerel->by_val;
	int			cmp_min,
				cmp_max,
				i = 0,
				startidx = 0,
				endidx = rangerel->ranges.length-1;
#ifdef USE_ASSERT_CHECKING
	int			counter = 0;
#endif

	*foundPtr = false;

	/* Check boundaries */
	cmp_min = FunctionCall2(cmp_func, value, PATHMAN_GET_DATUM(ranges[0].min, byVal)),
	cmp_max = FunctionCall2(cmp_func, value, PATHMAN_GET_DATUM(ranges[rangerel->ranges.length - 1].max, byVal));

	if (cmp_min < 0 || cmp_max >= 0)
	{
		return -1;
	}

	while (true)
	{
		i = startidx + (endidx - startidx) / 2;
		Assert(i >= 0 && i < rangerel->ranges.length);
		re = &ranges[i];
		cmp_min = FunctionCall2(cmp_func, value, PATHMAN_GET_DATUM(re->min, byVal));
		cmp_max = FunctionCall2(cmp_func, value, PATHMAN_GET_DATUM(re->max, byVal));

		if (cmp_min >= 0 && cmp_max < 0)
		{
			*foundPtr = true;
			break;
		}

		if (startidx >= endidx)
			return i;

		if (cmp_min < 0)
			endidx = i - 1;
		else if (cmp_max >= 0)
			startidx = i + 1;

		/* For debug's sake */
		Assert(++counter < 100);
	}

	return i;
}

static Const *
extract_const(WalkerContext *wcxt, Param *param)
{
	ExprState  *estate = ExecInitExpr((Expr *) param, NULL);
	bool		isnull;
	Datum		value = ExecEvalExpr(estate, wcxt->econtext, &isnull, NULL);

	return makeConst(param->paramtype, param->paramtypmod,
					 param->paramcollid, get_typlen(param->paramtype),
					 value, isnull, get_typbyval(param->paramtype));
}

/*
 * Operator expression handler
 */
static WrapperNode *
handle_opexpr(const OpExpr *expr, WalkerContext *context)
{
	WrapperNode	*result = (WrapperNode *)palloc(sizeof(WrapperNode));
	Node		*var, *param;
	const PartRelationInfo *prel = context->prel;

	result->orig = (const Node *)expr;
	result->args = NIL;

	if (list_length(expr->args) == 2)
	{
		if (pull_var_param(context, expr, &var, &param))
		{
			if (IsConstValue(context, param))
			{
				handle_binary_opexpr(context, result, var, ExtractConst(context, param));
				return result;
			}
			else if (IsA(param, Param) || IsA(param, Var))
			{
				handle_binary_opexpr_param(prel, result, var);
				return result;
			}
		}
	}

	result->rangeset = list_make1_irange(make_irange(0, prel->children_count - 1, true));
	result->paramsel = 1.0;
	return result;
}

/*
 * Checks if expression is a KEY OP PARAM or PARAM OP KEY,
 * where KEY is partition key (it could be Var or RelableType) and PARAM is
 * whatever. Function returns variable (or RelableType) and param via var_ptr 
 * and param_ptr pointers. If partition key isn't in expression then function
 * returns false.
 */
static bool
pull_var_param(const WalkerContext *ctx, const OpExpr *expr, Node **var_ptr, Node **param_ptr)
{
	Node   *left = linitial(expr->args),
		   *right = lsecond(expr->args);
	Var	   *v = NULL;

	/* Check the case when variable is on the left side */
	if (IsA(left, Var) || IsA(left, RelabelType))
	{
		v = !IsA(left, RelabelType) ?
						(Var *) left :
						(Var *) ((RelabelType *) left)->arg;

		if (v->varattno == ctx->prel->attnum)
		{
			*var_ptr = left;
			*param_ptr = right;
			return true;
		}
	}
	
	/* ... variable is on the right side */
	if (IsA(right, Var) || IsA(right, RelabelType))
	{
		v = !IsA(right, RelabelType) ?
						(Var *) right :
						(Var *) ((RelabelType *) right)->arg;

		if (v->varattno == ctx->prel->attnum)
		{
			*var_ptr = right;
			*param_ptr = left;
			return true;
		}
	}

	/* Variable isn't a partitionig key */
	return false;
}

/*
 * Boolean expression handler
 */
static WrapperNode *
handle_boolexpr(const BoolExpr *expr, WalkerContext *context)
{
	WrapperNode	*result = (WrapperNode *)palloc(sizeof(WrapperNode));
	ListCell	*lc;
	const PartRelationInfo *prel = context->prel;

	result->orig = (const Node *)expr;
	result->args = NIL;
	result->paramsel = 1.0;

	if (expr->boolop == AND_EXPR)
		result->rangeset = list_make1_irange(make_irange(0, prel->children_count - 1, false));
	else
		result->rangeset = NIL;

	foreach (lc, expr->args)
	{
		WrapperNode *arg;

		arg = walk_expr_tree((Expr *)lfirst(lc), context);
		result->args = lappend(result->args, arg);
		switch (expr->boolop)
		{
			case OR_EXPR:
				// finish_least_greatest(arg, context);
				result->rangeset = irange_list_union(result->rangeset, arg->rangeset);
				break;
			case AND_EXPR:
				result->rangeset = irange_list_intersect(result->rangeset, arg->rangeset);
				result->paramsel *= arg->paramsel;
				break;
			default:
				result->rangeset = list_make1_irange(make_irange(0, prel->children_count - 1, false));
				break;
		}
	}

	if (expr->boolop == OR_EXPR)
	{
		int totallen = irange_list_length(result->rangeset);

		foreach (lc, result->args)
		{
			WrapperNode *arg = (WrapperNode *) lfirst(lc);
			int len = irange_list_length(arg->rangeset);

			result->paramsel *= (1.0 - arg->paramsel * (double)len / (double)totallen);
		}
		result->paramsel = 1.0 - result->paramsel;
	}

	return result;
}

/*
 * Scalar array expression
 */
static WrapperNode *
handle_arrexpr(const ScalarArrayOpExpr *expr, WalkerContext *context)
{
	WrapperNode *result = (WrapperNode *)palloc(sizeof(WrapperNode));
	Node		*varnode = (Node *) linitial(expr->args);
	Var			*var;
	Node		*arraynode = (Node *) lsecond(expr->args);
	int			 hash;
	const PartRelationInfo *prel = context->prel;

	result->orig = (const Node *)expr;
	result->args = NIL;
	result->paramsel = 1.0;

	Assert(varnode != NULL);

	/* If variable is not the partition key then skip it */
	if (IsA(varnode, Var) || IsA(varnode, RelabelType))
	{
		var = !IsA(varnode, RelabelType) ?
			(Var *) varnode :
			(Var *) ((RelabelType *) varnode)->arg;
		if (var->varattno != prel->attnum)
			goto handle_arrexpr_return;
	}
	else
		goto handle_arrexpr_return;

	if (arraynode && IsA(arraynode, Const) &&
		!((Const *) arraynode)->constisnull)
	{
		ArrayType  *arrayval;
		int16		elmlen;
		bool		elmbyval;
		char		elmalign;
		int			num_elems;
		Datum	   *elem_values;
		bool	   *elem_nulls;
		int			i;
		Datum		value;
		uint32		int_value;

		/* Extract values from array */
		arrayval = DatumGetArrayTypeP(((Const *) arraynode)->constvalue);
		get_typlenbyvalalign(ARR_ELEMTYPE(arrayval),
							 &elmlen, &elmbyval, &elmalign);
		deconstruct_array(arrayval,
						  ARR_ELEMTYPE(arrayval),
						  elmlen, elmbyval, elmalign,
						  &elem_values, &elem_nulls, &num_elems);

		result->rangeset = NIL;

		/* Construct OIDs list */
		for (i = 0; i < num_elems; i++)
		{
			/* Invoke base hash function for value type */
			value = OidFunctionCall1(prel->hash_proc, elem_values[i]);
			int_value = DatumGetUInt32(value);
			hash = make_hash(int_value, prel->children_count);
			result->rangeset = irange_list_union(result->rangeset,
						list_make1_irange(make_irange(hash, hash, true)));
		}

		/* Free resources */
		pfree(elem_values);
		pfree(elem_nulls);

		return result;
	}

	if (arraynode && IsA(arraynode, Param))
		result->paramsel = DEFAULT_INEQ_SEL;

handle_arrexpr_return:
	result->rangeset = list_make1_irange(make_irange(0, prel->children_count - 1, true));
	return result;
}

/*
 * Theres are functions below copied from allpaths.c with (or without) some
 * modifications. Couldn't use original because of 'static' modifier.
 */

/*
 * set_plain_rel_size
 *	  Set size estimates for a plain relation (no subquery, no inheritance)
 */
static void
set_plain_rel_size(PlannerInfo *root, RelOptInfo *rel, RangeTblEntry *rte)
{
	/*
	 * Test any partial indexes of rel for applicability.  We must do this
	 * first since partial unique indexes can affect size estimates.
	 */
	check_partial_indexes(root, rel);

	/* Mark rel with estimated output rows, width, etc */
	set_baserel_size_estimates(root, rel);
}

/*
 * set_plain_rel_pathlist
 *	  Build access paths for a plain relation (no subquery, no inheritance)
 */
static void
set_plain_rel_pathlist(PlannerInfo *root, RelOptInfo *rel, RangeTblEntry *rte)
{
	Relids		required_outer;
	Path *path;

	/*
	 * We don't support pushing join clauses into the quals of a seqscan, but
	 * it could still have required parameterization due to LATERAL refs in
	 * its tlist.
	 */
	required_outer = rel->lateral_relids;

	/* Consider sequential scan */
#if PG_VERSION_NUM >= 90600
	path = create_seqscan_path(root, rel, required_outer, 0);
#else
	path = create_seqscan_path(root, rel, required_outer);
#endif
	add_path(rel, path);
	// set_pathkeys(root, rel, path);

	/* Consider index scans */
	create_index_paths(root, rel);

	/* Consider TID scans */
	create_tidscan_paths(root, rel);
}

/*
 * set_foreign_size
 *		Set size estimates for a foreign table RTE
 */
static void
set_foreign_size(PlannerInfo *root, RelOptInfo *rel, RangeTblEntry *rte)
{
	/* Mark rel with estimated output rows, width, etc */
	set_foreign_size_estimates(root, rel);

	/* Let FDW adjust the size estimates, if it can */
	rel->fdwroutine->GetForeignRelSize(root, rel, rte->relid);

	/* ... but do not let it set the rows estimate to zero */
	rel->rows = clamp_row_est(rel->rows);
}

/*
 * set_foreign_pathlist
 *		Build access paths for a foreign table RTE
 */
static void
set_foreign_pathlist(PlannerInfo *root, RelOptInfo *rel, RangeTblEntry *rte)
{
	/* Call the FDW's GetForeignPaths function to generate path(s) */
	rel->fdwroutine->GetForeignPaths(root, rel, rte->relid);
}

/*
 * set_append_rel_pathlist
 *	  Build access paths for an "append relation"
 */
void
set_append_rel_pathlist(PlannerInfo *root, RelOptInfo *rel,
						Index rti, RangeTblEntry *rte,
						PathKey *pathkeyAsc, PathKey *pathkeyDesc)
{
	int			parentRTindex = rti;
	List	   *live_childrels = NIL;
	List	   *subpaths = NIL;
	bool		subpaths_valid = true;
	List	   *all_child_pathkeys = NIL;
	List	   *all_child_outers = NIL;
	ListCell   *l;

	/*
	 * Generate access paths for each member relation, and remember the
	 * cheapest path for each one.  Also, identify all pathkeys (orderings)
	 * and parameterizations (required_outer sets) available for the member
	 * relations.
	 */
	foreach(l, root->append_rel_list)
	{
		AppendRelInfo *appinfo = (AppendRelInfo *) lfirst(l);
		int			childRTindex;
		RangeTblEntry *childRTE;
		RelOptInfo *childrel;
		ListCell   *lcp;

		/* append_rel_list contains all append rels; ignore others */
		if (appinfo->parent_relid != parentRTindex)
			continue;

		/* Re-locate the child RTE and RelOptInfo */
		childRTindex = appinfo->child_relid;
		childRTE = root->simple_rte_array[childRTindex];
		childrel = root->simple_rel_array[childRTindex];

		/*
		 * Compute the child's access paths.
		 */
		if (childRTE->relkind == RELKIND_FOREIGN_TABLE)
		{
			set_foreign_size(root, childrel, childRTE);
			set_foreign_pathlist(root, childrel, childRTE);
		}
		else
		{
			set_plain_rel_size(root, childrel, childRTE);
			set_plain_rel_pathlist(root, childrel, childRTE);
		}
		set_cheapest(childrel);

		/*
		 * If child is dummy, ignore it.
		 */
		if (IS_DUMMY_REL(childrel))
			continue;

		/*
		 * Child is live, so add it to the live_childrels list for use below.
		 */
		live_childrels = lappend(live_childrels, childrel);

		/*
		 * If child has an unparameterized cheapest-total path, add that to
		 * the unparameterized Append path we are constructing for the parent.
		 * If not, there's no workable unparameterized path.
		 */
		if (childrel->cheapest_total_path->param_info == NULL)
			subpaths = accumulate_append_subpath(subpaths,
											  childrel->cheapest_total_path);
		else
			subpaths_valid = false;

		/*
		 * Collect lists of all the available path orderings and
		 * parameterizations for all the children.  We use these as a
		 * heuristic to indicate which sort orderings and parameterizations we
		 * should build Append and MergeAppend paths for.
		 */
		foreach(lcp, childrel->pathlist)
		{
			Path	   *childpath = (Path *) lfirst(lcp);
			List	   *childkeys = childpath->pathkeys;
			Relids		childouter = PATH_REQ_OUTER(childpath);

			/* Unsorted paths don't contribute to pathkey list */
			if (childkeys != NIL)
			{
				ListCell   *lpk;
				bool		found = false;

				/* Have we already seen this ordering? */
				foreach(lpk, all_child_pathkeys)
				{
					List	   *existing_pathkeys = (List *) lfirst(lpk);

					if (compare_pathkeys(existing_pathkeys,
										 childkeys) == PATHKEYS_EQUAL)
					{
						found = true;
						break;
					}
				}
				if (!found)
				{
					/* No, so add it to all_child_pathkeys */
					all_child_pathkeys = lappend(all_child_pathkeys,
												 childkeys);
				}
			}

			/* Unparameterized paths don't contribute to param-set list */
			if (childouter)
			{
				ListCell   *lco;
				bool		found = false;

				/* Have we already seen this param set? */
				foreach(lco, all_child_outers)
				{
					Relids		existing_outers = (Relids) lfirst(lco);

					if (bms_equal(existing_outers, childouter))
					{
						found = true;
						break;
					}
				}
				if (!found)
				{
					/* No, so add it to all_child_outers */
					all_child_outers = lappend(all_child_outers,
											   childouter);
				}
			}
		}
	}

	/*
	 * If we found unparameterized paths for all children, build an unordered,
	 * unparameterized Append path for the rel.  (Note: this is correct even
	 * if we have zero or one live subpath due to constraint exclusion.)
	 */
	if (subpaths_valid)
		add_path(rel, (Path *) create_append_path(rel, subpaths, NULL));

	/*
	 * Also build unparameterized MergeAppend paths based on the collected
	 * list of child pathkeys.
	 */
	if (subpaths_valid)
		generate_mergeappend_paths(root, rel, live_childrels,
								   all_child_pathkeys, pathkeyAsc,
								   pathkeyDesc);

	/*
	 * Build Append paths for each parameterization seen among the child rels.
	 * (This may look pretty expensive, but in most cases of practical
	 * interest, the child rels will expose mostly the same parameterizations,
	 * so that not that many cases actually get considered here.)
	 *
	 * The Append node itself cannot enforce quals, so all qual checking must
	 * be done in the child paths.  This means that to have a parameterized
	 * Append path, we must have the exact same parameterization for each
	 * child path; otherwise some children might be failing to check the
	 * moved-down quals.  To make them match up, we can try to increase the
	 * parameterization of lesser-parameterized paths.
	 */
	foreach(l, all_child_outers)
	{
		Relids		required_outer = (Relids) lfirst(l);
		ListCell   *lcr;

		/* Select the child paths for an Append with this parameterization */
		subpaths = NIL;
		subpaths_valid = true;
		foreach(lcr, live_childrels)
		{
			RelOptInfo *childrel = (RelOptInfo *) lfirst(lcr);
			Path	   *subpath;

			subpath = get_cheapest_parameterized_child_path(root,
															childrel,
															required_outer);
			if (subpath == NULL)
			{
				/* failed to make a suitable path for this child */
				subpaths_valid = false;
				break;
			}
			subpaths = accumulate_append_subpath(subpaths, subpath);
		}

		if (subpaths_valid)
			add_path(rel, (Path *)
					 create_append_path(rel, subpaths, required_outer));
	}
}

static List *
accumulate_append_subpath(List *subpaths, Path *path)
{
	return lappend(subpaths, path);
}

/*
 * get_cheapest_parameterized_child_path
 *		Get cheapest path for this relation that has exactly the requested
 *		parameterization.
 *
 * Returns NULL if unable to create such a path.
 */
static Path *
get_cheapest_parameterized_child_path(PlannerInfo *root, RelOptInfo *rel,
									  Relids required_outer)
{
	Path	   *cheapest;
	ListCell   *lc;

	/*
	 * Look up the cheapest existing path with no more than the needed
	 * parameterization.  If it has exactly the needed parameterization, we're
	 * done.
	 */
	cheapest = get_cheapest_path_for_pathkeys(rel->pathlist,
											  NIL,
											  required_outer,
											  TOTAL_COST);
	Assert(cheapest != NULL);
	if (bms_equal(PATH_REQ_OUTER(cheapest), required_outer))
		return cheapest;

	/*
	 * Otherwise, we can "reparameterize" an existing path to match the given
	 * parameterization, which effectively means pushing down additional
	 * joinquals to be checked within the path's scan.  However, some existing
	 * paths might check the available joinquals already while others don't;
	 * therefore, it's not clear which existing path will be cheapest after
	 * reparameterization.  We have to go through them all and find out.
	 */
	cheapest = NULL;
	foreach(lc, rel->pathlist)
	{
		Path	   *path = (Path *) lfirst(lc);

		/* Can't use it if it needs more than requested parameterization */
		if (!bms_is_subset(PATH_REQ_OUTER(path), required_outer))
			continue;

		/*
		 * Reparameterization can only increase the path's cost, so if it's
		 * already more expensive than the current cheapest, forget it.
		 */
		if (cheapest != NULL &&
			compare_path_costs(cheapest, path, TOTAL_COST) <= 0)
			continue;

		/* Reparameterize if needed, then recheck cost */
		if (!bms_equal(PATH_REQ_OUTER(path), required_outer))
		{
			path = reparameterize_path(root, path, required_outer, 1.0);
			if (path == NULL)
				continue;		/* failed to reparameterize this one */
			Assert(bms_equal(PATH_REQ_OUTER(path), required_outer));

			if (cheapest != NULL &&
				compare_path_costs(cheapest, path, TOTAL_COST) <= 0)
				continue;
		}

		/* We have a new best path */
		cheapest = path;
	}

	/* Return the best path, or NULL if we found no suitable candidate */
	return cheapest;
}


//---------------------------------------------------------------

/*
 * generate_mergeappend_paths
 *		Generate MergeAppend paths for an append relation
 *
 * Generate a path for each ordering (pathkey list) appearing in
 * all_child_pathkeys.
 *
 * We consider both cheapest-startup and cheapest-total cases, ie, for each
 * interesting ordering, collect all the cheapest startup subpaths and all the
 * cheapest total paths, and build a MergeAppend path for each case.
 *
 * We don't currently generate any parameterized MergeAppend paths.  While
 * it would not take much more code here to do so, it's very unclear that it
 * is worth the planning cycles to investigate such paths: there's little
 * use for an ordered path on the inside of a nestloop.  In fact, it's likely
 * that the current coding of add_path would reject such paths out of hand,
 * because add_path gives no credit for sort ordering of parameterized paths,
 * and a parameterized MergeAppend is going to be more expensive than the
 * corresponding parameterized Append path.  If we ever try harder to support
 * parameterized mergejoin plans, it might be worth adding support for
 * parameterized MergeAppends to feed such joins.  (See notes in
 * optimizer/README for why that might not ever happen, though.)
 */
static void
generate_mergeappend_paths(PlannerInfo *root, RelOptInfo *rel,
						   List *live_childrels,
						   List *all_child_pathkeys,
						   PathKey *pathkeyAsc, PathKey *pathkeyDesc)
{
	ListCell   *lcp;

	foreach(lcp, all_child_pathkeys)
	{
		List	   *pathkeys = (List *) lfirst(lcp);
		List	   *startup_subpaths = NIL;
		List	   *total_subpaths = NIL;
		bool		startup_neq_total = false;
		bool		presorted = true;
		ListCell   *lcr;

		/* Select the child paths for this ordering... */
		foreach(lcr, live_childrels)
		{
			RelOptInfo *childrel = (RelOptInfo *) lfirst(lcr);
			Path	   *cheapest_startup,
					   *cheapest_total;

			/* Locate the right paths, if they are available. */
			cheapest_startup =
				get_cheapest_path_for_pathkeys(childrel->pathlist,
											   pathkeys,
											   NULL,
											   STARTUP_COST);
			cheapest_total =
				get_cheapest_path_for_pathkeys(childrel->pathlist,
											   pathkeys,
											   NULL,
											   TOTAL_COST);

			/*
			 * If we can't find any paths with the right order just use the
			 * cheapest-total path; we'll have to sort it later.
			 */
			if (cheapest_startup == NULL || cheapest_total == NULL)
			{
				cheapest_startup = cheapest_total =
					childrel->cheapest_total_path;
				/* Assert we do have an unparameterized path for this child */
				Assert(cheapest_total->param_info == NULL);
				presorted = false;
			}

			/*
			 * Notice whether we actually have different paths for the
			 * "cheapest" and "total" cases; frequently there will be no point
			 * in two create_merge_append_path() calls.
			 */
			if (cheapest_startup != cheapest_total)
				startup_neq_total = true;

			startup_subpaths =
				accumulate_append_subpath(startup_subpaths, cheapest_startup);
			total_subpaths =
				accumulate_append_subpath(total_subpaths, cheapest_total);
		}

		/*
		 * When first pathkey matching ascending/descending sort by partition
		 * column then build path with Append node, because MergeAppend is not
		 * required in this case.
		 */
		if ((PathKey *) linitial(pathkeys) == pathkeyAsc && presorted)
		{
			Path *path;

			path = (Path *) create_append_path(rel, startup_subpaths, NULL);
			path->pathkeys = pathkeys;
			add_path(rel, path);

			if (startup_neq_total)
			{
				path = (Path *) create_append_path(rel, total_subpaths, NULL);
				path->pathkeys = pathkeys;
				add_path(rel, path);
			}
		}
		else if ((PathKey *) linitial(pathkeys) == pathkeyDesc && presorted)
		{
			/*
			 * When pathkey is descending sort by partition column then we
			 * need to scan partitions in reversed order.
			 */
			Path *path;

			path = (Path *) create_append_path(rel,
								list_reverse(startup_subpaths), NULL);
			path->pathkeys = pathkeys;
			add_path(rel, path);

			if (startup_neq_total)
			{
				path = (Path *) create_append_path(rel,
								list_reverse(total_subpaths), NULL);
				path->pathkeys = pathkeys;
				add_path(rel, path);
			}
		}
		else
		{
			/* ... and build the MergeAppend paths */
			add_path(rel, (Path *) create_merge_append_path(root,
															rel,
															startup_subpaths,
															pathkeys,
															NULL));
			if (startup_neq_total)
				add_path(rel, (Path *) create_merge_append_path(root,
																rel,
																total_subpaths,
																pathkeys,
																NULL));
		}
	}
}
