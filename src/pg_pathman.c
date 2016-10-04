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

#include "pg_compat.h"

#include "pathman.h"
#include "init.h"
#include "hooks.h"
#include "utils.h"
#include "partition_filter.h"
#include "runtimeappend.h"
#include "runtime_merge_append.h"
#include "xact_handling.h"

#include "postgres.h"
#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/transam.h"
#include "access/xact.h"
#include "catalog/pg_cast.h"
#include "catalog/pg_type.h"
#include "executor/spi.h"
#include "foreign/fdwapi.h"
#include "fmgr.h"
#include "miscadmin.h"
#include "optimizer/clauses.h"
#include "optimizer/prep.h"
#include "optimizer/restrictinfo.h"
#include "optimizer/cost.h"
#include "utils/builtins.h"
#include "utils/datum.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/syscache.h"
#include "utils/selfuncs.h"
#include "utils/snapmgr.h"
#include "utils/typcache.h"

PG_MODULE_MAGIC;


List		   *inheritance_disabled_relids = NIL;
List		   *inheritance_enabled_relids = NIL;
PathmanState   *pmstate;
Oid				pathman_config_relid = InvalidOid;
Oid				pathman_config_params_relid = InvalidOid;


/* pg module functions */
void _PG_init(void);

/* Utility functions */
static Node *wrapper_make_expression(WrapperNode *wrap, int index, bool *alwaysTrue);
static bool disable_inheritance_subselect_walker(Node *node, void *context);

/* "Partition creation"-related functions */
static Datum extract_binary_interval_from_text(Datum interval_text,
											   Oid part_atttype,
											   Oid *interval_type);
static bool spawn_partitions(Oid partitioned_rel,
							 Datum value,
							 Datum leading_bound,
							 Oid leading_bound_type,
							 FmgrInfo *cmp_proc,
							 Datum interval_binary,
							 Oid interval_type,
							 bool forward,
							 Oid *last_partition);

/* Expression tree handlers */
static WrapperNode *handle_const(const Const *c, WalkerContext *context);
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
#define check_lt(finfo, arg1, arg2) \
	((int) FunctionCall2(finfo, arg1, arg2) < 0)
#define check_le(finfo, arg1, arg2) \
	((int) FunctionCall2(finfo, arg1, arg2) <= 0)
#define check_eq(finfo, arg1, arg2) \
	((int) FunctionCall2(finfo, arg1, arg2) == 0)
#define check_ge(finfo, arg1, arg2) \
	((int) FunctionCall2(finfo, arg1, arg2) >= 0)
#define check_gt(finfo, arg1, arg2) \
	((int) FunctionCall2(finfo, arg1, arg2) > 0)

/* We can transform Param into Const provided that 'econtext' is available */
#define IsConstValue(wcxt, node) \
	( IsA((node), Const) || (WcxtHasExprContext(wcxt) ? IsA((node), Param) : false) )

#define ExtractConst(wcxt, node) \
	( IsA((node), Param) ? extract_const((wcxt), (Param *) (node)) : ((Const *) (node)) )


/*
 * Set initial values for all Postmaster's forks.
 */
void
_PG_init(void)
{
	PathmanInitState	temp_init_state;

	if (!process_shared_preload_libraries_in_progress)
	{
		elog(ERROR, "pg_pathman module must be initialized by Postmaster. "
					"Put the following line to configuration file: "
					"shared_preload_libraries='pg_pathman'");
	}

	/* Request additional shared resources */
	RequestAddinShmemSpace(estimate_pathman_shmem_size());

	/* NOTE: we don't need LWLocks now. RequestAddinLWLocks(1); */

	/* Assign pg_pathman's initial state */
	temp_init_state.initialization_needed = true;
	temp_init_state.pg_pathman_enable = true;

	/* Apply initial state */
	restore_pathman_init_state(&temp_init_state);

	/* Initialize 'next' hook pointers */
	set_rel_pathlist_hook_next		= set_rel_pathlist_hook;
	set_rel_pathlist_hook			= pathman_rel_pathlist_hook;
	set_join_pathlist_next			= set_join_pathlist_hook;
	set_join_pathlist_hook			= pathman_join_pathlist_hook;
	shmem_startup_hook_next			= shmem_startup_hook;
	shmem_startup_hook				= pathman_shmem_startup_hook;
	post_parse_analyze_hook_next	= post_parse_analyze_hook;
	post_parse_analyze_hook			= pathman_post_parse_analysis_hook;
	planner_hook_next				= planner_hook;
	planner_hook					= pathman_planner_hook;
	process_utility_hook_next		= ProcessUtility_hook;
	ProcessUtility_hook				= pathman_process_utility_hook;

	/* Initialize static data for all subsystems */
	init_main_pathman_toggles();
	init_runtimeappend_static_data();
	init_runtime_merge_append_static_data();
	init_partition_filter_static_data();
}

/*
 * Disables inheritance for partitioned by pathman relations.
 * It must be done to prevent PostgresSQL from exhaustive search.
 */
void
disable_inheritance(Query *parse)
{
	const PartRelationInfo *prel;
	RangeTblEntry		   *rte;
	MemoryContext			oldcontext;
	ListCell			   *lc;

	/* If query contains CTE (WITH statement) then handle subqueries too */
	disable_inheritance_cte(parse);

	/* If query contains subselects */
	disable_inheritance_subselect(parse);

	foreach(lc, parse->rtable)
	{
		rte = (RangeTblEntry *) lfirst(lc);

		switch(rte->rtekind)
		{
			case RTE_RELATION:
				if (rte->inh)
				{
					/* Look up this relation in pathman local cache */
					prel = get_pathman_relation_info(rte->relid);
					if (prel)
					{
						/* We'll set this flag later */
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

void
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

void
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
 * Checks if query affects only one partition. If true then substitute
 */
void
handle_modification_query(Query *parse)
{
	const PartRelationInfo *prel;
	List				   *ranges;
	RangeTblEntry		   *rte;
	WrapperNode			   *wrap;
	Expr				   *expr;
	WalkerContext			context;

	Assert(parse->commandType == CMD_UPDATE ||
		   parse->commandType == CMD_DELETE);
	Assert(parse->resultRelation > 0);

	rte = rt_fetch(parse->resultRelation, parse->rtable);
	prel = get_pathman_relation_info(rte->relid);

	if (!prel)
		return;

	/* Parse syntax tree and extract partition ranges */
	ranges = list_make1_irange(make_irange(0, PrelLastChild(prel), false));
	expr = (Expr *) eval_const_expressions(NULL, parse->jointree->quals);
	if (!expr)
		return;

	/* Parse syntax tree and extract partition ranges */
	InitWalkerContext(&context, prel, NULL, false);
	wrap = walk_expr_tree(expr, &context);

	ranges = irange_list_intersect(ranges, wrap->rangeset);

	/* If only one partition is affected then substitute parent table with partition */
	if (irange_list_length(ranges) == 1)
	{
		IndexRange irange = linitial_irange(ranges);
		if (irange.ir_lower == irange.ir_upper)
		{
			Oid *children = PrelGetChildrenArray(prel);
			rte->relid = children[irange.ir_lower];
			rte->inh = false;
		}
	}

	return;
}

/*
 * Creates child relation and adds it to root.
 * Returns child index in simple_rel_array
 */
int
append_child_relation(PlannerInfo *root, RelOptInfo *rel, Index rti,
	RangeTblEntry *rte, int index, Oid childOid, List *wrappers)
{
	RangeTblEntry  *childrte;
	RelOptInfo	   *childrel;
	Index			childRTindex;
	AppendRelInfo  *appinfo;
	ListCell	   *lc,
				   *lc2;
	Relation		newrelation;
	PlanRowMark	   *parent_rowmark;
	PlanRowMark	   *child_rowmark;
	AttrNumber		i;

	/* FIXME: acquire a suitable lock on partition */
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
	copy_targetlist_compat(childrel, rel);

	/* Copy attr_needed & attr_widths */
	childrel->attr_needed = (Relids *)
		palloc0((rel->max_attr - rel->min_attr + 1) * sizeof(Relids));
	childrel->attr_widths = (int32 *)
		palloc0((rel->max_attr - rel->min_attr + 1) * sizeof(int32));

	for (i = 0; i < rel->max_attr - rel->min_attr + 1; i++)
		childrel->attr_needed[i] = bms_copy(rel->attr_needed[i]);

	memcpy(childrel->attr_widths, rel->attr_widths,
		   (rel->max_attr - rel->min_attr + 1) * sizeof(int32));

	/*
	 * Copy restrictions. If it's not the parent table then copy only those
	 * restrictions that reference to this partition
	 */
	childrel->baserestrictinfo = NIL;
	if (rte->relid != childOid)
	{
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
	}
	/* If it's the parent table then copy all restrictions */
	else
	{
		foreach(lc, rel->baserestrictinfo)
		{
			RestrictInfo *rinfo = (RestrictInfo *) lfirst(lc);
			RestrictInfo *new_rinfo = (RestrictInfo *) copyObject(rinfo);

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

	/* Close child relations, but keep locks */
	heap_close(newrelation, NoLock);


	/* Create rowmarks required for child rels */
	parent_rowmark = get_plan_rowmark(root->rowMarks, rti);
	if (parent_rowmark)
	{
		child_rowmark = makeNode(PlanRowMark);

		child_rowmark->rti = childRTindex;
		child_rowmark->prti = rti;
		child_rowmark->rowmarkId = parent_rowmark->rowmarkId;
		/* Reselect rowmark type, because relkind might not match parent */
		child_rowmark->markType = select_rowmark_type(childrte,
													  parent_rowmark->strength);
		child_rowmark->allMarkTypes = (1 << child_rowmark->markType);
		child_rowmark->strength = parent_rowmark->strength;
		child_rowmark->waitPolicy = parent_rowmark->waitPolicy;
		child_rowmark->isParent = false;

		/* Include child's rowmark type in parent's allMarkTypes */
		parent_rowmark->allMarkTypes |= child_rowmark->allMarkTypes;

		root->rowMarks = lappend(root->rowMarks, child_rowmark);

		parent_rowmark->isParent = true;
	}

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
		BoolExpr	   *result;

		if (expr->boolop == OR_EXPR || expr->boolop == AND_EXPR)
		{
			ListCell *lc;
			List *args = NIL;

			foreach (lc, wrap->args)
			{
				Node   *arg;
				bool	childAlwaysTrue;

				arg = wrapper_make_expression((WrapperNode *) lfirst(lc),
											  index, &childAlwaysTrue);
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
			return (Node *) result;
		}
		else
			return copyObject(wrap->orig);
	}
	else
		return copyObject(wrap->orig);
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
		/* Useful for INSERT optimization */
		case T_Const:
			return handle_const((Const *) expr, context);

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
			result = (WrapperNode *) palloc(sizeof(WrapperNode));
			result->orig = (const Node *) expr;
			result->args = NIL;
			result->rangeset = list_make1_irange(
						make_irange(0, PrelLastChild(context->prel), true));
			result->paramsel = 1.0;
			return result;
	}
}

/*
 * Append\prepend partitions if there's no partition to store 'value'.
 *
 * Used by create_partitions_internal().
 *
 * NB: 'value' type is not needed since we've already taken
 * it into account while searching for the 'cmp_proc'.
 */
static bool
spawn_partitions(Oid partitioned_rel,		/* parent's Oid */
				 Datum value,				/* value to be INSERTed */
				 Datum leading_bound,		/* current global min\max */
				 Oid leading_bound_type,	/* type of the boundary */
				 FmgrInfo *cmp_proc,		/* cmp(value, leading_bound) */
				 Datum interval_binary,		/* interval in binary form */
				 Oid interval_type,			/* INTERVALOID or prel->atttype */
				 bool forward,				/* append\prepend */
				 Oid *last_partition)		/* result (Oid of the last partition) */
{
/* Cache "+"(leading_bound, interval) or "-"(leading_bound, interval) operator */
#define CacheOperator(finfo, opname, arg1, arg2, is_cached) \
	do { \
		if (!is_cached) \
		{ \
			fmgr_info(get_binary_operator_oid((opname), (arg1), (arg2)), \
					  (finfo)); \
			is_cached = true; \
		} \
	} while (0)

/* Use "<" for prepend & ">=" for append */
#define do_compare(compar, a, b, fwd) \
	( \
		(fwd) ? \
			check_ge((compar), (a), (b)) : \
			check_lt((compar), (a), (b)) \
	)

	FmgrInfo 	interval_move_bound; /* function to move upper\lower boundary */
	bool		interval_move_bound_cached = false; /* is it cached already? */
	bool		spawned = false;

	Datum		cur_part_leading = leading_bound;

	char	   *query;

	/* Create querty statement */
	query = psprintf("SELECT part::regclass "
					 "FROM %s.create_single_range_partition($1, $2, $3) AS part",
					 get_namespace_name(get_pathman_schema()));

	/* Execute comparison function cmp(value, cur_part_leading) */
	while (do_compare(cmp_proc, value, cur_part_leading, forward))
	{
		char   *nulls = NULL; /* no params are NULL */
		Oid		types[3] = { REGCLASSOID, leading_bound_type, leading_bound_type };
		Datum	values[3];
		int		ret;

		/* Assign the 'following' boundary to current 'leading' value */
		Datum	cur_part_following = cur_part_leading;

		CacheOperator(&interval_move_bound, (forward ? "+" : "-"),
					  leading_bound_type, interval_type, interval_move_bound_cached);

		/* Move leading bound by interval (leading +\- INTERVAL) */
		cur_part_leading = FunctionCall2(&interval_move_bound,
										 cur_part_leading,
										 interval_binary);

		/* Fill in 'values' with parent's Oid and correct boundaries... */
		values[0] = partitioned_rel; /* partitioned table's Oid */
		values[1] = forward ? cur_part_following : cur_part_leading; /* value #1 */
		values[2] = forward ? cur_part_leading : cur_part_following; /* value #2 */

		/* ...and create partition */
		ret = SPI_execute_with_args(query, 3, types, values, nulls, false, 0);
		if (ret != SPI_OK_SELECT)
			elog(ERROR, "Could not spawn a partition");

		/* Set 'last_partition' if necessary */
		if (last_partition)
		{
			HeapTuple	htup = SPI_tuptable->vals[0];
			Datum		partid;
			bool		isnull;

			Assert(SPI_processed == 1);
			Assert(SPI_tuptable->tupdesc->natts == 1);
			partid = SPI_getbinval(htup, SPI_tuptable->tupdesc, 1, &isnull);

			*last_partition = DatumGetObjectId(partid);
		}

#ifdef USE_ASSERT_CHECKING
		elog(DEBUG2, "%s partition with following='%s' & leading='%s' [%u]",
			 (forward ? "Appending" : "Prepending"),
			 DebugPrintDatum(cur_part_following, leading_bound_type),
			 DebugPrintDatum(cur_part_leading, leading_bound_type),
			 MyProcPid);
#endif

		/* We have spawned at least 1 partition */
		spawned = true;
	}

	pfree(query);

	return spawned;
}

/*
 * Convert interval from TEXT to binary form using partitioned column's type.
 */
static Datum
extract_binary_interval_from_text(Datum interval_text,	/* interval as TEXT */
								  Oid part_atttype,		/* partitioned column's type */
								  Oid *interval_type)	/* returned value */
{
	Datum		interval_binary;
	const char *interval_cstring;

	interval_cstring = TextDatumGetCString(interval_text);

	/* If 'part_atttype' is a *date type*, cast 'range_interval' to INTERVAL */
	if (is_date_type_internal(part_atttype))
	{
		int32	interval_typmod = PATHMAN_CONFIG_interval_typmod;

		/* Convert interval from CSTRING to internal form */
		interval_binary = DirectFunctionCall3(interval_in,
											  CStringGetDatum(interval_cstring),
											  ObjectIdGetDatum(InvalidOid),
											  Int32GetDatum(interval_typmod));
		if (interval_type)
			*interval_type = INTERVALOID;
	}
	/* Otherwise cast it to the partitioned column's type */
	else
	{
		HeapTuple	htup;
		Oid			typein_proc = InvalidOid;

		htup = SearchSysCache1(TYPEOID, ObjectIdGetDatum(part_atttype));
		if (HeapTupleIsValid(htup))
		{
			typein_proc = ((Form_pg_type) GETSTRUCT(htup))->typinput;
			ReleaseSysCache(htup);
		}
		else
			elog(ERROR, "Cannot find input function for type %u", part_atttype);

		/*
		 * Convert interval from CSTRING to 'prel->atttype'.
		 *
		 * Note: We pass 3 arguments in case
		 * 'typein_proc' also takes Oid & typmod.
		 */
		interval_binary = OidFunctionCall3(typein_proc,
										   CStringGetDatum(interval_cstring),
										   ObjectIdGetDatum(part_atttype),
										   Int32GetDatum(-1));
		if (interval_type)
			*interval_type = part_atttype;
	}

	return interval_binary;
}

/*
 * Append partitions (if needed) and return Oid of the partition to contain value.
 *
 * NB: This function should not be called directly, use create_partitions() instead.
 */
Oid
create_partitions_internal(Oid relid, Datum value, Oid value_type)
{
	MemoryContext	old_mcxt = CurrentMemoryContext;
	Oid				partid = InvalidOid; /* last created partition (or InvalidOid) */

	PG_TRY();
	{
		const PartRelationInfo *prel;
		Datum					values[Natts_pathman_config];
		bool					isnull[Natts_pathman_config];

		/* Get both PartRelationInfo & PATHMAN_CONFIG contents for this relation */
		if (pathman_config_contains_relation(relid, values, isnull, NULL))
		{
			Oid			base_atttype;		/* base type of prel->atttype */
			Oid			base_value_type;	/* base type of value_type */

			Datum		min_rvalue,			/* absolute MIN */
						max_rvalue;			/* absolute MAX */

			Oid			interval_type = InvalidOid;
			Datum		interval_binary, /* assigned 'width' of a single partition */
						interval_text;

			FmgrInfo	interval_type_cmp;

			/* Fetch PartRelationInfo by 'relid' */
			prel = get_pathman_relation_info(relid);
			shout_if_prel_is_invalid(relid, prel, PT_RANGE);

			/* Fetch base types of prel->atttype & value_type */
			base_atttype = getBaseType(prel->atttype);
			base_value_type = getBaseType(value_type);

			/* Read max & min range values from PartRelationInfo */
			min_rvalue = PrelGetRangesArray(prel)[0].min;
			max_rvalue = PrelGetRangesArray(prel)[PrelLastChild(prel)].max;

			/* Retrieve interval as TEXT from tuple */
			interval_text = values[Anum_pathman_config_range_interval - 1];

			/* Convert interval to binary representation */
			interval_binary = extract_binary_interval_from_text(interval_text,
																base_atttype,
																&interval_type);

			/* Fill the FmgrInfo struct with a cmp(value, part_attribute) function */
			fill_type_cmp_fmgr_info(&interval_type_cmp, base_value_type, base_atttype);

			if (SPI_connect() != SPI_OK_CONNECT)
				elog(ERROR, "could not connect using SPI");

			/* while (value >= MAX) ... */
			spawn_partitions(PrelParentRelid(prel), value, max_rvalue,
							 base_atttype, &interval_type_cmp, interval_binary,
							 interval_type, true, &partid);

			/* while (value < MIN) ... */
			if (partid == InvalidOid)
				spawn_partitions(PrelParentRelid(prel), value, min_rvalue,
								 base_atttype, &interval_type_cmp, interval_binary,
								 interval_type, false, &partid);

			SPI_finish(); /* close SPI connection */
		}
		else
			elog(ERROR, "pg_pathman's config does not contain relation \"%s\"",
				 get_rel_name_or_relid(relid));
	}
	PG_CATCH();
	{
		ErrorData *edata;

		/* Switch to the original context & copy edata */
		MemoryContextSwitchTo(old_mcxt);
		edata = CopyErrorData();
		FlushErrorState();

		elog(LOG, "create_partitions_internal(): %s [%u]",
			 edata->message, MyProcPid);

		FreeErrorData(edata);

		SPI_finish(); /* no problem if not connected */

		/* Reset 'partid' in case of error */
		partid = InvalidOid;
	}
	PG_END_TRY();

	return partid;
}

/*
 * Create RANGE partitions (if needed) using either BGW or current backend.
 *
 * Returns Oid of the partition to store 'value'.
 */
Oid
create_partitions(Oid relid, Datum value, Oid value_type)
{
	TransactionId	rel_xmin;
	Oid				last_partition = InvalidOid;

	/* Check that table is partitioned and fetch xmin */
	if (pathman_config_contains_relation(relid, NULL, NULL, &rel_xmin))
	{
		bool part_in_prev_xact =
					TransactionIdPrecedes(rel_xmin, GetCurrentTransactionId()) ||
					TransactionIdEquals(rel_xmin, FrozenTransactionId);

		/*
		 * If table has been partitioned in some previous xact AND
		 * we don't hold any conflicting locks, run BGWorker.
		 */
		if (part_in_prev_xact && !xact_bgw_conflicting_lock_exists(relid))
		{
			elog(DEBUG2, "create_partitions(): chose BGWorker [%u]", MyProcPid);
			last_partition = create_partitions_bg_worker(relid, value, value_type);
		}
		/* Else it'd be better for the current backend to create partitions */
		else
		{
			elog(DEBUG2, "create_partitions(): chose backend [%u]", MyProcPid);
			last_partition = create_partitions_internal(relid, value, value_type);
		}
	}
	else
		elog(ERROR, "relation \"%s\" is not partitioned by pg_pathman",
			 get_rel_name_or_relid(relid));

	/* Check that 'last_partition' is valid */
	if (last_partition == InvalidOid)
		elog(ERROR, "could not create new partitions for relation \"%s\"",
			 get_rel_name_or_relid(relid));

	return last_partition;
}

/*
 * Given RangeEntry array and 'value', return selected
 * RANGE partitions inside the WrapperNode.
 */
void
select_range_partitions(const Datum value,
						FmgrInfo *cmp_func,
						const RangeEntry *ranges,
						const int nranges,
						const int strategy,
						WrapperNode *result)
{
	const RangeEntry   *current_re;
	bool				lossy = false,
						is_less,
						is_greater;

#ifdef USE_ASSERT_CHECKING
	bool				found = false;
	int					counter = 0;
#endif

	int					i,
						startidx = 0,
						endidx = nranges - 1,
						cmp_min,
						cmp_max;

	/* Initial value (no missing partitions found) */
	result->found_gap = false;

	/* Check boundaries */
	if (nranges == 0)
	{
		result->rangeset = NIL;
		return;
	}
	else
	{
		Assert(ranges);
		Assert(cmp_func);

		/* Corner cases */
		cmp_min = FunctionCall2(cmp_func, value, ranges[startidx].min),
		cmp_max = FunctionCall2(cmp_func, value, ranges[endidx].max);

		if ((cmp_min <= 0 && strategy == BTLessStrategyNumber) ||
			(cmp_min < 0 && (strategy == BTLessEqualStrategyNumber ||
							 strategy == BTEqualStrategyNumber)))
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
		Assert(cmp_func);

		i = startidx + (endidx - startidx) / 2;
		Assert(i >= 0 && i < nranges);

		current_re = &ranges[i];

		cmp_min = FunctionCall2(cmp_func, value, current_re->min);
		cmp_max = FunctionCall2(cmp_func, value, current_re->max);

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

		/* If we still haven't found partition then it doesn't exist */
		if (startidx >= endidx)
		{
			result->rangeset = NIL;
			result->found_gap = true;
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
					result->rangeset = lcons_irange(make_irange(0, i - 1, false),
													result->rangeset);
			}
			else
			{
				result->rangeset = list_make1_irange(make_irange(0, i, false));
			}
			break;

		case BTEqualStrategyNumber:
			result->rangeset = list_make1_irange(make_irange(i, i, true));
			break;

		case BTGreaterEqualStrategyNumber:
		case BTGreaterStrategyNumber:
			if (lossy)
			{
				result->rangeset = list_make1_irange(make_irange(i, i, true));
				if (i < nranges - 1)
					result->rangeset =
							lappend_irange(result->rangeset,
										   make_irange(i + 1,
													   nranges - 1,
													   false));
			}
			else
			{
				result->rangeset =
						list_make1_irange(make_irange(i,
													  nranges - 1,
													  false));
			}
			break;

		default:
			elog(ERROR, "Unknown btree strategy (%u)", strategy);
			break;
	}
}

/*
 * This function determines which partitions should appear in query plan.
 */
static void
handle_binary_opexpr(WalkerContext *context, WrapperNode *result,
					 const Node *varnode, const Const *c)
{
	int						strategy;
	TypeCacheEntry		   *tce;
	FmgrInfo				cmp_func;
	Oid						vartype;
	const OpExpr		   *expr = (const OpExpr *) result->orig;
	const PartRelationInfo *prel = context->prel;

	Assert(IsA(varnode, Var) || IsA(varnode, RelabelType));

	vartype = !IsA(varnode, RelabelType) ?
			((Var *) varnode)->vartype :
			((RelabelType *) varnode)->resulttype;

	tce = lookup_type_cache(vartype, TYPECACHE_BTREE_OPFAMILY);
	strategy = get_op_opfamily_strategy(expr->opno, tce->btree_opf);
	fill_type_cmp_fmgr_info(&cmp_func,
							getBaseType(c->consttype),
							getBaseType(prel->atttype));

	switch (prel->parttype)
	{
		case PT_HASH:
			if (strategy == BTEqualStrategyNumber)
			{
				Datum	value = OidFunctionCall1(prel->hash_proc, c->constvalue);
				uint32	idx = hash_to_part_index(DatumGetInt32(value),
												 PrelChildrenCount(prel));

				result->rangeset = list_make1_irange(make_irange(idx, idx, true));

				return; /* exit on equal */
			}
			break; /* continue to function's end */

		case PT_RANGE:
			{
				select_range_partitions(c->constvalue,
										&cmp_func,
										PrelGetRangesArray(context->prel),
										PrelChildrenCount(context->prel),
										strategy,
										result);
				return;
			}

		default:
			elog(ERROR, "Unknown partitioning type %u", prel->parttype);
	}

	result->rangeset = list_make1_irange(make_irange(0, PrelLastChild(prel), true));
	result->paramsel = 1.0;
}

/*
 * Estimate selectivity of parametrized quals.
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

	result->rangeset = list_make1_irange(make_irange(0, PrelLastChild(prel), true));

	if (strategy == BTEqualStrategyNumber)
	{
		result->paramsel = 1.0 / (double) PrelChildrenCount(prel);
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
 * Convert hash value to the partition index.
 */
uint32
hash_to_part_index(uint32 value, uint32 partitions)
{
	return value % partitions;
}

search_rangerel_result
search_range_partition_eq(const Datum value,
						  FmgrInfo *cmp_func,
						  const PartRelationInfo *prel,
						  RangeEntry *out_re) /* returned RangeEntry */
{
	RangeEntry *ranges;
	int			nranges;
	WrapperNode	result;

	ranges = PrelGetRangesArray(prel);
	nranges = PrelChildrenCount(prel);

	select_range_partitions(value,
							cmp_func,
							ranges,
							nranges,
							BTEqualStrategyNumber,
							&result);

	if (result.found_gap)
	{
		return SEARCH_RANGEREL_GAP;
	}
	else if (result.rangeset == NIL)
	{
		return SEARCH_RANGEREL_OUT_OF_RANGE;
	}
	else
	{
		IndexRange irange = linitial_irange(result.rangeset);

		Assert(list_length(result.rangeset) == 1);
		Assert(irange.ir_lower == irange.ir_upper);
		Assert(irange.ir_valid);

		/* Write result to the 'out_rentry' if necessary */
		if (out_re)
			memcpy((void *) out_re,
				   (const void *) &ranges[irange.ir_lower],
				   sizeof(RangeEntry));

		return SEARCH_RANGEREL_FOUND;
	}
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

static WrapperNode *
handle_const(const Const *c, WalkerContext *context)
{
	const PartRelationInfo *prel = context->prel;
	WrapperNode			   *result = (WrapperNode *) palloc(sizeof(WrapperNode));

	result->orig = (const Node *) c;

	/*
	 * Had to add this check for queries like:
	 *   select * from test.hash_rel where txt = NULL;
	 */
	if (!context->for_insert)
	{
		result->rangeset = list_make1_irange(make_irange(0,
														 PrelLastChild(prel),
														 true));
		result->paramsel = 1.0;

		return result;
	}

	switch (prel->parttype)
	{
		case PT_HASH:
			{
				Datum	value = OidFunctionCall1(prel->hash_proc, c->constvalue);
				uint32	idx = hash_to_part_index(DatumGetInt32(value),
												 PrelChildrenCount(prel));
				result->rangeset = list_make1_irange(make_irange(idx, idx, true));
			}
			break;

		case PT_RANGE:
			{
				TypeCacheEntry *tce;

				tce = lookup_type_cache(c->consttype, TYPECACHE_CMP_PROC_FINFO);

				select_range_partitions(c->constvalue,
										&tce->cmp_proc_finfo,
										PrelGetRangesArray(context->prel),
										PrelChildrenCount(context->prel),
										BTEqualStrategyNumber,
										result);
			}
			break;

		default:
			elog(ERROR, "Unknown partitioning type %u", prel->parttype);
			break;
	}

	return result;
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

	result->rangeset = list_make1_irange(make_irange(0, PrelLastChild(prel), true));
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
pull_var_param(const WalkerContext *ctx,
			   const OpExpr *expr,
			   Node **var_ptr,
			   Node **param_ptr)
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

		if (v->varoattno == ctx->prel->attnum)
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

		if (v->varoattno == ctx->prel->attnum)
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
		result->rangeset = list_make1_irange(make_irange(0,
														 PrelLastChild(prel),
														 false));
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
				result->rangeset = irange_list_union(result->rangeset, arg->rangeset);
				break;
			case AND_EXPR:
				result->rangeset = irange_list_intersect(result->rangeset, arg->rangeset);
				result->paramsel *= arg->paramsel;
				break;
			default:
				result->rangeset = list_make1_irange(make_irange(0,
																 PrelLastChild(prel),
																 false));
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
		if (var->varoattno != prel->attnum)
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
			Datum		value;
			uint32		idx;

			/* Invoke base hash function for value type */
			value = OidFunctionCall1(prel->hash_proc, elem_values[i]);
			idx = hash_to_part_index(DatumGetInt32(value), PrelChildrenCount(prel));
			result->rangeset = irange_list_union(result->rangeset,
												 list_make1_irange(make_irange(idx,
																			   idx,
																			   true)));
		}

		/* Free resources */
		pfree(elem_values);
		pfree(elem_nulls);

		return result;
	}

	if (arraynode && IsA(arraynode, Param))
		result->paramsel = DEFAULT_INEQ_SEL;

handle_arrexpr_return:
	result->rangeset = list_make1_irange(make_irange(0, PrelLastChild(prel), true));
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
	check_index_predicates_compat(root, rel);

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
	Index		parentRTindex = rti;
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
		Index		childRTindex;
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
		add_path(rel,
				 (Path *) create_append_path_compat(rel, subpaths, NULL, 0));

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
					 create_append_path_compat(rel, subpaths, required_outer, 0));
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

			path = (Path *) create_append_path_compat(rel, startup_subpaths,
													  NULL, 0);
			path->pathkeys = pathkeys;
			add_path(rel, path);

			if (startup_neq_total)
			{
				path = (Path *) create_append_path_compat(rel, total_subpaths,
														  NULL, 0);
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

			path = (Path *) create_append_path_compat(rel,
								list_reverse(startup_subpaths), NULL, 0);
			path->pathkeys = pathkeys;
			add_path(rel, path);

			if (startup_neq_total)
			{
				path = (Path *) create_append_path_compat(rel,
								list_reverse(total_subpaths), NULL, 0);
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

/*
 * Get cached PATHMAN_CONFIG relation Oid.
 */
Oid
get_pathman_config_relid(void)
{
	return pathman_config_relid;
}

/*
 * Get cached PATHMAN_CONFIG_PARAMS relation Oid.
 */
Oid
get_pathman_config_params_relid(void)
{
	return pathman_config_params_relid;
}
