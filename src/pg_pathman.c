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

#include "init.h"
#include "hooks.h"
#include "pathman.h"
#include "partition_filter.h"
#include "planner_tree_modification.h"
#include "runtimeappend.h"
#include "runtime_merge_append.h"

#include "postgres.h"
#include "foreign/fdwapi.h"
#include "miscadmin.h"
#include "optimizer/clauses.h"
#include "optimizer/plancat.h"
#include "optimizer/prep.h"
#include "optimizer/restrictinfo.h"
#include "optimizer/cost.h"
#include "utils/datum.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/selfuncs.h"
#include "utils/typcache.h"


PG_MODULE_MAGIC;


PathmanState   *pmstate;
Oid				pathman_config_relid = InvalidOid;
Oid				pathman_config_params_relid = InvalidOid;


/* pg module functions */
void _PG_init(void);


/* Expression tree handlers */
static Node *wrapper_make_expression(WrapperNode *wrap, int index, bool *alwaysTrue);

static WrapperNode *handle_const(const Const *c, WalkerContext *context);

static void handle_binary_opexpr(WalkerContext *context,
								 WrapperNode *result,
								 const Node *varnode,
								 const Const *c);

static void handle_binary_opexpr_param(const PartRelationInfo *prel,
									   WrapperNode *result,
									   const Node *varnode);

static WrapperNode *handle_opexpr(const OpExpr *expr, WalkerContext *context);
static WrapperNode *handle_boolexpr(const BoolExpr *expr, WalkerContext *context);
static WrapperNode *handle_arrexpr(const ScalarArrayOpExpr *expr, WalkerContext *context);

static double estimate_paramsel_using_prel(const PartRelationInfo *prel,
										   int strategy);

static bool pull_var_param(const WalkerContext *ctx,
						   const OpExpr *expr,
						   Node **var_ptr,
						   Node **param_ptr);


/* Misc */
static void make_inh_translation_list(Relation oldrelation, Relation newrelation,
									  Index newvarno, List **translated_vars);


/* Copied from allpaths.h */
static void set_plain_rel_size(PlannerInfo *root,
							   RelOptInfo *rel,
							   RangeTblEntry *rte);

static void set_plain_rel_pathlist(PlannerInfo *root,
								   RelOptInfo *rel,
								   RangeTblEntry *rte);

static List *accumulate_append_subpath(List *subpaths, Path *path);

static void generate_mergeappend_paths(PlannerInfo *root,
									   RelOptInfo *rel,
									   List *live_childrels,
									   List *all_child_pathkeys,
									   PathKey *pathkeyAsc,
									   PathKey *pathkeyDesc);

static Path *get_cheapest_parameterized_child_path(PlannerInfo *root,
												   RelOptInfo *rel,
												   Relids required_outer);


/* We can transform Param into Const provided that 'econtext' is available */
#define IsConstValue(wcxt, node) \
	( IsA((node), Const) || (WcxtHasExprContext(wcxt) ? IsA((node), Param) : false) )

#define ExtractConst(wcxt, node) \
	( \
		IsA((node), Param) ? \
				extract_const((wcxt), (Param *) (node)) : \
				((Const *) (node)) \
	)


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
	temp_init_state.pg_pathman_enable = true;
	temp_init_state.auto_partition = true;
	temp_init_state.override_copy = true;
	temp_init_state.initialization_needed = true;

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
 * make_inh_translation_list
 *	  Build the list of translations from parent Vars to child Vars for
 *	  an inheritance child.
 *
 * For paranoia's sake, we match type/collation as well as attribute name.
 *
 * NOTE: borrowed from prepunion.c
 */
static void
make_inh_translation_list(Relation oldrelation, Relation newrelation,
						  Index newvarno, List **translated_vars)
{
	List	   *vars = NIL;
	TupleDesc	old_tupdesc = RelationGetDescr(oldrelation);
	TupleDesc	new_tupdesc = RelationGetDescr(newrelation);
	int			oldnatts = old_tupdesc->natts;
	int			newnatts = new_tupdesc->natts;
	int			old_attno;

	for (old_attno = 0; old_attno < oldnatts; old_attno++)
	{
		Form_pg_attribute att;
		char	   *attname;
		Oid			atttypid;
		int32		atttypmod;
		Oid			attcollation;
		int			new_attno;

		att = old_tupdesc->attrs[old_attno];
		if (att->attisdropped)
		{
			/* Just put NULL into this list entry */
			vars = lappend(vars, NULL);
			continue;
		}
		attname = NameStr(att->attname);
		atttypid = att->atttypid;
		atttypmod = att->atttypmod;
		attcollation = att->attcollation;

		/*
		 * When we are generating the "translation list" for the parent table
		 * of an inheritance set, no need to search for matches.
		 */
		if (oldrelation == newrelation)
		{
			vars = lappend(vars, makeVar(newvarno,
										 (AttrNumber) (old_attno + 1),
										 atttypid,
										 atttypmod,
										 attcollation,
										 0));
			continue;
		}

		/*
		 * Otherwise we have to search for the matching column by name.
		 * There's no guarantee it'll have the same column position, because
		 * of cases like ALTER TABLE ADD COLUMN and multiple inheritance.
		 * However, in simple cases it will be the same column number, so try
		 * that before we go groveling through all the columns.
		 *
		 * Note: the test for (att = ...) != NULL cannot fail, it's just a
		 * notational device to include the assignment into the if-clause.
		 */
		if (old_attno < newnatts &&
			(att = new_tupdesc->attrs[old_attno]) != NULL &&
			!att->attisdropped && att->attinhcount != 0 &&
			strcmp(attname, NameStr(att->attname)) == 0)
			new_attno = old_attno;
		else
		{
			for (new_attno = 0; new_attno < newnatts; new_attno++)
			{
				att = new_tupdesc->attrs[new_attno];

				/*
				 * Make clang analyzer happy:
				 *
				 * Access to field 'attisdropped' results
				 * in a dereference of a null pointer
				 */
				if (!att)
					elog(ERROR, "error in function "
								CppAsString(make_inh_translation_list));

				if (!att->attisdropped && att->attinhcount != 0 &&
					strcmp(attname, NameStr(att->attname)) == 0)
					break;
			}
			if (new_attno >= newnatts)
				elog(ERROR, "could not find inherited attribute \"%s\" of relation \"%s\"",
					 attname, RelationGetRelationName(newrelation));
		}

		/* Found it, check type and collation match */
		if (atttypid != att->atttypid || atttypmod != att->atttypmod)
			elog(ERROR, "attribute \"%s\" of relation \"%s\" does not match parent's type",
				 attname, RelationGetRelationName(newrelation));
		if (attcollation != att->attcollation)
			elog(ERROR, "attribute \"%s\" of relation \"%s\" does not match parent's collation",
				 attname, RelationGetRelationName(newrelation));

		vars = lappend(vars, makeVar(newvarno,
									 (AttrNumber) (new_attno + 1),
									 atttypid,
									 atttypmod,
									 attcollation,
									 0));
	}

	*translated_vars = vars;
}

/*
 * Creates child relation and adds it to root.
 * Returns child index in simple_rel_array.
 *
 * NOTE: This code is partially based on the expand_inherited_rtentry() function.
 */
int
append_child_relation(PlannerInfo *root, Relation parent_relation,
					  Index parent_rti, int ir_index, Oid child_oid,
					  List *wrappers)
{
	RangeTblEntry  *parent_rte,
				   *child_rte;
	RelOptInfo	   *parent_rel,
				   *child_rel;
	Relation		child_relation;
	AppendRelInfo  *appinfo;
	Index			childRTindex;
	PlanRowMark	   *parent_rowmark,
				   *child_rowmark;
	Node		   *childqual;
	List		   *childquals;
	ListCell	   *lc,
				   *lc2;

	parent_rel = root->simple_rel_array[parent_rti];
	parent_rte = root->simple_rte_array[parent_rti];

	/* FIXME: acquire a suitable lock on partition */
	child_relation = heap_open(child_oid, NoLock);

	/* Create RangeTblEntry for child relation */
	child_rte = copyObject(parent_rte);
	child_rte->relid = child_oid;
	child_rte->relkind = child_relation->rd_rel->relkind;
	child_rte->inh = false;
	child_rte->requiredPerms = 0;

	/* Add 'child_rte' to rtable and 'root->simple_rte_array' */
	root->parse->rtable = lappend(root->parse->rtable, child_rte);
	childRTindex = list_length(root->parse->rtable);
	root->simple_rte_array[childRTindex] = child_rte;

	/* Create RelOptInfo for this child (and make some estimates as well) */
	child_rel = build_simple_rel(root, childRTindex, RELOPT_OTHER_MEMBER_REL);

	/* Increase total_table_pages using the 'child_rel' */
	root->total_table_pages += (double) child_rel->pages;


	/* Build an AppendRelInfo for this child */
	appinfo = makeNode(AppendRelInfo);
	appinfo->parent_relid = parent_rti;
	appinfo->child_relid = childRTindex;
	appinfo->parent_reloid = parent_rte->relid;

	make_inh_translation_list(parent_relation, child_relation, childRTindex,
							  &appinfo->translated_vars);

	/* Now append 'appinfo' to 'root->append_rel_list' */
	root->append_rel_list = lappend(root->append_rel_list, appinfo);

	/* Adjust target list for this child */
	adjust_targetlist_compat(root, child_rel, parent_rel, appinfo);

	/*
	 * Copy restrictions. If it's not the parent table, copy only
	 * those restrictions that are related to this partition.
	 */
	if (parent_rte->relid != child_oid)
	{
		childquals = NIL;

		forboth(lc, wrappers, lc2, parent_rel->baserestrictinfo)
		{
			WrapperNode	   *wrap = (WrapperNode *) lfirst(lc);
			Node		   *new_clause;
			bool			always_true;

			/* Generate a set of clauses for this child using WrapperNode */
			new_clause = wrapper_make_expression(wrap, ir_index, &always_true);

			/* Don't add this clause if it's always true */
			if (always_true)
				continue;

			/* Clause should not be NULL */
			Assert(new_clause);
			childquals = lappend(childquals, new_clause);
		}
	}
	/* If it's the parent table, copy all restrictions */
	else childquals = get_all_actual_clauses(parent_rel->baserestrictinfo);

	/* Now it's time to change varnos and rebuld quals */
	childquals = (List *) adjust_appendrel_attrs(root,
												 (Node *) childquals,
												 appinfo);
	childqual = eval_const_expressions(root, (Node *)
									   make_ands_explicit(childquals));
	if (childqual && IsA(childqual, Const) &&
		(((Const *) childqual)->constisnull ||
		 !DatumGetBool(((Const *) childqual)->constvalue)))
	{
		/*
		 * Restriction reduces to constant FALSE or constant NULL after
		 * substitution, so this child need not be scanned.
		 */
		set_dummy_rel_pathlist(child_rel);
	}
	childquals = make_ands_implicit((Expr *) childqual);
	childquals = make_restrictinfos_from_actual_clauses(root, childquals);

	/* Set new shiny childquals */
	child_rel->baserestrictinfo = childquals;

	if (relation_excluded_by_constraints(root, child_rel, child_rte))
	{
		/*
		 * This child need not be scanned, so we can omit it from the
		 * appendrel.
		 */
		set_dummy_rel_pathlist(child_rel);
	}

	/*
	 * We have to make child entries in the EquivalenceClass data
	 * structures as well.
	 */
	if (parent_rel->has_eclass_joins || has_useful_pathkeys(root, parent_rel))
		add_child_rel_equivalences(root, appinfo, parent_rel, child_rel);
	child_rel->has_eclass_joins = parent_rel->has_eclass_joins;

	/* Close child relations, but keep locks */
	heap_close(child_relation, NoLock);


	/* Create rowmarks required for child rels */
	parent_rowmark = get_plan_rowmark(root->rowMarks, parent_rti);
	if (parent_rowmark)
	{
		child_rowmark = makeNode(PlanRowMark);

		child_rowmark->rti = childRTindex;
		child_rowmark->prti = parent_rti;
		child_rowmark->rowmarkId = parent_rowmark->rowmarkId;
		/* Reselect rowmark type, because relkind might not match parent */
		child_rowmark->markType = select_rowmark_type(child_rte,
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
		cmp_min = IsInfinite(&ranges[startidx].min) ?
			1 : DatumGetInt32(FunctionCall2(cmp_func, value, BoundGetValue(&ranges[startidx].min)));
		cmp_max = IsInfinite(&ranges[endidx].max) ?
			-1 : DatumGetInt32(FunctionCall2(cmp_func, value, BoundGetValue(&ranges[endidx].max)));

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
			result->rangeset = list_make1_irange(make_irange(startidx,
															 endidx,
															 IR_COMPLETE));
			return;
		}

		if (cmp_max >= 0 && (strategy == BTLessEqualStrategyNumber ||
							 strategy == BTLessStrategyNumber))
		{
			result->rangeset = list_make1_irange(make_irange(startidx,
															 endidx,
															 IR_COMPLETE));
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

		cmp_min = IsInfinite(&current_re->min) ?
						1 :
						FunctionCall2(cmp_func, value,
									  BoundGetValue(&current_re->min));
		cmp_max = IsInfinite(&current_re->max) ?
						-1 :
						FunctionCall2(cmp_func, value,
									  BoundGetValue(&current_re->max));

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
				result->rangeset = list_make1_irange(make_irange(i, i, IR_LOSSY));
				if (i > 0)
					result->rangeset = lcons_irange(make_irange(0, i - 1, IR_COMPLETE),
													result->rangeset);
			}
			else
			{
				result->rangeset = list_make1_irange(make_irange(0, i, IR_COMPLETE));
			}
			break;

		case BTEqualStrategyNumber:
			result->rangeset = list_make1_irange(make_irange(i, i, IR_LOSSY));
			break;

		case BTGreaterEqualStrategyNumber:
		case BTGreaterStrategyNumber:
			if (lossy)
			{
				result->rangeset = list_make1_irange(make_irange(i, i, IR_LOSSY));
				if (i < nranges - 1)
					result->rangeset =
							lappend_irange(result->rangeset,
										   make_irange(i + 1,
													   nranges - 1,
													   IR_COMPLETE));
			}
			else
			{
				result->rangeset =
						list_make1_irange(make_irange(i,
													  nranges - 1,
													  IR_COMPLETE));
			}
			break;

		default:
			elog(ERROR, "Unknown btree strategy (%u)", strategy);
			break;
	}
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
			ListCell   *lc;
			List	   *args = NIL;

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
	WrapperNode *result;

	switch (nodeTag(expr))
	{
		/* Useful for INSERT optimization */
		case T_Const:
			return handle_const((Const *) expr, context);

		/* AND, OR, NOT expressions */
		case T_BoolExpr:
			return handle_boolexpr((BoolExpr *) expr, context);

		/* =, !=, <, > etc. */
		case T_OpExpr:
			return handle_opexpr((OpExpr *) expr, context);

		/* IN expression */
		case T_ScalarArrayOpExpr:
			return handle_arrexpr((ScalarArrayOpExpr *) expr, context);

		default:
			result = (WrapperNode *) palloc(sizeof(WrapperNode));
			result->orig = (const Node *) expr;
			result->args = NIL;
			result->paramsel = 1.0;

			result->rangeset = list_make1_irange(
						make_irange(0, PrelLastChild(context->prel), IR_LOSSY));

			return result;
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
	Oid						vartype;
	const OpExpr		   *expr = (const OpExpr *) result->orig;
	const PartRelationInfo *prel = context->prel;

	Assert(IsA(varnode, Var) || IsA(varnode, RelabelType));

	vartype = !IsA(varnode, RelabelType) ?
			((Var *) varnode)->vartype :
			((RelabelType *) varnode)->resulttype;

	/* Exit if Constant is NULL */
	if (c->constisnull)
	{
		result->rangeset = NIL;
		result->paramsel = 1.0;
		return;
	}

	tce = lookup_type_cache(vartype, TYPECACHE_BTREE_OPFAMILY);
	strategy = get_op_opfamily_strategy(expr->opno, tce->btree_opf);

	/* There's no strategy for this operator, go to end */
	if (strategy == 0)
		goto binary_opexpr_return;

	switch (prel->parttype)
	{
		case PT_HASH:
			/* If strategy is "=", select one partiton */
			if (strategy == BTEqualStrategyNumber)
			{
				Datum	value = OidFunctionCall1(prel->hash_proc, c->constvalue);
				uint32	idx = hash_to_part_index(DatumGetInt32(value),
												 PrelChildrenCount(prel));

				result->paramsel = estimate_paramsel_using_prel(prel, strategy);
				result->rangeset = list_make1_irange(make_irange(idx, idx, IR_LOSSY));

				return; /* exit on equal */
			}
			/* Else go to end */
			else goto binary_opexpr_return;

		case PT_RANGE:
			{
				FmgrInfo cmp_func;

				fill_type_cmp_fmgr_info(&cmp_func,
										getBaseType(c->consttype),
										getBaseType(prel->atttype));

				select_range_partitions(c->constvalue,
										&cmp_func,
										PrelGetRangesArray(context->prel),
										PrelChildrenCount(context->prel),
										strategy,
										result); /* output */

				result->paramsel = estimate_paramsel_using_prel(prel, strategy);

				return; /* done, now exit */
			}

		default:
			elog(ERROR, "Unknown partitioning type %u", prel->parttype);
	}

binary_opexpr_return:
	result->rangeset = list_make1_irange(make_irange(0, PrelLastChild(prel), IR_LOSSY));
	result->paramsel = 1.0;
}

/*
 * Estimate selectivity of parametrized quals.
 */
static void
handle_binary_opexpr_param(const PartRelationInfo *prel,
						   WrapperNode *result, const Node *varnode)
{
	const OpExpr	   *expr = (const OpExpr *) result->orig;
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

	result->rangeset = list_make1_irange(make_irange(0, PrelLastChild(prel), IR_LOSSY));
	result->paramsel = estimate_paramsel_using_prel(prel, strategy);
}

/*
 * Extracted common 'paramsel' estimator.
 */
static double
estimate_paramsel_using_prel(const PartRelationInfo *prel, int strategy)
{
	/* If it's "=", divide by partitions number */
	if (strategy == BTEqualStrategyNumber)
		return 1.0 / (double) PrelChildrenCount(prel);

	/* Default selectivity estimate for inequalities */
	else if (prel->parttype == PT_RANGE && strategy > 0)
		return DEFAULT_INEQ_SEL;

	/* Else there's not much to do */
	else return 1.0;
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
							&result); /* output */

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
		Assert(irange_lower(irange) == irange_upper(irange));
		Assert(is_irange_valid(irange));

		/* Write result to the 'out_rentry' if necessary */
		if (out_re)
			memcpy((void *) out_re,
				   (const void *) &ranges[irange_lower(irange)],
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
	WrapperNode			   *result = (WrapperNode *) palloc0(sizeof(WrapperNode));
	int						strategy = BTEqualStrategyNumber;

	result->orig = (const Node *) c;

	/*
	 * Had to add this check for queries like:
	 *		select * from test.hash_rel where txt = NULL;
	 */
	if (!context->for_insert || c->constisnull)
	{
		result->rangeset = NIL;
		result->paramsel = 1.0;

		return result;
	}

	switch (prel->parttype)
	{
		case PT_HASH:
			{
				Datum	value,	/* value to be hashed */
						hash;	/* 32-bit hash */
				uint32	idx;	/* index of partition */
				bool	cast_success;

				/* Peform type cast if types mismatch */
				if (prel->atttype != c->consttype)
				{
					value = perform_type_cast(c->constvalue,
											  getBaseType(c->consttype),
											  getBaseType(prel->atttype),
											  &cast_success);

					if (!cast_success)
						elog(ERROR, "Cannot select partition: "
									"unable to perform type cast");
				}
				/* Else use the Const's value */
				else value = c->constvalue;

				/* Calculate 32-bit hash of 'value' and corresponding index */
				hash = OidFunctionCall1(prel->hash_proc, value);
				idx = hash_to_part_index(DatumGetInt32(hash),
										 PrelChildrenCount(prel));

				result->paramsel = estimate_paramsel_using_prel(prel, strategy);
				result->rangeset = list_make1_irange(make_irange(idx, idx, IR_LOSSY));
			}
			break;

		case PT_RANGE:
			{
				FmgrInfo cmp_finfo;

				fill_type_cmp_fmgr_info(&cmp_finfo,
										getBaseType(c->consttype),
										getBaseType(prel->atttype));

				select_range_partitions(c->constvalue,
										&cmp_finfo,
										PrelGetRangesArray(context->prel),
										PrelChildrenCount(context->prel),
										strategy,
										result); /* output */

				result->paramsel = estimate_paramsel_using_prel(prel, strategy);
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
	WrapperNode	*result = (WrapperNode *) palloc0(sizeof(WrapperNode));
	Node		*var, *param;
	const PartRelationInfo *prel = context->prel;

	result->orig = (const Node *) expr;
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

	result->rangeset = list_make1_irange(make_irange(0, PrelLastChild(prel), IR_LOSSY));
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

		/* Check if 'v' is partitioned column of 'prel' */
		if (v->varoattno == ctx->prel->attnum &&
			v->varno == ctx->prel_varno)
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

		/* Check if 'v' is partitioned column of 'prel' */
		if (v->varoattno == ctx->prel->attnum &&
			v->varno == ctx->prel_varno)
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
	WrapperNode	*result = (WrapperNode *) palloc0(sizeof(WrapperNode));
	ListCell	*lc;
	const PartRelationInfo *prel = context->prel;

	result->orig = (const Node *)expr;
	result->args = NIL;
	result->paramsel = 1.0;

	if (expr->boolop == AND_EXPR)
		result->rangeset = list_make1_irange(make_irange(0,
														 PrelLastChild(prel),
														 IR_COMPLETE));
	else
		result->rangeset = NIL;

	foreach (lc, expr->args)
	{
		WrapperNode *arg;

		arg = walk_expr_tree((Expr *) lfirst(lc), context);
		result->args = lappend(result->args, arg);

		switch (expr->boolop)
		{
			case OR_EXPR:
				result->rangeset = irange_list_union(result->rangeset,
													 arg->rangeset);
				break;

			case AND_EXPR:
				result->rangeset = irange_list_intersection(result->rangeset,
															arg->rangeset);
				result->paramsel *= arg->paramsel;
				break;

			default:
				result->rangeset = list_make1_irange(make_irange(0,
																 PrelLastChild(prel),
																 IR_LOSSY));
				break;
		}
	}

	if (expr->boolop == OR_EXPR)
	{
		int totallen = irange_list_length(result->rangeset);

		foreach (lc, result->args)
		{
			WrapperNode	   *arg = (WrapperNode *) lfirst(lc);
			int				len = irange_list_length(arg->rangeset);

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
	WrapperNode *result = (WrapperNode *) palloc(sizeof(WrapperNode));
	Node		*varnode = (Node *) linitial(expr->args);
	Var			*var;
	Node		*arraynode = (Node *) lsecond(expr->args);
	const PartRelationInfo *prel = context->prel;

	result->orig = (const Node *) expr;
	result->args = NIL;
	result->paramsel = 0.0;

	Assert(varnode != NULL);

	/* If variable is not the partition key then skip it */
	if (IsA(varnode, Var) || IsA(varnode, RelabelType))
	{
		var = !IsA(varnode, RelabelType) ?
			(Var *) varnode :
			(Var *) ((RelabelType *) varnode)->arg;

		/* Skip if base types or attribute numbers do not match */
		if (getBaseType(var->vartype) != getBaseType(prel->atttype) ||
			var->varoattno != prel->attnum ||	/* partitioned attribute */
			var->varno != context->prel_varno)	/* partitioned table */
		{
			goto handle_arrexpr_return;
		}
	}
	else
		goto handle_arrexpr_return;

	if (arraynode && IsA(arraynode, Const) &&
		!((Const *) arraynode)->constisnull)
	{
		ArrayType  *arrayval;
		int16		elemlen;
		bool		elembyval;
		char		elemalign;
		int			num_elems;
		Datum	   *elem_values;
		bool	   *elem_nulls;
		int			strategy = BTEqualStrategyNumber;

		/* Extract values from array */
		arrayval = DatumGetArrayTypeP(((Const *) arraynode)->constvalue);
		get_typlenbyvalalign(ARR_ELEMTYPE(arrayval),
							 &elemlen, &elembyval, &elemalign);
		deconstruct_array(arrayval,
						  ARR_ELEMTYPE(arrayval),
						  elemlen, elembyval, elemalign,
						  &elem_values, &elem_nulls, &num_elems);

		result->rangeset = NIL;

		switch (prel->parttype)
		{
			case PT_HASH:
				{
					List   *ranges = NIL;
					int		i;

					/* Construct OIDs list */
					for (i = 0; i < num_elems; i++)
					{
						Datum		value;
						uint32		idx;
						List	   *irange;
						double		cur_paramsel;

						if (!elem_nulls[i])
						{
							/* Invoke base hash function for value type */
							value = OidFunctionCall1(prel->hash_proc, elem_values[i]);
							idx = hash_to_part_index(DatumGetUInt32(value),
													 PrelChildrenCount(prel));

							irange = list_make1_irange(make_irange(idx, idx, IR_LOSSY));
						}
						/* No children if Const is NULL */
						else irange = NIL;

						ranges = irange_list_union(ranges, irange);

						cur_paramsel = estimate_paramsel_using_prel(prel, strategy);
						result->paramsel = Max(result->paramsel, cur_paramsel);
					}

					result->rangeset = ranges;
				}
				break;

			case PT_RANGE:
				{
					WalkerContext  *nested_wcxt;
					List		   *ranges = NIL;
					int				i;

					nested_wcxt = palloc(sizeof(WalkerContext));
					memcpy((void *) nested_wcxt,
						   (const void *) context,
						   sizeof(WalkerContext));

					/* Overload variable to allow search by Const */
					nested_wcxt->for_insert = true;

					/* Construct OIDs list */
					for (i = 0; i < num_elems; i++)
					{
						WrapperNode    *wrap;
						Const		   *c = makeConst(ARR_ELEMTYPE(arrayval),
													  -1, InvalidOid,
													  datumGetSize(elem_values[i],
																   elembyval,
																   elemlen),
													  elem_values[i],
													  elem_nulls[i],
													  elembyval);

						wrap = walk_expr_tree((Expr *) c, nested_wcxt);
						ranges = irange_list_union(ranges, wrap->rangeset);

						pfree(c);

						result->paramsel = Max(result->paramsel, wrap->paramsel);
					}

					result->rangeset = ranges;
				}
				break;

			default:
				elog(ERROR, "Unknown partitioning type %u", prel->parttype);
		}

		/* Free resources */
		pfree(elem_values);
		pfree(elem_nulls);

		return result;
	}

	if (arraynode && IsA(arraynode, Param))
		result->paramsel = DEFAULT_INEQ_SEL;

handle_arrexpr_return:
	result->rangeset = list_make1_irange(make_irange(0, PrelLastChild(prel), IR_LOSSY));
	result->paramsel = 1.0;
	return result;
}

/*
 * These functions below are copied from allpaths.c with (or without) some
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
	Relids	required_outer;
	Path   *path;

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

#if PG_VERSION_NUM >= 90600
	/* If appropriate, consider parallel sequential scan */
	if (rel->consider_parallel && required_outer == NULL)
		create_plain_partial_paths_compat(root, rel);
#endif

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
set_append_rel_pathlist(PlannerInfo *root, RelOptInfo *rel, Index rti,
						PathKey *pathkeyAsc, PathKey *pathkeyDesc)
{
	Index		parentRTindex = rti;
	List	   *live_childrels = NIL;
	List	   *subpaths = NIL;
	bool		subpaths_valid = true;
#if PG_VERSION_NUM >= 90600
	List	   *partial_subpaths = NIL;
	bool		partial_subpaths_valid = true;
#endif
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
		AppendRelInfo  *appinfo = (AppendRelInfo *) lfirst(l);
		Index			childRTindex;
		RangeTblEntry  *childRTE;
		RelOptInfo	   *childrel;
		ListCell	   *lcp;

		/* append_rel_list contains all append rels; ignore others */
		if (appinfo->parent_relid != parentRTindex)
			continue;

		/* Re-locate the child RTE and RelOptInfo */
		childRTindex = appinfo->child_relid;
		childRTE = root->simple_rte_array[childRTindex];
		childrel = root->simple_rel_array[childRTindex];

#if PG_VERSION_NUM >= 90600
		/*
		 * If parallelism is allowable for this query in general and for parent
		 * appendrel, see whether it's allowable for this childrel in
		 * particular.
		 *
		 * For consistency, do this before calling set_rel_size() for the child.
		 */
		if (root->glob->parallelModeOK && rel->consider_parallel)
			set_rel_consider_parallel_compat(root, childrel, childRTE);
#endif

		/* Compute child's access paths & sizes */
		if (childRTE->relkind == RELKIND_FOREIGN_TABLE)
		{
			/* childrel->rows should be >= 1 */
			set_foreign_size(root, childrel, childRTE);

			/* If child IS dummy, ignore it */
			if (IS_DUMMY_REL(childrel))
				continue;

			set_foreign_pathlist(root, childrel, childRTE);
		}
		else
		{
			/* childrel->rows should be >= 1 */
			set_plain_rel_size(root, childrel, childRTE);

			/* If child IS dummy, ignore it */
			if (IS_DUMMY_REL(childrel))
				continue;

			set_plain_rel_pathlist(root, childrel, childRTE);
		}

		/* Set cheapest path for child */
		set_cheapest(childrel);

		/* If child BECAME dummy, ignore it */
		if (IS_DUMMY_REL(childrel))
			continue;

		/*
		 * Child is live, so add it to the live_childrels list for use below.
		 */
		live_childrels = lappend(live_childrels, childrel);

#if PG_VERSION_NUM >= 90600
		/*
		 * If any live child is not parallel-safe, treat the whole appendrel
		 * as not parallel-safe.  In future we might be able to generate plans
		 * in which some children are farmed out to workers while others are
		 * not; but we don't have that today, so it's a waste to consider
		 * partial paths anywhere in the appendrel unless it's all safe.
		 */
		if (!childrel->consider_parallel)
			rel->consider_parallel = false;
#endif

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

#if PG_VERSION_NUM >= 90600
		/* Same idea, but for a partial plan. */
		if (childrel->partial_pathlist != NIL)
			partial_subpaths = accumulate_append_subpath(partial_subpaths,
									   linitial(childrel->partial_pathlist));
		else
			partial_subpaths_valid = false;
#endif

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

#if PG_VERSION_NUM >= 90600
	/*
	 * Consider an append of partial unordered, unparameterized partial paths.
	 */
	if (partial_subpaths_valid)
	{
		AppendPath *appendpath;
		ListCell   *lc;
		int			parallel_workers = 0;

		/*
		 * Decide on the number of workers to request for this append path.
		 * For now, we just use the maximum value from among the members.  It
		 * might be useful to use a higher number if the Append node were
		 * smart enough to spread out the workers, but it currently isn't.
		 */
		foreach(lc, partial_subpaths)
		{
			Path	   *path = lfirst(lc);

			parallel_workers = Max(parallel_workers, path->parallel_workers);
		}

		if (parallel_workers > 0)
		{

			/* Generate a partial append path. */
			appendpath = create_append_path_compat(rel, partial_subpaths, NULL,
					parallel_workers);
			add_partial_path(rel, (Path *) appendpath);
		}
	}
#endif

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
get_pathman_config_relid(bool invalid_is_ok)
{
	/* Raise ERROR if Oid is invalid */
	if (!OidIsValid(pathman_config_relid) && !invalid_is_ok)
		elog(ERROR,
			 (!IsPathmanInitialized() ?
				"pg_pathman is not initialized yet" :
				"unexpected error in function "
						  CppAsString(get_pathman_config_relid)));

	return pathman_config_relid;
}

/*
 * Get cached PATHMAN_CONFIG_PARAMS relation Oid.
 */
Oid
get_pathman_config_params_relid(bool invalid_is_ok)
{
	/* Raise ERROR if Oid is invalid */
	if (!OidIsValid(pathman_config_relid) && !invalid_is_ok)
		elog(ERROR,
			 (!IsPathmanInitialized() ?
				"pg_pathman is not initialized yet" :
				"unexpected error in function "
						  CppAsString(get_pathman_config_params_relid)));

	return pathman_config_params_relid;
}
