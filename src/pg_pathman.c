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

#include "compat/pg_compat.h"
#include "compat/rowmarks_fix.h"

#include "init.h"
#include "hooks.h"
#include "pathman.h"
#include "partition_filter.h"
#include "runtimeappend.h"
#include "runtime_merge_append.h"

#include "postgres.h"
#include "access/sysattr.h"
#include "catalog/pg_type.h"
#include "foreign/fdwapi.h"
#include "miscadmin.h"
#include "optimizer/clauses.h"
#include "optimizer/plancat.h"
#include "optimizer/prep.h"
#include "optimizer/restrictinfo.h"
#include "optimizer/cost.h"
#include "utils/datum.h"
#include "utils/rel.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"
#include "utils/selfuncs.h"
#include "utils/typcache.h"


PG_MODULE_MAGIC;


Oid		pathman_config_relid		= InvalidOid,
		pathman_config_params_relid	= InvalidOid;


/* pg module functions */
void _PG_init(void);


/* Expression tree handlers */
static Node *wrapper_make_expression(WrapperNode *wrap, int index, bool *alwaysTrue);

static void handle_const(const Const *c,
						 const Oid collid,
						 const int strategy,
						 const WalkerContext *context,
						 WrapperNode *result);

static void handle_array(ArrayType *array,
						 const Oid collid,
						 const int strategy,
						 const bool use_or,
						 const WalkerContext *context,
						 WrapperNode *result);

static void handle_boolexpr(const BoolExpr *expr,
							const WalkerContext *context,
							WrapperNode *result);

static void handle_arrexpr(const ScalarArrayOpExpr *expr,
						   const WalkerContext *context,
						   WrapperNode *result);

static void handle_opexpr(const OpExpr *expr,
						  const WalkerContext *context,
						  WrapperNode *result);

static Datum array_find_min_max(Datum *values,
								bool *isnull,
								int length,
								Oid value_type,
								Oid collid,
								bool take_min,
								bool *result_null);


/* Copied from PostgreSQL (allpaths.c) */
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


/* Can we transform this node into a Const? */
static bool
IsConstValue(Node *node, const WalkerContext *context)
{
	switch (nodeTag(node))
	{
		case T_Const:
			return true;

		case T_Param:
			return WcxtHasExprContext(context);

		case T_RowExpr:
			{
				RowExpr	   *row = (RowExpr *) node;
				ListCell   *lc;

				/* Can't do anything about RECORD of wrong type */
				if (row->row_typeid != context->prel->ev_type)
					return false;

				/* Check that args are const values */
				foreach (lc, row->args)
					if (!IsConstValue((Node *) lfirst(lc), context))
						return false;
			}
			return true;

		default:
			return false;
	}
}

/* Extract a Const from node that has been checked by IsConstValue() */
static Const *
ExtractConst(Node *node, const WalkerContext *context)
{
	ExprState	   *estate;
	ExprContext	   *econtext = context->econtext;

	Datum			value;
	bool			isnull;

	Oid				typid,
					collid;
	int				typmod;

	/* Fast path for Consts */
	if (IsA(node, Const))
		return (Const *) node;

	/* Just a paranoid check */
	Assert(IsConstValue(node, context));

	switch (nodeTag(node))
	{
		case T_Param:
			{
				Param *param = (Param *) node;

				typid	= param->paramtype;
				typmod	= param->paramtypmod;
				collid	= param->paramcollid;

				/* It must be provided */
				Assert(WcxtHasExprContext(context));
			}
			break;

		case T_RowExpr:
			{
				RowExpr *row = (RowExpr *) node;

				typid	= row->row_typeid;
				typmod	= -1;
				collid	= InvalidOid;

#if PG_VERSION_NUM >= 100000
				/* If there's no context - create it! */
				if (!WcxtHasExprContext(context))
					econtext = CreateStandaloneExprContext();
#endif
			}
			break;

		default:
			elog(ERROR, "error in function " CppAsString(ExtractConst));
	}

	/* Evaluate expression */
	estate = ExecInitExpr((Expr *) node, NULL);
	value = ExecEvalExprCompat(estate, econtext, &isnull,
							   mult_result_handler);

#if PG_VERSION_NUM >= 100000
	/* Free temp econtext if needed */
	if (econtext && !WcxtHasExprContext(context))
		FreeExprContext(econtext, true);
#endif

	/* Finally return Const */
	return makeConst(typid, typmod, collid, get_typlen(typid),
					 value, isnull, get_typbyval(typid));
}

/*
 * Checks if expression is a KEY OP PARAM or PARAM OP KEY,
 * where KEY is partitioning expression and PARAM is whatever.
 *
 * Returns:
 *		operator's Oid if KEY is a partitioning expr,
 *		otherwise InvalidOid.
 */
static Oid
IsKeyOpParam(const OpExpr *expr,
			 const WalkerContext *context,
			 Node **param_ptr) /* ret value #1 */
{
	Node   *left = linitial(expr->args),
		   *right = lsecond(expr->args);

	/* Check number of arguments */
	if (list_length(expr->args) != 2)
		return InvalidOid;

	/* KEY OP PARAM */
	if (match_expr_to_operand(context->prel_expr, left))
	{
		*param_ptr = right;

		/* return the same operator */
		return expr->opno;
	}

	/* PARAM OP KEY */
	if (match_expr_to_operand(context->prel_expr, right))
	{
		*param_ptr = left;

		/* commute to (KEY OP PARAM) */
		return get_commutator(expr->opno);
	}

	return InvalidOid;
}

/* Selectivity estimator for common 'paramsel' */
static inline double
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
 * -------------------
 *  General functions
 * -------------------
 */

/* Set initial values for all Postmaster's forks */
void
_PG_init(void)
{
	PathmanInitState temp_init_state;

	if (!process_shared_preload_libraries_in_progress)
	{
		elog(ERROR, "pg_pathman module must be initialized by Postmaster. "
					"Put the following line to configuration file: "
					"shared_preload_libraries='pg_pathman'");
	}

	/* Request additional shared resources */
	RequestAddinShmemSpace(estimate_pathman_shmem_size());

	/* Assign pg_pathman's initial state */
	temp_init_state.pg_pathman_enable		= DEFAULT_PATHMAN_ENABLE;
	temp_init_state.auto_partition			= DEFAULT_PATHMAN_AUTO;
	temp_init_state.override_copy			= DEFAULT_PATHMAN_OVERRIDE_COPY;
	temp_init_state.initialization_needed	= true; /* ofc it's needed! */

	/* Apply initial state */
	restore_pathman_init_state(&temp_init_state);

	/* Set basic hooks */
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
	init_relation_info_static_data();
	init_runtimeappend_static_data();
	init_runtime_merge_append_static_data();
	init_partition_filter_static_data();
}

/* Get cached PATHMAN_CONFIG relation Oid */
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

/* Get cached PATHMAN_CONFIG_PARAMS relation Oid */
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



/*
 * ----------------------------------------
 *  RTE expansion (add RTE for partitions)
 * ----------------------------------------
 */

/*
 * Creates child relation and adds it to root.
 * Returns child index in simple_rel_array.
 *
 * NOTE: partially based on the expand_inherited_rtentry() function.
 */
Index
append_child_relation(PlannerInfo *root,
					  Relation parent_relation,
					  PlanRowMark *parent_rowmark,
					  Index parent_rti,
					  int ir_index,
					  Oid child_oid,
					  List *wrappers)
{
	RangeTblEntry  *parent_rte,
				   *child_rte;
	RelOptInfo	   *parent_rel,
				   *child_rel;
	Relation		child_relation;
	AppendRelInfo  *appinfo;
	Index			childRTindex;
	PlanRowMark	   *child_rowmark;
	Node		   *childqual;
	List		   *childquals;
	ListCell	   *lc1,
				   *lc2;
	LOCKMODE		lockmode;

	/* Choose a correct lock mode */
	if (parent_rti == root->parse->resultRelation)
		lockmode = RowExclusiveLock;
	else if (parent_rowmark && RowMarkRequiresRowShareLock(parent_rowmark->markType))
		lockmode = RowShareLock;
	else
		lockmode = AccessShareLock;

	/* Acquire a suitable lock on partition */
	LockRelationOid(child_oid, lockmode);

	/* Check that partition exists */
	if (!SearchSysCacheExists1(RELOID, ObjectIdGetDatum(child_oid)))
	{
		UnlockRelationOid(child_oid, lockmode);
		return 0;
	}

	parent_rel = root->simple_rel_array[parent_rti];
	parent_rte = root->simple_rte_array[parent_rti];

	/* Open child relation (we've just locked it) */
	child_relation = heap_open(child_oid, NoLock);

	/* Create RangeTblEntry for child relation */
	child_rte = copyObject(parent_rte);
	child_rte->relid			= child_oid;
	child_rte->relkind			= child_relation->rd_rel->relkind;
	child_rte->inh				= false;	/* relation has no children */
	child_rte->requiredPerms	= 0;		/* perform all checks on parent */

	/* Add 'child_rte' to rtable and 'root->simple_rte_array' */
	root->parse->rtable = lappend(root->parse->rtable, child_rte);
	childRTindex = list_length(root->parse->rtable);
	root->simple_rte_array[childRTindex] = child_rte;

	/* Create RelOptInfo for this child (and make some estimates as well) */
	child_rel = build_simple_rel_compat(root, childRTindex, parent_rel);

	/* Increase total_table_pages using the 'child_rel' */
	root->total_table_pages += (double) child_rel->pages;


	/* Create rowmarks required for child rels */
	if (parent_rowmark)
	{
		child_rowmark = makeNode(PlanRowMark);

		child_rowmark->rti			= childRTindex;
		child_rowmark->prti			= parent_rti;
		child_rowmark->rowmarkId	= parent_rowmark->rowmarkId;
		/* Reselect rowmark type, because relkind might not match parent */
		child_rowmark->markType		= select_rowmark_type(child_rte,
														  parent_rowmark->strength);
		child_rowmark->allMarkTypes	= (1 << child_rowmark->markType);
		child_rowmark->strength		= parent_rowmark->strength;
		child_rowmark->waitPolicy	= parent_rowmark->waitPolicy;
		child_rowmark->isParent		= false;

		root->rowMarks = lappend(root->rowMarks, child_rowmark);

		/* Adjust tlist for RowMarks (see planner.c) */
		if (!parent_rowmark->isParent && !root->parse->setOperations)
		{
			append_tle_for_rowmark(root, parent_rowmark);
		}

		/* Include child's rowmark type in parent's allMarkTypes */
		parent_rowmark->allMarkTypes |= child_rowmark->allMarkTypes;
		parent_rowmark->isParent = true;
	}


	/* Build an AppendRelInfo for this child */
	appinfo = makeNode(AppendRelInfo);
	appinfo->parent_relid	= parent_rti;
	appinfo->child_relid	= childRTindex;
	appinfo->parent_reloid	= parent_rte->relid;

	/* Store table row types for wholerow references */
	appinfo->parent_reltype = RelationGetDescr(parent_relation)->tdtypeid;
	appinfo->child_reltype  = RelationGetDescr(child_relation)->tdtypeid;

	make_inh_translation_list(parent_relation, child_relation, childRTindex,
							  &appinfo->translated_vars);

	/* Now append 'appinfo' to 'root->append_rel_list' */
	root->append_rel_list = lappend(root->append_rel_list, appinfo);

	/* Translate column privileges for this child */
	if (parent_rte->relid != child_oid)
	{
		child_rte->selectedCols = translate_col_privs(parent_rte->selectedCols,
													  appinfo->translated_vars);
		child_rte->insertedCols = translate_col_privs(parent_rte->insertedCols,
													  appinfo->translated_vars);
		child_rte->updatedCols = translate_col_privs(parent_rte->updatedCols,
													 appinfo->translated_vars);
	}

	/* Here and below we assume that parent RelOptInfo exists */
	AssertState(parent_rel);

	/* Adjust join quals for this child */
	child_rel->joininfo = (List *) adjust_appendrel_attrs(root,
														  (Node *) parent_rel->joininfo,
														  appinfo);

	/* Adjust target list for this child */
	adjust_rel_targetlist_compat(root, child_rel, parent_rel, appinfo);

	/*
	 * Copy restrictions. If it's not the parent table, copy only
	 * those restrictions that are related to this partition.
	 */
	if (parent_rte->relid != child_oid)
	{
		childquals = NIL;

		forboth (lc1, wrappers, lc2, parent_rel->baserestrictinfo)
		{
			WrapperNode	   *wrap = (WrapperNode *) lfirst(lc1);
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

	return childRTindex;
}



/*
 * -------------------------
 *  RANGE partition pruning
 * -------------------------
 */

/* Given 'value' and 'ranges', return selected partitions list */
void
select_range_partitions(const Datum value,
						const Oid collid,
						FmgrInfo *cmp_func,
						const RangeEntry *ranges,
						const int nranges,
						const int strategy,
						WrapperNode *result) /* returned partitions */
{
	bool	lossy = false,
			is_less,
			is_greater;

#ifdef USE_ASSERT_CHECKING
	bool	found = false;
	int		counter = 0;
#endif

	int		startidx = 0,
			endidx = nranges - 1,
			cmp_min,
			cmp_max,
			i;

	Bound	value_bound = MakeBound(value); /* convert value to Bound */


	/* Initial value (no missing partitions found) */
	result->found_gap = false;

	/* Check 'ranges' array */
	if (nranges == 0)
	{
		result->rangeset = NIL;
		return;
	}

	/* Check corner cases */
	else
	{
		Assert(ranges);
		Assert(cmp_func);

		/* Compare 'value' to absolute MIN and MAX bounds */
		cmp_min = cmp_bounds(cmp_func, collid, &value_bound, &ranges[startidx].min);
		cmp_max = cmp_bounds(cmp_func, collid, &value_bound, &ranges[endidx].max);

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
		Assert(ranges);
		Assert(cmp_func);

		/* Calculate new pivot */
		i = startidx + (endidx - startidx) / 2;
		Assert(i >= 0 && i < nranges);

		/* Compare 'value' to current MIN and MAX bounds */
		cmp_min = cmp_bounds(cmp_func, collid, &value_bound, &ranges[i].min);
		cmp_max = cmp_bounds(cmp_func, collid, &value_bound, &ranges[i].max);

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

		/* Indices have met, looks like there's no partition */
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

	/* Should've been found by now */
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



/*
 * ---------------------------------
 *  walk_expr_tree() implementation
 * ---------------------------------
 */

/* Examine expression in order to select partitions */
WrapperNode *
walk_expr_tree(Expr *expr, const WalkerContext *context)
{
	WrapperNode *result = (WrapperNode *) palloc0(sizeof(WrapperNode));

	switch (nodeTag(expr))
	{
		/* Useful for INSERT optimization */
		case T_Const:
			handle_const((Const *) expr, ((Const *) expr)->constcollid,
						 BTEqualStrategyNumber, context, result);
			return result;

		/* AND, OR, NOT expressions */
		case T_BoolExpr:
			handle_boolexpr((BoolExpr *) expr, context, result);
			return result;

		/* =, !=, <, > etc. */
		case T_OpExpr:
			handle_opexpr((OpExpr *) expr, context, result);
			return result;

		/* ANY, ALL, IN expressions */
		case T_ScalarArrayOpExpr:
			handle_arrexpr((ScalarArrayOpExpr *) expr, context, result);
			return result;

		default:
			result->orig = (const Node *) expr;
			result->args = NIL;

			result->rangeset = list_make1_irange_full(context->prel, IR_LOSSY);
			result->paramsel = 1.0;

			return result;
	}
}

/* Convert wrapper into expression for given index */
static Node *
wrapper_make_expression(WrapperNode *wrap, int index, bool *alwaysTrue)
{
	bool lossy, found;

	*alwaysTrue = false;

	/* TODO: possible optimization (we enumerate indexes sequntially). */
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
				 * We shouldn't get there for always true clause
				 * under OR and always false clause under AND.
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
			result->args = args;
			result->boolop = expr->boolop;
			result->location = expr->location;
			return (Node *) result;
		}
		else
			return (Node *) copyObject(wrap->orig);
	}
	else
		return (Node *) copyObject(wrap->orig);
}


/* Const handler */
static void
handle_const(const Const *c,
			 const Oid collid,
			 const int strategy,
			 const WalkerContext *context,
			 WrapperNode *result)		/* ret value #1 */
{
	const PartRelationInfo *prel = context->prel;

	/* Deal with missing strategy */
	if (strategy == 0)
		goto handle_const_return;

	/*
	 * Had to add this check for queries like:
	 *		select * from test.hash_rel where txt = NULL;
	 */
	if (c->constisnull)
	{
		result->rangeset = NIL;
		result->paramsel = 0.0;

		return; /* done, exit */
	}

	/*
	 * Had to add this check for queries like:
	 *		select * from test.hash_rel where true = false;
	 *		select * from test.hash_rel where false;
	 *		select * from test.hash_rel where $1;
	 */
	if (c->consttype == BOOLOID)
	{
		if (c->constvalue == BoolGetDatum(false))
		{
			result->rangeset = NIL;
			result->paramsel = 0.0;
		}
		else
		{
			result->rangeset = list_make1_irange_full(prel, IR_COMPLETE);
			result->paramsel = 1.0;
		}

		return; /* done, exit */
	}

	switch (prel->parttype)
	{
		case PT_HASH:
			{
				Datum	value,	/* value to be hashed */
						hash;	/* 32-bit hash */
				uint32	idx;	/* index of partition */
				bool	cast_success;

				/* Cannot do much about non-equal strategies */
				if (strategy != BTEqualStrategyNumber)
					goto handle_const_return;

				/* Peform type cast if types mismatch */
				if (prel->ev_type != c->consttype)
				{
					value = perform_type_cast(c->constvalue,
											  getBaseType(c->consttype),
											  getBaseType(prel->ev_type),
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

				result->rangeset = list_make1_irange(make_irange(idx, idx, IR_LOSSY));
				result->paramsel = 1.0;

				return; /* done, exit */
			}

		case PT_RANGE:
			{
				FmgrInfo cmp_finfo;

				/* Cannot do much about non-equal strategies + diff. collations */
				if (strategy != BTEqualStrategyNumber && collid != prel->ev_collid)
				{
					goto handle_const_return;
				}

				fill_type_cmp_fmgr_info(&cmp_finfo,
										getBaseType(c->consttype),
										getBaseType(prel->ev_type));

				select_range_partitions(c->constvalue,
										collid,
										&cmp_finfo,
										PrelGetRangesArray(context->prel),
										PrelChildrenCount(context->prel),
										strategy,
										result); /* result->rangeset = ... */
				result->paramsel = 1.0;

				return; /* done, exit */
			}

		default:
			WrongPartType(prel->parttype);
	}

handle_const_return:
	result->rangeset = list_make1_irange_full(prel, IR_LOSSY);
	result->paramsel = 1.0;
}

/* Array handler */
static void
handle_array(ArrayType *array,
			 const Oid collid,
			 const int strategy,
			 const bool use_or,
			 const WalkerContext *context,
			 WrapperNode *result)		/* ret value #1 */
{
	const PartRelationInfo *prel = context->prel;

	/* Elements of the array */
	Datum	   *elem_values;
	bool	   *elem_isnull;
	int			elem_count;

	/* Element's properties */
	Oid			elem_type;
	int16		elem_len;
	bool		elem_byval;
	char		elem_align;

	/* Check if we can work with this strategy */
	if (strategy == 0)
		goto handle_array_return;

	/* Get element's properties */
	elem_type = ARR_ELEMTYPE(array);
	get_typlenbyvalalign(elem_type, &elem_len, &elem_byval, &elem_align);

	/* Extract values from the array */
	deconstruct_array(array, elem_type, elem_len, elem_byval, elem_align,
					  &elem_values, &elem_isnull, &elem_count);

	/* Handle non-null Const arrays */
	if (elem_count > 0)
	{
		List   *ranges;
		int		i;

		/* This is only for paranoia's sake */
		Assert(BTMaxStrategyNumber == 5 && BTEqualStrategyNumber == 3);

		/* Optimizations for <, <=, >=, > */
		if (strategy != BTEqualStrategyNumber)
		{
			bool	take_min;
			Datum	pivot;
			bool	pivot_null;

			/*
			 * OR:		Max for (< | <=); Min for (> | >=)
			 * AND:		Min for (< | <=); Max for (> | >=)
			 */
			take_min = strategy < BTEqualStrategyNumber ? !use_or : use_or;

			/* Extract Min (or Max) element */
			pivot = array_find_min_max(elem_values, elem_isnull,
									   elem_count, elem_type, collid,
									   take_min, &pivot_null);

			/* Write data and "shrink" the array */
			elem_values[0]	= pivot_null ? (Datum) 0 : pivot;
			elem_isnull[0]	= pivot_null;
			elem_count		= 1;

			/* If pivot is not NULL ... */
			if (!pivot_null)
			{
				/* ... append single NULL if array contains NULLs */
				if (array_contains_nulls(array))
				{
					/* Make sure that we have enough space for 2 elements */
					Assert(ArrayGetNItems(ARR_NDIM(array), ARR_DIMS(array)) >= 2);

					elem_values[1]	= (Datum) 0;
					elem_isnull[1]	= true;
					elem_count		= 2;
				}
				/* ... optimize clause ('orig') if array does not contain NULLs */
				else if (result->orig)
				{
					/* Should've been provided by the caller */
					ScalarArrayOpExpr *orig = (ScalarArrayOpExpr *) result->orig;

					/* Rebuild clause using 'pivot' */
					result->orig = (Node *)
						   make_opclause(orig->opno, BOOLOID, false,
										 (Expr *) linitial(orig->args),
										 (Expr *) makeConst(elem_type,
															-1,
															collid,
															elem_len,
															elem_values[0],
															elem_isnull[0],
															elem_byval),
										 InvalidOid,
										 collid);
				}
			}
		}

		/* Set default rangeset */
		ranges = use_or ? NIL : list_make1_irange_full(prel, IR_COMPLETE);

		/* Select partitions using values */
		for (i = 0; i < elem_count; i++)
		{
			Const			c;
			WrapperNode		wrap = InvalidWrapperNode;

			NodeSetTag(&c, T_Const);
			c.consttype		= elem_type;
			c.consttypmod	= -1;
			c.constcollid	= InvalidOid;
			c.constlen		= datumGetSize(elem_values[i],
										   elem_byval,
										   elem_len);
			c.constvalue	= elem_values[i];
			c.constisnull	= elem_isnull[i];
			c.constbyval	= elem_byval;
			c.location		= -1;

			handle_const(&c, collid, strategy, context, &wrap);

			/* Should we use OR | AND? */
			ranges = use_or ?
						irange_list_union(ranges, wrap.rangeset) :
						irange_list_intersection(ranges, wrap.rangeset);
		}

		/* Free resources */
		pfree(elem_values);
		pfree(elem_isnull);

		result->rangeset = ranges;
		result->paramsel = 1.0;

		return; /* done, exit */
	}

handle_array_return:
	result->rangeset = list_make1_irange_full(prel, IR_LOSSY);
	result->paramsel = 1.0;
}

/* Boolean expression handler */
static void
handle_boolexpr(const BoolExpr *expr,
				const WalkerContext *context,
				WrapperNode *result)	/* ret value #1 */
{
	const PartRelationInfo *prel = context->prel;
	List				   *ranges,
						   *args = NIL;
	double					paramsel = 1.0;
	ListCell			   *lc;

	/* Set default rangeset */
	ranges = (expr->boolop == AND_EXPR) ?
					list_make1_irange_full(prel, IR_COMPLETE) :
					NIL;

	/* Examine expressions */
	foreach (lc, expr->args)
	{
		WrapperNode *wrap;

		wrap = walk_expr_tree((Expr *) lfirst(lc), context);
		args = lappend(args, wrap);

		switch (expr->boolop)
		{
			case OR_EXPR:
				ranges = irange_list_union(ranges, wrap->rangeset);
				break;

			case AND_EXPR:
				ranges = irange_list_intersection(ranges, wrap->rangeset);
				paramsel *= wrap->paramsel;
				break;

			default:
				ranges = list_make1_irange_full(prel, IR_LOSSY);
				break;
		}
	}

	/* Adjust paramsel for OR */
	if (expr->boolop == OR_EXPR)
	{
		int totallen = irange_list_length(ranges);

		foreach (lc, args)
		{
			WrapperNode	   *arg = (WrapperNode *) lfirst(lc);
			int				len = irange_list_length(arg->rangeset);

			paramsel *= (1.0 - arg->paramsel * (double)len / (double)totallen);
		}

		paramsel = 1.0 - paramsel;
	}

	/* Save results */
	result->rangeset	= ranges;
	result->paramsel	= paramsel;
	result->orig		= (const Node *) expr;
	result->args		= args;
}

/* Scalar array expression handler */
static void
handle_arrexpr(const ScalarArrayOpExpr *expr,
			   const WalkerContext *context,
			   WrapperNode *result)		/* ret value #1 */
{
	Node					   *part_expr = (Node *) linitial(expr->args);
	Node					   *array = (Node *) lsecond(expr->args);
	const PartRelationInfo	   *prel = context->prel;
	TypeCacheEntry			   *tce;
	int							strategy;

	/* Small sanity check */
	Assert(list_length(expr->args) == 2);

	tce = lookup_type_cache(prel->ev_type, TYPECACHE_BTREE_OPFAMILY);
	strategy = get_op_opfamily_strategy(expr->opno, tce->btree_opf);

	/* Save expression */
	result->orig = (const Node *) expr;

	/* Check if expression tree is a partitioning expression */
	if (!match_expr_to_operand(context->prel_expr, part_expr))
		goto handle_arrexpr_all;

	/* Check if we can work with this strategy */
	if (strategy == 0)
		goto handle_arrexpr_all;

	/* Examine the array node */
	switch (nodeTag(array))
	{
		case T_Const:
			{
				Const	   *c = (Const *) array;

				/* Array is NULL */
				if (c->constisnull)
				{
					result->rangeset = NIL;
					result->paramsel = 0.0;

					return; /* done, exit */
				}

				/* Examine array */
				handle_array(DatumGetArrayTypeP(c->constvalue),
							 expr->inputcollid, strategy,
							 expr->useOr, context, result);

				return; /* done, exit */
			}

		case T_ArrayExpr:
			{
				ArrayExpr  *arr_expr = (ArrayExpr *) array;
				Oid			elem_type = arr_expr->element_typeid;
				int			array_params = 0;
				double		paramsel = 1.0;
				List	   *ranges;
				ListCell   *lc;

				if (list_length(arr_expr->elements) == 0)
					goto handle_arrexpr_all;

				/* Set default ranges for OR | AND */
				ranges = expr->useOr ? NIL : list_make1_irange_full(prel, IR_COMPLETE);

				/* Walk trough elements list */
				foreach (lc, arr_expr->elements)
				{
					Node		   *elem = lfirst(lc);
					WrapperNode		wrap = InvalidWrapperNode;

					/* Stop if ALL + quals evaluate to NIL */
					if (!expr->useOr && ranges == NIL)
						break;

					/* Is this a const value? */
					if (IsConstValue(elem, context))
					{
						Const *c = ExtractConst(elem, context);

						/* Is this an array?. */
						if (c->consttype != elem_type && !c->constisnull)
						{
							handle_array(DatumGetArrayTypeP(c->constvalue),
										 expr->inputcollid, strategy,
										 expr->useOr, context, &wrap);
						}
						/* ... or a single element? */
						else
						{
							handle_const(c, expr->inputcollid,
										 strategy, context, &wrap);
						}

						/* Should we use OR | AND? */
						ranges = expr->useOr ?
									irange_list_union(ranges, wrap.rangeset) :
									irange_list_intersection(ranges, wrap.rangeset);
					}
					else array_params++; /* we've just met non-const nodes */
				}

				/* Check for PARAM-related optimizations */
				if (array_params > 0)
				{
					double	sel = estimate_paramsel_using_prel(prel, strategy);
					int		i;

					if (expr->useOr)
					{
						/* We can't say anything if PARAMs + ANY */
						ranges = list_make1_irange_full(prel, IR_LOSSY);

						/* See handle_boolexpr() */
						for (i = 0; i < array_params; i++)
							paramsel *= (1 - sel);

						paramsel = 1 - paramsel;
					}
					else
					{
						/* Recheck condition on a narrowed set of partitions */
						ranges = irange_list_set_lossiness(ranges, IR_LOSSY);

						/* See handle_boolexpr() */
						for (i = 0; i < array_params; i++)
							paramsel *= sel;
					}
				}

				/* Save result */
				result->rangeset = ranges;
				result->paramsel = paramsel;

				return; /* done, exit */
			}

		default:
			break;
	}

handle_arrexpr_all:
	result->rangeset = list_make1_irange_full(prel, IR_LOSSY);
	result->paramsel = 1.0;
}

/* Operator expression handler */
static void
handle_opexpr(const OpExpr *expr,
			  const WalkerContext *context,
			  WrapperNode *result)		/* ret value #1 */
{
	Node					   *param;
	const PartRelationInfo	   *prel = context->prel;
	Oid							opid; /* operator's Oid */

	/* Save expression */
	result->orig = (const Node *) expr;

	/* Is it KEY OP PARAM or PARAM OP KEY? */
	if (OidIsValid(opid = IsKeyOpParam(expr, context, &param)))
	{
		TypeCacheEntry *tce;
		int				strategy;

		tce = lookup_type_cache(prel->ev_type, TYPECACHE_BTREE_OPFAMILY);
		strategy = get_op_opfamily_strategy(opid, tce->btree_opf);

		if (IsConstValue(param, context))
		{
			handle_const(ExtractConst(param, context),
						 expr->inputcollid,
						 strategy, context, result);

			return; /* done, exit */
		}
		/* TODO: estimate selectivity for param if it's Var */
		else if (IsA(param, Param) || IsA(param, Var))
		{
			result->rangeset = list_make1_irange_full(prel, IR_LOSSY);
			result->paramsel = estimate_paramsel_using_prel(prel, strategy);

			return; /* done, exit */
		}
	}

	result->rangeset = list_make1_irange_full(prel, IR_LOSSY);
	result->paramsel = 1.0;
}


/* Find Max or Min value of array */
static Datum
array_find_min_max(Datum *values,
				   bool *isnull,
				   int length,
				   Oid value_type,
				   Oid collid,
				   bool take_min,
				   bool *result_null) /* ret value #2 */
{
	TypeCacheEntry *tce = lookup_type_cache(value_type, TYPECACHE_CMP_PROC_FINFO);
	Datum		   *pivot = NULL;
	int				i;

	for (i = 0; i < length; i++)
	{
		if (isnull[i])
			continue;

		/* Update 'pivot' */
		if (pivot == NULL || (take_min ?
								check_lt(&tce->cmp_proc_finfo,
										 collid, values[i], *pivot) :
								check_gt(&tce->cmp_proc_finfo,
										 collid, values[i], *pivot)))
		{
			pivot = &values[i];
		}
	}

	/* Return results */
	*result_null = (pivot == NULL);
	return (pivot == NULL) ? (Datum) 0 : *pivot;
}


/*
 * ----------------------------------------------------------------------------------
 *  NOTE: The following functions below are copied from PostgreSQL with (or without)
 *  some modifications. Couldn't use original because of 'static' modifier.
 * ----------------------------------------------------------------------------------
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


static List *
accumulate_append_subpath(List *subpaths, Path *path)
{
	return lappend(subpaths, path);
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
				get_cheapest_path_for_pathkeys_compat(childrel->pathlist,
													  pathkeys,
													  NULL,
													  STARTUP_COST,
													  false);
			cheapest_total =
				get_cheapest_path_for_pathkeys_compat(childrel->pathlist,
													  pathkeys,
													  NULL,
													  TOTAL_COST,
													  false);

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
			add_path(rel, (Path *) create_merge_append_path_compat(
							root, rel, startup_subpaths, pathkeys, NULL));
			if (startup_neq_total)
				add_path(rel, (Path *) create_merge_append_path_compat(
								root, rel, total_subpaths, pathkeys, NULL));
		}
	}
}


/*
 * translate_col_privs
 *	  Translate a bitmapset representing per-column privileges from the
 *	  parent rel's attribute numbering to the child's.
 *
 * The only surprise here is that we don't translate a parent whole-row
 * reference into a child whole-row reference.  That would mean requiring
 * permissions on all child columns, which is overly strict, since the
 * query is really only going to reference the inherited columns.  Instead
 * we set the per-column bits for all inherited columns.
 */
Bitmapset *
translate_col_privs(const Bitmapset *parent_privs,
					List *translated_vars)
{
	Bitmapset  *child_privs = NULL;
	bool		whole_row;
	int			attno;
	ListCell   *lc;

	/* System attributes have the same numbers in all tables */
	for (attno = FirstLowInvalidHeapAttributeNumber + 1; attno < 0; attno++)
	{
		if (bms_is_member(attno - FirstLowInvalidHeapAttributeNumber,
						  parent_privs))
			child_privs = bms_add_member(child_privs,
								 attno - FirstLowInvalidHeapAttributeNumber);
	}

	/* Check if parent has whole-row reference */
	whole_row = bms_is_member(InvalidAttrNumber - FirstLowInvalidHeapAttributeNumber,
							  parent_privs);

	/* And now translate the regular user attributes, using the vars list */
	attno = InvalidAttrNumber;
	foreach(lc, translated_vars)
	{
		Var *var = (Var *) lfirst(lc);

		attno++;
		if (var == NULL)		/* ignore dropped columns */
			continue;
		Assert(IsA(var, Var));
		if (whole_row ||
			bms_is_member(attno - FirstLowInvalidHeapAttributeNumber,
						  parent_privs))
			child_privs = bms_add_member(child_privs,
						 var->varattno - FirstLowInvalidHeapAttributeNumber);
	}

	return child_privs;
}


/*
 * make_inh_translation_list
 *	  Build the list of translations from parent Vars to child Vars for
 *	  an inheritance child.
 *
 * For paranoia's sake, we match type/collation as well as attribute name.
 */
void
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
 * set_append_rel_pathlist
 *	  Build access paths for an "append relation"
 *
 * NOTE: this function is 'public' (used in hooks.c)
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

/*
 * get_cheapest_parameterized_child_path
 *		Get cheapest path for this relation that has exactly the requested
 *		parameterization.
 *
 * Returns NULL if unable to create such a path.
 */
Path *
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
	cheapest = get_cheapest_path_for_pathkeys_compat(rel->pathlist,
													 NIL,
													 required_outer,
													 TOTAL_COST,
													 false);
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
