/* ------------------------------------------------------------------------
 *
 * planner_tree_modification.c
 *		Functions for query- and plan- tree modification
 *
 * Copyright (c) 2016-2020, Postgres Professional
 * Portions Copyright (c) 1996-2016, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * ------------------------------------------------------------------------
 */

#include "compat/rowmarks_fix.h"

#include "declarative.h"
#include "partition_filter.h"
#include "partition_router.h"
#include "partition_overseer.h"
#include "planner_tree_modification.h"
#include "relation_info.h"
#include "rewrite/rewriteManip.h"

#if PG_VERSION_NUM >= 120000
#include "access/table.h"
#endif
#include "access/htup_details.h"
#include "foreign/fdwapi.h"
#include "miscadmin.h"
#include "optimizer/clauses.h"
#include "storage/lmgr.h"
#include "utils/syscache.h"


/*
 * Drop conflicting macros for the sake of TRANSFORM_CONTEXT_FIELD(...).
 * For instance, Windows.h contains a nasty "#define DELETE".
 */
#ifdef SELECT
#undef SELECT
#endif

#ifdef INSERT
#undef INSERT
#endif

#ifdef UPDATE
#undef UPDATE
#endif

#ifdef DELETE
#undef DELETE
#endif


/* for assign_rel_parenthood_status() */
#define PARENTHOOD_TAG CppAsString(PARENTHOOD)

/* Build transform_query_cxt field name */
#define TRANSFORM_CONTEXT_FIELD(command_type) \
	has_parent_##command_type##_query

/* Check that transform_query_cxt field is TRUE */
#define TRANSFORM_CONTEXT_HAS_PARENT(context, command_type) \
	( (context)->TRANSFORM_CONTEXT_FIELD(command_type) )

/* Used in switch(CmdType) statements */
#define TRANSFORM_CONTEXT_SWITCH_SET(context, command_type) \
	case CMD_##command_type: \
		(context)->TRANSFORM_CONTEXT_FIELD(command_type) = true; \
		break; \

#define TRANSFORM_CONTEXT_QUERY_IS_CTE_CTE(context, query) \
	( (context)->parent_cte && \
	  (context)->parent_cte->ctequery == (Node *) (query) )

#define TRANSFORM_CONTEXT_QUERY_IS_CTE_SL(context, query) \
	( (context)->parent_sublink && \
	  (context)->parent_sublink->subselect == (Node *) (query) && \
	  (context)->parent_sublink->subLinkType == CTE_SUBLINK )

/* Check if 'query' is CTE according to 'context' */
#define TRANSFORM_CONTEXT_QUERY_IS_CTE(context, query) \
	( TRANSFORM_CONTEXT_QUERY_IS_CTE_CTE((context), (query)) || \
	  TRANSFORM_CONTEXT_QUERY_IS_CTE_SL ((context), (query)) )

typedef struct
{
	/* Do we have a parent CmdType query? */
	bool				TRANSFORM_CONTEXT_FIELD(SELECT),
						TRANSFORM_CONTEXT_FIELD(INSERT),
						TRANSFORM_CONTEXT_FIELD(UPDATE),
						TRANSFORM_CONTEXT_FIELD(DELETE);

	/* Parameters for handle_modification_query() */
	ParamListInfo		query_params;

	/* SubLink that might contain an examined query */
	SubLink			   *parent_sublink;

	/* CommonTableExpr that might contain an examined query */
	CommonTableExpr	   *parent_cte;
} transform_query_cxt;

typedef struct
{
	Index	child_varno;
	Oid		parent_relid,
			parent_reltype,
			child_reltype;
	List   *translated_vars;
} adjust_appendrel_varnos_cxt;

static bool pathman_transform_query_walker(Node *node, void *context);
static bool pathman_post_analyze_query_walker(Node *node, void *context);

static void disable_standard_inheritance(Query *parse, transform_query_cxt *context);
static void handle_modification_query(Query *parse, transform_query_cxt *context);

static Plan *partition_filter_visitor(Plan *plan, void *context);
static Plan *partition_router_visitor(Plan *plan, void *context);

static void state_visit_subplans(List *plans, void (*visitor) (), void *context);
static void state_visit_members(PlanState **planstates, int nplans, void (*visitor) (), void *context);

static Oid find_deepest_partition(Oid relid, Index rti, Expr *quals);
static Node *eval_extern_params_mutator(Node *node, ParamListInfo params);
static Node *adjust_appendrel_varnos(Node *node, adjust_appendrel_varnos_cxt *context);
static bool inh_translation_list_is_trivial(List *translated_vars);
static bool modifytable_contains_fdw(List *rtable, ModifyTable *node);


/*
 * HACK: We have to mark each Query with a unique
 * id in order to recognize them properly.
 */
#define QUERY_ID_INITIAL 0
static uint32 latest_query_id = QUERY_ID_INITIAL;


void
assign_query_id(Query *query)
{
	uint32	prev_id = latest_query_id++;

	if (prev_id > latest_query_id)
		elog(WARNING, "assign_query_id(): queryId overflow");

	query->queryId = latest_query_id;
}

void
reset_query_id_generator(void)
{
	latest_query_id = QUERY_ID_INITIAL;
}


/*
 * Basic plan tree walker.
 *
 * 'visitor' is applied right before return.
 */
Plan *
plan_tree_visitor(Plan *plan,
				  Plan *(*visitor) (Plan *plan, void *context),
				  void *context)
{
	ListCell   *l;

	if (plan == NULL)
		return NULL;

	check_stack_depth();

	/* Plan-type-specific fixes */
	switch (nodeTag(plan))
	{
		case T_SubqueryScan:
			plan_tree_visitor(((SubqueryScan *) plan)->subplan, visitor, context);
			break;

		case T_CustomScan:
			foreach (l, ((CustomScan *) plan)->custom_plans)
				plan_tree_visitor((Plan *) lfirst(l), visitor, context);
			break;

		case T_ModifyTable:
			foreach (l, ((ModifyTable *) plan)->plans)
				plan_tree_visitor((Plan *) lfirst(l), visitor, context);
			break;

		case T_Append:
			foreach (l, ((Append *) plan)->appendplans)
				plan_tree_visitor((Plan *) lfirst(l), visitor, context);
			break;

		case T_MergeAppend:
			foreach (l, ((MergeAppend *) plan)->mergeplans)
				plan_tree_visitor((Plan *) lfirst(l), visitor, context);
			break;

		case T_BitmapAnd:
			foreach (l, ((BitmapAnd *) plan)->bitmapplans)
				plan_tree_visitor((Plan *) lfirst(l), visitor, context);
			break;

		case T_BitmapOr:
			foreach (l, ((BitmapOr *) plan)->bitmapplans)
				plan_tree_visitor((Plan *) lfirst(l), visitor, context);
			break;

		default:
			break;
	}

	plan_tree_visitor(plan->lefttree, visitor, context);
	plan_tree_visitor(plan->righttree, visitor, context);

	/* Apply visitor to the current node */
	return visitor(plan, context);
}

void
state_tree_visitor(PlanState *state,
				   void (*visitor) (PlanState *plan, void *context),
				   void *context)
{
	Plan	   *plan;
	ListCell   *lc;

	if (state == NULL)
		return;

	plan = state->plan;

	check_stack_depth();

	/* Plan-type-specific fixes */
	switch (nodeTag(plan))
	{
		case T_SubqueryScan:
			state_tree_visitor(((SubqueryScanState *) state)->subplan, visitor, context);
			break;

		case T_CustomScan:
			foreach (lc, ((CustomScanState *) state)->custom_ps)
				state_tree_visitor((PlanState *) lfirst(lc), visitor, context);
			break;

		case T_ModifyTable:
			state_visit_members(((ModifyTableState *) state)->mt_plans,
								((ModifyTableState *) state)->mt_nplans,
								visitor, context);
			break;

		case T_Append:
			state_visit_members(((AppendState *) state)->appendplans,
								((AppendState *) state)->as_nplans,
								visitor, context);
			break;

		case T_MergeAppend:
			state_visit_members(((MergeAppendState *) state)->mergeplans,
								((MergeAppendState *) state)->ms_nplans,
								visitor, context);
			break;

		case T_BitmapAnd:
			state_visit_members(((BitmapAndState *) state)->bitmapplans,
								((BitmapAndState *) state)->nplans,
								visitor, context);
			break;

		case T_BitmapOr:
			state_visit_members(((BitmapOrState *) state)->bitmapplans,
								((BitmapOrState *) state)->nplans,
								visitor, context);
			break;

		default:
			break;
	}

	state_visit_subplans(state->initPlan, visitor, context);
	state_visit_subplans(state->subPlan, visitor, context);

	state_tree_visitor(state->lefttree, visitor, context);
	state_tree_visitor(state->righttree, visitor, context);

	/* Apply visitor to the current node */
	visitor(state, context);
}

/*
 * Walk a list of SubPlans (or initPlans, which also use SubPlan nodes).
 */
static void
state_visit_subplans(List *plans,
					 void (*visitor) (),
					 void *context)
{
	ListCell *lc;

	foreach (lc, plans)
	{
		SubPlanState *sps = lfirst_node(SubPlanState, lc);
		visitor(sps->planstate, context);
	}
}

/*
 * Walk the constituent plans of a ModifyTable, Append, MergeAppend,
 * BitmapAnd, or BitmapOr node.
 */
static void
state_visit_members(PlanState **planstates, int nplans,
					void (*visitor) (), void *context)
{
	int i;

	for (i = 0; i < nplans; i++)
		visitor(planstates[i], context);
}


/*
 * -------------------------------
 *  Walker for Query modification
 * -------------------------------
 */

/* Perform some transformations on Query tree */
void
pathman_transform_query(Query *parse, ParamListInfo params)
{
	transform_query_cxt context;

	/* Initialize context */
	memset((void *) &context, 0, sizeof(context));
	context.query_params = params;

	pathman_transform_query_walker((Node *) parse, (void *) &context);
}

void
pathman_post_analyze_query(Query *parse)
{
	pathman_post_analyze_query_walker((Node *) parse, NULL);
}

/* Walker for pathman_transform_query() */
static bool
pathman_transform_query_walker(Node *node, void *context)
{
	if (node == NULL)
		return false;

	else if (IsA(node, SubLink) || IsA(node, CommonTableExpr))
	{
		transform_query_cxt	   *current_context = context,
								next_context;

		/* Initialize next context for bottom subqueries */
		next_context = *current_context;

		if (IsA(node, SubLink))
		{
			next_context.parent_sublink = (SubLink *) node;
			next_context.parent_cte = NULL;
		}
		else
		{
			next_context.parent_sublink = NULL;
			next_context.parent_cte = (CommonTableExpr *) node;
		}

		/* Handle expression subtree */
		return expression_tree_walker(node,
									  pathman_transform_query_walker,
									  (void *) &next_context);
	}

	else if (IsA(node, Query))
	{
		Query				   *query = (Query *) node;
		transform_query_cxt	   *current_context = context,
								next_context;

		/* Initialize next context for bottom subqueries */
		next_context = *current_context;
		switch (query->commandType)
		{
			TRANSFORM_CONTEXT_SWITCH_SET(&next_context, SELECT);
			TRANSFORM_CONTEXT_SWITCH_SET(&next_context, INSERT);
			TRANSFORM_CONTEXT_SWITCH_SET(&next_context, UPDATE);
			TRANSFORM_CONTEXT_SWITCH_SET(&next_context, DELETE);

			default:
				break;
		}
		next_context.parent_sublink	= NULL;
		next_context.parent_cte		= NULL;

		/* Assign Query a 'queryId' */
		assign_query_id(query);

		/* Apply Query tree modifiers */
		disable_standard_inheritance(query, current_context);
		handle_modification_query(query, current_context);

		/* Handle Query node */
		return query_tree_walker(query,
								 pathman_transform_query_walker,
								 (void *) &next_context,
								 0);
	}

	/* Handle expression subtree */
	return expression_tree_walker(node,
								  pathman_transform_query_walker,
								  context);
}

static bool
pathman_post_analyze_query_walker(Node *node, void *context)
{
	if (node == NULL)
		return false;

	else if (IsA(node, Query))
	{
		Query *query = (Query *) node;

		/* Make changes for declarative syntax */
#ifdef ENABLE_DECLARATIVE
		modify_declarative_partitioning_query(query);
#endif

		/* Handle Query node */
		return query_tree_walker(query,
								 pathman_post_analyze_query_walker,
								 context,
								 0);
	}

	/* Handle expression subtree */
	return expression_tree_walker(node,
								  pathman_post_analyze_query_walker,
								  context);
}

/*
 * ----------------------
 *  Query tree modifiers
 * ----------------------
 */

/* Disable standard inheritance if table is partitioned by pg_pathman */
static void
disable_standard_inheritance(Query *parse, transform_query_cxt *context)
{
	ListCell   *lc;
	Index		current_rti; /* current range table entry index */

#ifdef LEGACY_ROWMARKS_95
	/* Don't process non-SELECT queries */
	if (parse->commandType != CMD_SELECT)
		return;

	/* Don't process queries under UPDATE or DELETE (except for CTEs) */
	if ((TRANSFORM_CONTEXT_HAS_PARENT(context, UPDATE) ||
		 TRANSFORM_CONTEXT_HAS_PARENT(context, DELETE)) &&
			!TRANSFORM_CONTEXT_QUERY_IS_CTE(context, parse))
		return;
#endif

	/* Walk through RangeTblEntries list */
	current_rti = 0;
	foreach (lc, parse->rtable)
	{
		RangeTblEntry *rte = (RangeTblEntry *) lfirst(lc);

		current_rti++; /* increment RTE index */
		Assert(current_rti != 0);

		/* Process only non-result base relations */
		if (rte->rtekind != RTE_RELATION ||
			rte->relkind != RELKIND_RELATION ||
			parse->resultRelation == current_rti) /* is it a result relation? */
			continue;

		/* Table may be partitioned */
		if (rte->inh)
		{
#ifdef LEGACY_ROWMARKS_95
			/* Don't process queries with RowMarks on 9.5 */
			if (get_parse_rowmark(parse, current_rti))
				continue;
#endif

			/* Proceed if table is partitioned by pg_pathman */
			if (has_pathman_relation_info(rte->relid))
			{
				/* HACK: unset the 'inh' flag to disable standard planning */
				rte->inh = false;

				/* Try marking it using PARENTHOOD_ALLOWED */
				assign_rel_parenthood_status(rte, PARENTHOOD_ALLOWED);
			}
		}
		/* Else try marking it using PARENTHOOD_DISALLOWED */
		else assign_rel_parenthood_status(rte, PARENTHOOD_DISALLOWED);
	}
}

/* Checks if query affects only one partition */
static void
handle_modification_query(Query *parse, transform_query_cxt *context)
{
	RangeTblEntry  *rte;
	Oid				child;
	Node		   *quals;
	Index			result_rti = parse->resultRelation;
	ParamListInfo	params = context->query_params;

	/* Exit if it's not a DELETE or UPDATE query */
	if (result_rti == 0 || (parse->commandType != CMD_UPDATE &&
							parse->commandType != CMD_DELETE))
		return;

	/* can't set earlier because CMD_UTILITY doesn't have jointree */
	quals = parse->jointree->quals;
	rte = rt_fetch(result_rti, parse->rtable);

	/* Exit if it's ONLY table */
	if (!rte->inh)
		return;

	/* Check if we can replace PARAMs with CONSTs */
	if (params && clause_contains_params(quals))
		quals = eval_extern_params_mutator(quals, params);

	/* Evaluate constaint expressions */
	quals = eval_const_expressions(NULL, quals);

	/* Parse syntax tree and extract deepest partition if possible */
	child = find_deepest_partition(rte->relid, result_rti, (Expr *) quals);

	/* Substitute parent table with partition */
	if (OidIsValid(child))
	{
		Relation	child_rel,
					parent_rel;

		LOCKMODE	lockmode = RowExclusiveLock; /* UPDATE | DELETE */

		HeapTuple	syscache_htup;
		char		child_relkind;
		Oid			parent = rte->relid;

		List	   *translated_vars;
		adjust_appendrel_varnos_cxt aav_cxt;

		/* Lock 'child' table */
		LockRelationOid(child, lockmode);

		/* Make sure that 'child' exists */
		syscache_htup = SearchSysCache1(RELOID, ObjectIdGetDatum(child));
		if (HeapTupleIsValid(syscache_htup))
		{
			Form_pg_class reltup = (Form_pg_class) GETSTRUCT(syscache_htup);

			/* Fetch child's relkind and free cache entry */
			child_relkind = reltup->relkind;
			ReleaseSysCache(syscache_htup);
		}
		else
		{
			UnlockRelationOid(child, lockmode);
			return; /* nothing to do here */
		}

		/* Update RTE's relid and relkind (for FDW) */
		rte->relid   = child;
		rte->relkind = child_relkind;

		/* HACK: unset the 'inh' flag (no children) */
		rte->inh = false;

		/* Both tables are already locked */
		child_rel  = heap_open_compat(child,  NoLock);
		parent_rel = heap_open_compat(parent, NoLock);

		make_inh_translation_list(parent_rel, child_rel, 0, &translated_vars);

		/* Perform some additional adjustments */
		if (!inh_translation_list_is_trivial(translated_vars))
		{
			/* Translate varnos for this child */
			aav_cxt.child_varno		= result_rti;
			aav_cxt.parent_relid	= parent;
			aav_cxt.parent_reltype	= RelationGetDescr(parent_rel)->tdtypeid;
			aav_cxt.child_reltype	= RelationGetDescr(child_rel)->tdtypeid;
			aav_cxt.translated_vars	= translated_vars;
			adjust_appendrel_varnos((Node *) parse, &aav_cxt);

			/* Translate column privileges for this child */
			rte->selectedCols = translate_col_privs(rte->selectedCols, translated_vars);
			rte->insertedCols = translate_col_privs(rte->insertedCols, translated_vars);
			rte->updatedCols = translate_col_privs(rte->updatedCols, translated_vars);
		}

		/* Close relations (should remain locked, though) */
		heap_close_compat(child_rel,  NoLock);
		heap_close_compat(parent_rel, NoLock);
	}
}

/* Remap parent's attributes to child ones */
static Node *
adjust_appendrel_varnos(Node *node, adjust_appendrel_varnos_cxt *context)
{
	if (node == NULL)
		return NULL;

	if (IsA(node, Query))
	{
		Query	   *query = (Query *) node;
		ListCell   *lc;

		/* FIXME: we might need to reorder TargetEntries */
		foreach (lc, query->targetList)
		{
			TargetEntry *te = (TargetEntry *) lfirst(lc);
			Var			*child_var;

			if (te->resjunk)
				continue;

			if (te->resno > list_length(context->translated_vars))
				elog(ERROR, "attribute %d of relation \"%s\" does not exist",
					 te->resno, get_rel_name(context->parent_relid));

			child_var = list_nth(context->translated_vars, te->resno - 1);
			if (!child_var)
				elog(ERROR, "attribute %d of relation \"%s\" does not exist",
					 te->resno, get_rel_name(context->parent_relid));

			/* Transform attribute number */
			te->resno = child_var->varattno;
		}

		/* NOTE: we shouldn't copy top-level Query */
		return (Node *) query_tree_mutator((Query *) node,
										   adjust_appendrel_varnos,
										   context,
										   (QTW_IGNORE_RC_SUBQUERIES |
											QTW_DONT_COPY_QUERY));
	}

	if (IsA(node, Var))
	{
		Var *var = (Var *) node;

		/* See adjust_appendrel_attrs_mutator() */
		if (var->varno == context->child_varno)
		{
			if (var->varattno > 0)
			{
				Var *child_var;

				var = copyObject(var);

				if (var->varattno > list_length(context->translated_vars))
					elog(ERROR, "attribute %d of relation \"%s\" does not exist",
						 var->varattno, get_rel_name(context->parent_relid));

				child_var = list_nth(context->translated_vars, var->varattno - 1);
				if (!child_var)
					elog(ERROR, "attribute %d of relation \"%s\" does not exist",
						 var->varattno, get_rel_name(context->parent_relid));

				/* Transform attribute number */
				var->varattno = child_var->varattno;
			}
			else if (var->varattno == 0)
			{
				ConvertRowtypeExpr *r = makeNode(ConvertRowtypeExpr);

				Assert(var->vartype = context->parent_reltype);

				r->arg = (Expr *) var;
				r->resulttype = context->parent_reltype;
				r->convertformat = COERCE_IMPLICIT_CAST;
				r->location = -1;

				/* Make sure the Var node has the right type ID, too */
				var->vartype = context->child_reltype;

				return (Node *) r;
			}
		}

		return (Node *) var;
	}

	if (IsA(node, SubLink))
	{
		SubLink *sl = (SubLink *) node;

		/* Examine its expression */
		sl->testexpr = expression_tree_mutator_compat(sl->testexpr,
													  adjust_appendrel_varnos,
													  context);
		return (Node *) sl;
	}

	return expression_tree_mutator_compat(node,
										  adjust_appendrel_varnos,
										  context);
}


/*
 * ----------------------------------------------------
 *  PartitionFilter and PartitionRouter -related stuff
 * ----------------------------------------------------
 */

/* Add PartitionFilter nodes to the plan tree */
Plan *
add_partition_filters(List *rtable, Plan *plan)
{
	if (pg_pathman_enable_partition_filter)
		return plan_tree_visitor(plan, partition_filter_visitor, rtable);

	return NULL;
}

/* Add PartitionRouter nodes to the plan tree */
Plan *
add_partition_routers(List *rtable, Plan *plan)
{
	if (pg_pathman_enable_partition_router)
		return plan_tree_visitor(plan, partition_router_visitor, rtable);

	return NULL;
}

/*
 * Add PartitionFilters to ModifyTable node's children.
 *
 * 'context' should point to the PlannedStmt->rtable.
 */
static Plan *
partition_filter_visitor(Plan *plan, void *context)
{
	List		   *rtable = (List *) context;
	ModifyTable	   *modify_table = (ModifyTable *) plan;
	ListCell	   *lc1,
				   *lc2,
				   *lc3;

	/* Skip if not ModifyTable with 'INSERT' command */
	if (!IsA(modify_table, ModifyTable) || modify_table->operation != CMD_INSERT)
		return NULL;

	Assert(rtable && IsA(rtable, List));

	lc3 = list_head(modify_table->returningLists);
	forboth (lc1, modify_table->plans,
			 lc2, modify_table->resultRelations)
	{
		Index	rindex = lfirst_int(lc2);
		Oid		relid = getrelid(rindex, rtable);

		/* Check that table is partitioned */
		if (has_pathman_relation_info(relid))
		{
			List *returning_list = NIL;

			/* Extract returning list if possible */
			if (lc3)
			{
				returning_list = lfirst(lc3);
				lc3 = lnext_compat(modify_table->returningLists, lc3);
			}

			lfirst(lc1) = make_partition_filter((Plan *) lfirst(lc1), relid,
												modify_table->nominalRelation,
												modify_table->onConflictAction,
												modify_table->operation,
												returning_list);
		}
	}

	return NULL;
}

/*
 * Add PartitionRouter to ModifyTable node's children.
 *
 * 'context' should point to the PlannedStmt->rtable.
 */
static Plan *
partition_router_visitor(Plan *plan, void *context)
{
	List		   *rtable = (List *) context;
	ModifyTable	   *modify_table = (ModifyTable *) plan;
	ListCell	   *lc1,
				   *lc2,
				   *lc3;
	bool			changed = false;

	/* Skip if not ModifyTable with 'UPDATE' command */
	if (!IsA(modify_table, ModifyTable) || modify_table->operation != CMD_UPDATE)
		return NULL;

	Assert(rtable && IsA(rtable, List));

	if (modifytable_contains_fdw(rtable, modify_table))
	{
		ereport(WARNING,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg(UPDATE_NODE_NAME " does not support foreign data wrappers")));
		return NULL;
	}

	lc3 = list_head(modify_table->returningLists);
	forboth (lc1, modify_table->plans,
			 lc2, modify_table->resultRelations)
	{
		Index	rindex = lfirst_int(lc2);
		Oid		relid = getrelid(rindex, rtable),
				tmp_relid;

		/* Find topmost parent */
		while (OidIsValid(tmp_relid = get_parent_of_partition(relid)))
			relid = tmp_relid;

		/* Check that table is partitioned */
		if (has_pathman_relation_info(relid))
		{
			List   *returning_list = NIL;
			Plan   *prouter,
				   *pfilter;

			/* Extract returning list if possible */
			if (lc3)
			{
				returning_list = lfirst(lc3);
				lc3 = lnext_compat(modify_table->returningLists, lc3);
			}

			prouter = make_partition_router((Plan *) lfirst(lc1),
											modify_table->epqParam);

			pfilter = make_partition_filter((Plan *) prouter, relid,
											modify_table->nominalRelation,
											ONCONFLICT_NONE,
											CMD_UPDATE,
											returning_list);

			lfirst(lc1) = pfilter;
			changed = true;
		}
	}

	if (changed)
		return make_partition_overseer(plan);

	return NULL;
}


/*
 * -----------------------------------------------
 *  Parenthood safety checks (SELECT * FROM ONLY)
 * -----------------------------------------------
 */

#define RPS_STATUS_ASSIGNED		( (Index) 0x2 )
#define RPS_ENABLE_PARENT		( (Index) 0x1 )

/* Set parenthood status (per query level) */
void
assign_rel_parenthood_status(RangeTblEntry *rte,
							 rel_parenthood_status new_status)
{
	Assert(rte->rtekind != RTE_CTE);

	/* HACK: set relevant bits in RTE */
	rte->ctelevelsup |= RPS_STATUS_ASSIGNED;
	if (new_status == PARENTHOOD_ALLOWED)
		rte->ctelevelsup |= RPS_ENABLE_PARENT;
}

/* Get parenthood status (per query level) */
rel_parenthood_status
get_rel_parenthood_status(RangeTblEntry *rte)
{
	Assert(rte->rtekind != RTE_CTE);

	/* HACK: check relevant bits in RTE */
	if (rte->ctelevelsup & RPS_STATUS_ASSIGNED)
		return (rte->ctelevelsup & RPS_ENABLE_PARENT) ?
					PARENTHOOD_ALLOWED :
					PARENTHOOD_DISALLOWED;

	/* Not found, return stub value */
	return PARENTHOOD_NOT_SET;
}


/*
 * --------------------------
 *  Various helper functions
 * --------------------------
 */

/* Does ModifyTable node contain any FDWs? */
static bool
modifytable_contains_fdw(List *rtable, ModifyTable *node)
{
	ListCell	*lc;

	foreach(lc, node->resultRelations)
	{
		Index			 rti = lfirst_int(lc);
		RangeTblEntry	*rte = rt_fetch(rti, rtable);

		if (rte->relkind == RELKIND_FOREIGN_TABLE)
			return true;
	}

	return false;
}

/*
 * Find a single deepest subpartition using quals.
 * It's always better to narrow down the set of tables to be scanned.
 * Return InvalidOid if it's not possible (e.g. table is not partitioned).
 */
static Oid
find_deepest_partition(Oid relid, Index rti, Expr *quals)
{
	PartRelationInfo   *prel;
	Oid					result = InvalidOid;

	/* Exit if there's no quals (no use) */
	if (!quals)
		return result;

	/* Try pruning if table is partitioned */
	if ((prel = get_pathman_relation_info(relid)) != NULL)
	{
		Node		   *prel_expr;
		WalkerContext	context;
		List		   *ranges;
		WrapperNode	   *wrap;

		/* Prepare partitioning expression */
		prel_expr = PrelExpressionForRelid(prel, rti);

		/* First we select all available partitions... */
		ranges = list_make1_irange_full(prel, IR_COMPLETE);

		/* Parse syntax tree and extract partition ranges */
		InitWalkerContext(&context, prel_expr, prel, NULL);
		wrap = walk_expr_tree(quals, &context);
		ranges = irange_list_intersection(ranges, wrap->rangeset);

		switch (irange_list_length(ranges))
		{
			/* Scan only parent (don't do constraint elimination) */
			case 0:
				result = relid;
				break;

			/* Handle the remaining partition */
			case 1:
				if (!prel->enable_parent)
				{
					IndexRange	irange = linitial_irange(ranges);
					Oid		   *children = PrelGetChildrenArray(prel),
								child = children[irange_lower(irange)];

					/* Scan this partition */
					result = child;

					/* Try to go deeper and see if there are subpartitions */
					child = find_deepest_partition(child, rti, quals);
					if (OidIsValid(child))
						result = child;
				}
				break;

			default:
				break;
		}

		/* Don't forget to close 'prel'! */
		close_pathman_relation_info(prel);
	}

	return result;
}

/* Replace extern param nodes with consts */
static Node *
eval_extern_params_mutator(Node *node, ParamListInfo params)
{
	if (node == NULL)
		return NULL;

	if (IsA(node, Param))
	{
		Param *param = (Param *) node;

		Assert(params);

		/* Look to see if we've been given a value for this Param */
		if (param->paramkind == PARAM_EXTERN &&
			param->paramid > 0 &&
			param->paramid <= params->numParams)
		{
			ParamExternData		prmdata; /* storage for 'prm' (PG 11) */
			ParamExternData	   *prm = CustomEvalParamExternCompat(param,
																  params,
																  &prmdata);

			if (OidIsValid(prm->ptype))
			{
				/* OK to substitute parameter value? */
				if (prm->pflags & PARAM_FLAG_CONST)
				{
					/*
					 * Return a Const representing the param value.
					 * Must copy pass-by-ref datatypes, since the
					 * Param might be in a memory context
					 * shorter-lived than our output plan should be.
					 */
					int16		typLen;
					bool		typByVal;
					Datum		pval;

					Assert(prm->ptype == param->paramtype);
					get_typlenbyval(param->paramtype,
									&typLen, &typByVal);
					if (prm->isnull || typByVal)
						pval = prm->value;
					else
						pval = datumCopy(prm->value, typByVal, typLen);
					return (Node *) makeConst(param->paramtype,
											  param->paramtypmod,
											  param->paramcollid,
											  (int) typLen,
											  pval,
											  prm->isnull,
											  typByVal);
				}
			}
		}
	}

	return expression_tree_mutator_compat(node, eval_extern_params_mutator,
										  (void *) params);
}

/* Check whether Var translation list is trivial (no shuffle) */
static bool
inh_translation_list_is_trivial(List *translated_vars)
{
	ListCell   *lc;
	AttrNumber	i = 1;

	foreach (lc, translated_vars)
	{
		Var *var = (Var *) lfirst(lc);

		if (var && var->varattno != i)
			return false;

		i++;
	}

	return true;
}


/*
 * -----------------------------------------------
 *  Count number of times we've visited planner()
 * -----------------------------------------------
 */

static int32 planner_calls = 0;

void
incr_planner_calls_count(void)
{
	Assert(planner_calls < PG_INT32_MAX);

	planner_calls++;
}

void
decr_planner_calls_count(void)
{
	Assert(planner_calls > 0);

	planner_calls--;
}

int32
get_planner_calls_count(void)
{
	return planner_calls;
}
