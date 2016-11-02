/* ------------------------------------------------------------------------
 *
 * planner_tree_modification.c
 *		Functions for query- and plan- tree modification
 *
 * Copyright (c) 2016, Postgres Professional
 *
 * ------------------------------------------------------------------------
 */

#include "partition_filter.h"
#include "planner_tree_modification.h"
#include "rangeset.h"

#include "access/sysattr.h"
#include "catalog/pg_type.h"
#include "miscadmin.h"
#include "optimizer/clauses.h"
#include "utils/builtins.h"
#include "utils/memutils.h"


/* Special column name for rowmarks */
#define TABLEOID_STR(subst)		( "pathman_tableoid" subst )
#define TABLEOID_STR_BASE_LEN	( sizeof(TABLEOID_STR("")) - 1 )


/* context for pathman_transform_query_walker() */
typedef struct
{
	int query_level; /* level of current Query */
} query_transform_cxt;

static bool pathman_transform_query_walker(Node *node, void *context);

static void disable_standard_inheritance(Query *parse, query_transform_cxt *cxt);
static void rowmark_add_tableoids(Query *parse, query_transform_cxt *cxt);
static void handle_modification_query(Query *parse, query_transform_cxt *cxt);

static void partition_filter_visitor(Plan *plan, void *context);

static void lock_rows_visitor(Plan *plan, void *context);
static List *get_tableoids_list(List *tlist);


/*
 * This list is used to ensure that partitioned relation
 * isn't used with both and without ONLY modifiers
 */
static List *per_query_parenthood_lists = NIL;


/*
 * Basic plan tree walker
 *
 * 'visitor' is applied right before return
 */
void
plan_tree_walker(Plan *plan,
				 void (*visitor) (Plan *plan, void *context),
				 void *context)
{
	ListCell   *l;

	if (plan == NULL)
		return;

	check_stack_depth();

	/* Plan-type-specific fixes */
	switch (nodeTag(plan))
	{
		case T_SubqueryScan:
			plan_tree_walker(((SubqueryScan *) plan)->subplan, visitor, context);
			break;

		case T_CustomScan:
			foreach(l, ((CustomScan *) plan)->custom_plans)
				plan_tree_walker((Plan *) lfirst(l), visitor, context);
			break;

		case T_ModifyTable:
			foreach (l, ((ModifyTable *) plan)->plans)
				plan_tree_walker((Plan *) lfirst(l), visitor, context);
			break;

		/* Since they look alike */
		case T_MergeAppend:
		case T_Append:
			foreach(l, ((Append *) plan)->appendplans)
				plan_tree_walker((Plan *) lfirst(l), visitor, context);
			break;

		case T_BitmapAnd:
			foreach(l, ((BitmapAnd *) plan)->bitmapplans)
				plan_tree_walker((Plan *) lfirst(l), visitor, context);
			break;

		case T_BitmapOr:
			foreach(l, ((BitmapOr *) plan)->bitmapplans)
				plan_tree_walker((Plan *) lfirst(l), visitor, context);
			break;

		default:
			break;
	}

	plan_tree_walker(plan->lefttree, visitor, context);
	plan_tree_walker(plan->righttree, visitor, context);

	/* Apply visitor to the current node */
	visitor(plan, context);
}


/*
 * -------------------------------
 *  Walker for Query modification
 * -------------------------------
 */

/* Perform some transformations on Query tree */
void
pathman_transform_query(Query *parse)
{
	query_transform_cxt context = { 0 };

	pathman_transform_query_walker((Node *) parse, (void *) &context);
}

/* Walker for pathman_transform_query() */
static bool
pathman_transform_query_walker(Node *node, void *context)
{
	if (node == NULL)
		return false;

	else if (IsA(node, Query))
	{
		Query				   *query = (Query *) node;
		query_transform_cxt	   *cxt = (query_transform_cxt *) context;
		bool					walker_result;

		/* Increment Query level */
		cxt->query_level++;

		/* Apply Query tree modifiers */
		rowmark_add_tableoids(query, cxt);
		disable_standard_inheritance(query, cxt);
		handle_modification_query(query, cxt);

		/* Handle Query node */
		walker_result = query_tree_walker(query,
										  pathman_transform_query_walker,
										  context,
										  0);

		/* Decrement Query level */
		cxt->query_level--;

		/* Result of query_tree_walker() */
		return walker_result;
	}

	/* Handle expression subtree */
	return expression_tree_walker(node,
								  pathman_transform_query_walker,
								  context);
}


/*
 * ----------------------
 *  Query tree modifiers
 * ----------------------
 */

/*
 * Disable standard inheritance if table is partitioned by pg_pathman.
 *
 * This function sets RangeTblEntry::inh flag to false.
 */
static void
disable_standard_inheritance(Query *parse, query_transform_cxt *cxt)
{
	ListCell *lc;

	/* Exit if it's not a SELECT query */
	if (parse->commandType != CMD_SELECT)
		return;

	/* Walk through RangeTblEntries list */
	foreach (lc, parse->rtable)
	{
		RangeTblEntry *rte = (RangeTblEntry *) lfirst(lc);

		/* Operate only on simple (non-join etc) relations */
		if (rte->rtekind != RTE_RELATION || rte->relkind != RELKIND_RELATION)
			continue;

		/* Table may be partitioned */
		if (rte->inh)
		{
			const PartRelationInfo *prel;

			/* Proceed if table is partitioned by pg_pathman */
			if ((prel = get_pathman_relation_info(rte->relid)) != NULL)
			{
				/* We'll set this flag later */
				rte->inh = false;

				/* Try marking it using PARENTHOOD_ALLOWED */
				assign_rel_parenthood_status(cxt->query_level,
											 rte->relid,
											 PARENTHOOD_ALLOWED);
			}
		}
		/* Else try marking it using PARENTHOOD_DISALLOWED */
		else assign_rel_parenthood_status(cxt->query_level,
										  rte->relid,
										  PARENTHOOD_DISALLOWED);
	}
}

/*
 * Add missing 'TABLEOID_STR%u' junk attributes for inherited partitions
 *
 * This is necessary since preprocess_targetlist() heavily
 * depends on the 'inh' flag which we have to unset.
 *
 * postprocess_lock_rows() will later transform 'TABLEOID_STR:Oid'
 * relnames into 'tableoid:rowmarkId'.
 */
static void
rowmark_add_tableoids(Query *parse, query_transform_cxt *cxt)
{
	ListCell *lc;

	/* Generate 'tableoid' for partitioned table rowmark */
	foreach (lc, parse->rowMarks)
	{
		RowMarkClause  *rc = (RowMarkClause *) lfirst(lc);
		Oid				parent = getrelid(rc->rti, parse->rtable);
		Var			   *var;
		TargetEntry	   *tle;
		char			resname[64];

		/* Check that table is partitioned */
		if (!get_pathman_relation_info(parent))
			continue;

		var = makeVar(rc->rti,
					  TableOidAttributeNumber,
					  OIDOID,
					  -1,
					  InvalidOid,
					  0);

		/* Use parent's Oid as TABLEOID_STR's key (%u) */
		snprintf(resname, sizeof(resname), TABLEOID_STR("%u"), parent);

		tle = makeTargetEntry((Expr *) var,
							  list_length(parse->targetList) + 1,
							  pstrdup(resname),
							  true);

		/* There's no problem here since new attribute is junk */
		parse->targetList = lappend(parse->targetList, tle);
	}
}

/* Checks if query affects only one partition */
static void
handle_modification_query(Query *parse, query_transform_cxt *cxt)
{
	const PartRelationInfo *prel;
	List				   *ranges;
	RangeTblEntry		   *rte;
	WrapperNode			   *wrap;
	Expr				   *expr;
	WalkerContext			context;

	/* Exit if it's not a DELETE or UPDATE query */
	if (parse->resultRelation == 0 ||
			(parse->commandType != CMD_UPDATE &&
			 parse->commandType != CMD_DELETE))
		return;

	rte = rt_fetch(parse->resultRelation, parse->rtable);
	prel = get_pathman_relation_info(rte->relid);

	/* Exit if it's not partitioned */
	if (!prel) return;

	/* Parse syntax tree and extract partition ranges */
	ranges = list_make1_irange(make_irange(0, PrelLastChild(prel), false));
	expr = (Expr *) eval_const_expressions(NULL, parse->jointree->quals);

	/* Exit if there's no expr (no use) */
	if (!expr) return;

	/* Parse syntax tree and extract partition ranges */
	InitWalkerContext(&context, prel, NULL, false);
	wrap = walk_expr_tree(expr, &context);

	ranges = irange_list_intersect(ranges, wrap->rangeset);

	/*
	 * If only one partition is affected,
	 * substitute parent table with partition.
	 */
	if (irange_list_length(ranges) == 1)
	{
		IndexRange irange = linitial_irange(ranges);

		if (irange.ir_lower == irange.ir_upper)
		{
			Oid *children = PrelGetChildrenArray(prel);

			rte->relid = children[irange.ir_lower];

			/* Disable standard planning */
			rte->inh = false;
		}
	}
}


/*
 * -------------------------------
 *  PartitionFilter-related stuff
 * -------------------------------
 */

/* Add PartitionFilter nodes to the plan tree */
void
add_partition_filters(List *rtable, Plan *plan)
{
	if (pg_pathman_enable_partition_filter)
		plan_tree_walker(plan, partition_filter_visitor, rtable);
}

/*
 * Add partition filters to ModifyTable node's children.
 *
 * 'context' should point to the PlannedStmt->rtable.
 */
static void
partition_filter_visitor(Plan *plan, void *context)
{
	List		   *rtable = (List *) context;
	ModifyTable	   *modify_table = (ModifyTable *) plan;
	ListCell	   *lc1,
				   *lc2;

	/* Skip if not ModifyTable with 'INSERT' command */
	if (!IsA(modify_table, ModifyTable) || modify_table->operation != CMD_INSERT)
		return;

	Assert(rtable && IsA(rtable, List));

	forboth (lc1, modify_table->plans, lc2, modify_table->resultRelations)
	{
		Index					rindex = lfirst_int(lc2);
		Oid						relid = getrelid(rindex, rtable);
		const PartRelationInfo *prel = get_pathman_relation_info(relid);

		/* Check that table is partitioned */
		if (prel)
			lfirst(lc1) = make_partition_filter((Plan *) lfirst(lc1),
												relid,
												modify_table->onConflictAction);
	}
}


/*
 * -----------------------
 *  Rowmark-related stuff
 * -----------------------
 */

/* Final rowmark processing for partitioned tables */
void
postprocess_lock_rows(List *rtable, Plan *plan)
{
	plan_tree_walker(plan, lock_rows_visitor, rtable);
}

/*
 * Extract target entries with resnames beginning with TABLEOID_STR
 * and var->varoattno == TableOidAttributeNumber
 */
static List *
get_tableoids_list(List *tlist)
{
	List	   *result = NIL;
	ListCell   *lc;

	foreach (lc, tlist)
	{
		TargetEntry	   *te = (TargetEntry *) lfirst(lc);
		Var			   *var = (Var *) te->expr;

		if (!IsA(var, Var))
			continue;

		/* Check that column name begins with TABLEOID_STR & it's tableoid */
		if (var->varoattno == TableOidAttributeNumber &&
			(te->resname && strlen(te->resname) > TABLEOID_STR_BASE_LEN) &&
			0 == strncmp(te->resname, TABLEOID_STR(""), TABLEOID_STR_BASE_LEN))
		{
			result = lappend(result, te);
		}
	}

	return result;
}

/*
 * Find 'TABLEOID_STR%u' attributes that were manually
 * created for partitioned tables and replace Oids
 * (used for '%u') with expected rc->rowmarkIds
 */
static void
lock_rows_visitor(Plan *plan, void *context)
{
	List		   *rtable = (List *) context;
	LockRows	   *lock_rows = (LockRows *) plan;
	Plan		   *lock_child = outerPlan(plan);
	List		   *tableoids;
	ListCell	   *lc;

	if (!IsA(lock_rows, LockRows))
		return;

	Assert(rtable && IsA(rtable, List) && lock_child);

	/* Select tableoid attributes that must be renamed */
	tableoids = get_tableoids_list(lock_child->targetlist);
	if (!tableoids)
		return; /* this LockRows has nothing to do with partitioned table */

	foreach (lc, lock_rows->rowMarks)
	{
		PlanRowMark	   *rc = (PlanRowMark *) lfirst(lc);
		Oid				parent_oid = getrelid(rc->rti, rtable);
		ListCell	   *mark_lc;
		List		   *finished_tes = NIL; /* postprocessed target entries */

		foreach (mark_lc, tableoids)
		{
			TargetEntry	   *te = (TargetEntry *) lfirst(mark_lc);
			const char	   *cur_oid_str = &(te->resname[TABLEOID_STR_BASE_LEN]);
			Datum			cur_oid_datum;

			cur_oid_datum = DirectFunctionCall1(oidin, CStringGetDatum(cur_oid_str));

			if (DatumGetObjectId(cur_oid_datum) == parent_oid)
			{
				char resname[64];

				/* Replace 'TABLEOID_STR:Oid' with 'tableoid:rowmarkId' */
				snprintf(resname, sizeof(resname), "tableoid%u", rc->rowmarkId);
				te->resname = pstrdup(resname);

				finished_tes = lappend(finished_tes, te);
			}
		}

		/* Remove target entries that have been processed in this step */
		foreach (mark_lc, finished_tes)
			tableoids = list_delete_ptr(tableoids, lfirst(mark_lc));

		if (list_length(tableoids) == 0)
			break; /* nothing to do */
	}
}


/*
 * -----------------------------------------------
 *  Parenthood safety checks (SELECT * FROM ONLY)
 * -----------------------------------------------
 */

/* private struct stored by parenthood lists */
typedef struct
{
	rel_parenthood_status	parenthood_status;
	Oid						relid;
}	cached_parenthood_status;


inline static rel_parenthood_status
list_member_parenthood(List *list, Oid relid)
{
	ListCell *lc;

	foreach (lc, list)
	{
		cached_parenthood_status *status;

		status = (cached_parenthood_status *) lfirst(lc);

		if (status->relid == relid)
			return status->parenthood_status;
	}

	return PARENTHOOD_NOT_SET;
}

inline static void
list_free_parenthood(List *list)
{
	list_free_deep(list);
}

inline static List *
lappend_parenthood(List *list, Oid relid, rel_parenthood_status new_status)
{
	cached_parenthood_status *status;

	status = palloc(sizeof(cached_parenthood_status));
	status->parenthood_status = new_status;
	status->relid = relid;

	return lappend(list, (void *) status);
}

/* Set parenthood status (per query level) */
void
assign_rel_parenthood_status(Index query_level,
							 Oid relid,
							 rel_parenthood_status new_status)
{
	List				   *nth_parenthood_list;
	ListCell			   *nth_parenthood_cell = NULL;
	rel_parenthood_status	existing_status;
	MemoryContext			old_mcxt;

	Assert(query_level > 0);

	/* Create new ListCells if it's a new Query level */
	while (query_level > list_length(per_query_parenthood_lists))
	{
		old_mcxt = MemoryContextSwitchTo(TopTransactionContext);
		per_query_parenthood_lists = lappend(per_query_parenthood_lists, NIL);
		MemoryContextSwitchTo(old_mcxt);

		nth_parenthood_cell = list_tail(per_query_parenthood_lists);
	}

	/* Else fetch an existing ListCell */
	if (nth_parenthood_cell == NULL)
		nth_parenthood_cell = list_nth_cell(per_query_parenthood_lists,
											query_level - 1);

	/* Extract parenthood list from ListCell */
	nth_parenthood_list = (List *) lfirst(nth_parenthood_cell);

	/* Search for a parenthood status */
	existing_status = list_member_parenthood(nth_parenthood_list, relid);

	/* Parenthood statuses mismatched, emit an ERROR */
	if (existing_status != new_status && existing_status != PARENTHOOD_NOT_SET)
	{
		/* Don't forget to clear all lists! */
		reset_parenthood_statuses();

		elog(ERROR, "It is prohibited to apply ONLY modifier to partitioned "
					"tables which have already been mentioned without ONLY");
	}

	/* Append new element (relid, status) */
	old_mcxt = MemoryContextSwitchTo(TopTransactionContext);
	nth_parenthood_list = lappend_parenthood(nth_parenthood_list, relid, new_status);
	MemoryContextSwitchTo(old_mcxt);

	/* Update ListCell */
	lfirst(nth_parenthood_cell) = nth_parenthood_list;
}

/* Get parenthood status (per query level) */
rel_parenthood_status
get_parenthood_status(Index query_level, Oid relid)
{
	List *nth_parenthood_list;

	Assert(query_level > 0);

	/* Return PARENTHOOD_NOT_SET if there's no such level */
	if (query_level > list_length(per_query_parenthood_lists))
		return PARENTHOOD_NOT_SET;

	/* Fetch a parenthood list for a Query indentified by 'query_level' */
	nth_parenthood_list = (List *) list_nth(per_query_parenthood_lists,
											query_level - 1);

	return list_member_parenthood(nth_parenthood_list, relid);
}

/* Reset all cached statuses (query end) */
void
reset_parenthood_statuses(void)
{
	ListCell *lc;

	/* Clear parenthood lists for each Query level */
	foreach (lc, per_query_parenthood_lists)
		list_free_parenthood((List *) lfirst(lc));

	/* Now free the main list and point it to NIL */
	list_free(per_query_parenthood_lists);
	per_query_parenthood_lists = NIL;
}
