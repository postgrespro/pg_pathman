/* ------------------------------------------------------------------------
 *
 * planner_tree_modification.c
 *		Functions for query- and plan- tree modification
 *
 * Copyright (c) 2016, Postgres Professional
 *
 * ------------------------------------------------------------------------
 */

#include "nodes_common.h"
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


static bool pathman_transform_query_walker(Node *node, void *context);

static void disable_standard_inheritance(Query *parse);
static void rowmark_add_tableoids(Query *parse);
static void handle_modification_query(Query *parse);

static void partition_filter_visitor(Plan *plan, void *context);

static void lock_rows_visitor(Plan *plan, void *context);
static List *get_tableoids_list(List *tlist);


/*
 * This table is used to ensure that partitioned relation
 * cant't be used with both and without ONLY modifiers.
 */
static HTAB	   *per_table_parenthood_mapping = NULL;
static int		per_table_parenthood_mapping_refcount = 0;

/*
 * We have to mark each Query with a unique id in order
 * to recognize them properly.
 */
#define QUERY_ID_INITIAL 0
static uint32	latest_query_id = QUERY_ID_INITIAL;

static inline void
assign_query_id(Query *query)
{
	uint32	prev_id = latest_query_id++;

	if (prev_id > latest_query_id)
		elog(WARNING, "assign_query_id(): queryId overflow");

	query->queryId = latest_query_id;
}

static inline void
reset_query_id_generator(void)
{
	latest_query_id = QUERY_ID_INITIAL;
}


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
	pathman_transform_query_walker((Node *) parse, NULL);
}

/* Walker for pathman_transform_query() */
static bool
pathman_transform_query_walker(Node *node, void *context)
{
	if (node == NULL)
		return false;

	else if (IsA(node, Query))
	{
		Query *query = (Query *) node;

		/* Assign Query a 'queryId' */
		assign_query_id(query);

		/* Apply Query tree modifiers */
		rowmark_add_tableoids(query);
		disable_standard_inheritance(query);
		handle_modification_query(query);

		/* Handle Query node */
		return query_tree_walker(query,
								 pathman_transform_query_walker,
								 context,
								 0);
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
disable_standard_inheritance(Query *parse)
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
				assign_rel_parenthood_status(parse->queryId,
											 rte->relid,
											 PARENTHOOD_ALLOWED);
			}
		}
		/* Else try marking it using PARENTHOOD_DISALLOWED */
		else assign_rel_parenthood_status(parse->queryId,
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
rowmark_add_tableoids(Query *parse)
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
handle_modification_query(Query *parse)
{
	const PartRelationInfo *prel;
	List				   *ranges;
	RangeTblEntry		   *rte;
	WrapperNode			   *wrap;
	Expr				   *expr;
	WalkerContext			context;
	Index					result_rel;

	/* Fetch index of result relation */
	result_rel = parse->resultRelation;

	/* Exit if it's not a DELETE or UPDATE query */
	if (result_rel == 0 ||
			(parse->commandType != CMD_UPDATE &&
			 parse->commandType != CMD_DELETE))
		return;

	rte = rt_fetch(result_rel, parse->rtable);

	/* Exit if it's DELETE FROM ONLY table */
	if (!rte->inh) return;

	prel = get_pathman_relation_info(rte->relid);

	/* Exit if it's not partitioned */
	if (!prel) return;

	/* Exit if we must include parent */
	if (prel->enable_parent) return;

	/* Parse syntax tree and extract partition ranges */
	ranges = list_make1_irange(make_irange(0, PrelLastChild(prel), false));
	expr = (Expr *) eval_const_expressions(NULL, parse->jointree->quals);

	/* Exit if there's no expr (no use) */
	if (!expr) return;

	/* Parse syntax tree and extract partition ranges */
	InitWalkerContext(&context, result_rel, prel, NULL, false);
	wrap = walk_expr_tree(expr, &context);

	ranges = irange_list_intersection(ranges, wrap->rangeset);

	/*
	 * If only one partition is affected,
	 * substitute parent table with partition.
	 */
	if (irange_list_length(ranges) == 1)
	{
		IndexRange irange = linitial_irange(ranges);

		/* Exactly one partition (bounds are equal) */
		if (irange_lower(irange) == irange_upper(irange))
		{
			Oid *children = PrelGetChildrenArray(prel);

			rte->relid = children[irange_lower(irange)];

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
				   *lc2,
				   *lc3;

	/* Skip if not ModifyTable with 'INSERT' command */
	if (!IsA(modify_table, ModifyTable) || modify_table->operation != CMD_INSERT)
		return;

	Assert(rtable && IsA(rtable, List));

	lc3 = list_head(modify_table->returningLists);
	forboth (lc1, modify_table->plans, lc2, modify_table->resultRelations)
	{
		Index					rindex = lfirst_int(lc2);
		Oid						relid = getrelid(rindex, rtable);
		const PartRelationInfo *prel = get_pathman_relation_info(relid);

		/* Check that table is partitioned */
		if (prel)
		{
			List *returning_list = NIL;

			/* Extract returning list if possible */
			if (lc3)
			{
				returning_list = lfirst(lc3);
				lc3 = lnext(lc3);
			}

			lfirst(lc1) = make_partition_filter((Plan *) lfirst(lc1),
												relid,
												modify_table->onConflictAction,
												returning_list);
		}
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
	Oid						relid;		/* key (part #1) */
	uint32					queryId;	/* key (part #2) */
	rel_parenthood_status	parenthood_status;
} cached_parenthood_status;


/* Set parenthood status (per query level) */
void
assign_rel_parenthood_status(uint32 query_id,
							 Oid relid,
							 rel_parenthood_status new_status)
{
	cached_parenthood_status   *status_entry,
								key = { relid, query_id, PARENTHOOD_NOT_SET };
	bool						found;

	/* We prefer to init this table lazily */
	if (per_table_parenthood_mapping == NULL)
	{
		const long	start_elems = 50;
		HASHCTL		hashctl;

		memset(&hashctl, 0, sizeof(HASHCTL));
		hashctl.entrysize = sizeof(cached_parenthood_status);
		hashctl.keysize = offsetof(cached_parenthood_status, parenthood_status);
		hashctl.hcxt = TopTransactionContext;

		per_table_parenthood_mapping = hash_create("Parenthood Storage",
												   start_elems, &hashctl,
												   HASH_ELEM | HASH_BLOBS);
	}

	/* Search by 'key' */
	status_entry = hash_search(per_table_parenthood_mapping,
							   &key, HASH_ENTER, &found);

	if (found)
	{
		/* Saved status conflicts with 'new_status' */
		if (status_entry->parenthood_status != new_status)
		{
			elog(ERROR, "it is prohibited to apply ONLY modifier to partitioned "
						"tables which have already been mentioned without ONLY");
		}
	}
	else
	{
		/* This should NEVER happen! */
		Assert(new_status != PARENTHOOD_NOT_SET);

		status_entry->parenthood_status = new_status;
	}
}

/* Get parenthood status (per query level) */
rel_parenthood_status
get_rel_parenthood_status(uint32 query_id, Oid relid)
{
	cached_parenthood_status   *status_entry,
								key = { relid, query_id, PARENTHOOD_NOT_SET };

	/* Skip if table is not initialized */
	if (per_table_parenthood_mapping)
	{
		/* Search by 'key' */
		status_entry = hash_search(per_table_parenthood_mapping,
								   &key, HASH_FIND, NULL);

		if (status_entry)
		{
			/* This should NEVER happen! */
			Assert(status_entry->parenthood_status != PARENTHOOD_NOT_SET);

			/* Return cached parenthood status */
			return status_entry->parenthood_status;
		}
	}

	/* Not found, return stub value */
	return PARENTHOOD_NOT_SET;
}

/* Increate usage counter by 1 */
void
incr_refcount_parenthood_statuses(void)
{
	/* Increment reference counter */
	if (++per_table_parenthood_mapping_refcount <= 0)
		elog(WARNING, "imbalanced %s",
			 CppAsString(incr_refcount_parenthood_statuses));
}

/* Return current value of usage counter */
uint32
get_refcount_parenthood_statuses(void)
{
	/* incr_refcount_parenthood_statuses() is called by pathman_planner_hook() */
	return per_table_parenthood_mapping_refcount;
}

/* Reset all cached statuses if needed (query end) */
void
decr_refcount_parenthood_statuses(void)
{
	/* Decrement reference counter */
	if (--per_table_parenthood_mapping_refcount < 0)
		elog(WARNING, "imbalanced %s",
			 CppAsString(decr_refcount_parenthood_statuses));

	/* Free resources if no one is using them */
	if (per_table_parenthood_mapping_refcount == 0)
	{
		reset_query_id_generator();

		hash_destroy(per_table_parenthood_mapping);
		per_table_parenthood_mapping = NULL;
	}
}
