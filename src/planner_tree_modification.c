/* ------------------------------------------------------------------------
 *
 * planner_tree_modification.c
 *		Functions for query- and plan- tree modification
 *
 * Copyright (c) 2016, Postgres Professional
 * Portions Copyright (c) 1996-2016, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * ------------------------------------------------------------------------
 */

#include "compat/relation_tags.h"
#include "compat/rowmarks_fix.h"

#include "partition_filter.h"
#include "planner_tree_modification.h"
#include "rewrite/rewriteManip.h"

#include "access/htup_details.h"
#include "miscadmin.h"
#include "optimizer/clauses.h"
#include "storage/lmgr.h"
#include "utils/syscache.h"


/* for assign_rel_parenthood_status() */
#define PARENTHOOD_TAG CppAsString(PARENTHOOD)


static bool pathman_transform_query_walker(Node *node, void *context);

static void disable_standard_inheritance(Query *parse);
static void handle_modification_query(Query *parse, ParamListInfo params);

static void partition_filter_visitor(Plan *plan, void *context);

static rel_parenthood_status tag_extract_parenthood_status(List *relation_tag);

static Node *eval_extern_params_mutator(Node *node, ParamListInfo params);


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
pathman_transform_query(Query *parse, ParamListInfo params)
{
	pathman_transform_query_walker((Node *) parse, (void *) params);
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
		handle_modification_query(query, (ParamListInfo) context);

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

/* Disable standard inheritance if table is partitioned by pg_pathman */
static void
disable_standard_inheritance(Query *parse)
{
	ListCell   *lc;
	Index		current_rti; /* current range table entry index */

/*
 * We can't handle non-SELECT queries unless
 * there's a pathman_expand_inherited_rtentry_hook()
 */
#ifndef NATIVE_EXPAND_RTE_HOOK
	if (parse->commandType != CMD_SELECT)
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
			const PartRelationInfo *prel;

			/* Proceed if table is partitioned by pg_pathman */
			if ((prel = get_pathman_relation_info(rte->relid)) != NULL)
			{
				/*
				 * HACK: unset the 'inh' flag to disable standard
				 * planning. We'll set it again later.
				 */
				rte->inh = false;

				/* Try marking it using PARENTHOOD_ALLOWED */
				assign_rel_parenthood_status(parse->queryId, rte,
											 PARENTHOOD_ALLOWED);
			}
		}
		/* Else try marking it using PARENTHOOD_DISALLOWED */
		else assign_rel_parenthood_status(parse->queryId, rte,
										  PARENTHOOD_DISALLOWED);
	}
}

/* Checks if query affects only one partition */
static void
handle_modification_query(Query *parse, ParamListInfo params)
{
	const PartRelationInfo *prel;
	Node				   *prel_expr;
	List				   *ranges;
	RangeTblEntry		   *rte;
	WrapperNode			   *wrap;
	Expr				   *expr;
	WalkerContext			context;
	Index					result_rel;
	int						num_selected;

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
	ranges = list_make1_irange_full(prel, IR_COMPLETE);
	expr = (Expr *) eval_const_expressions(NULL, parse->jointree->quals);

	/* Exit if there's no expr (no use) */
	if (!expr) return;

	/* Check if we can replace PARAMs with CONSTs */
	if (params && clause_contains_params((Node *) expr))
		expr = (Expr *) eval_extern_params_mutator((Node *) expr, params);

	/* Prepare partitioning expression */
	prel_expr = PrelExpressionForRelid(prel, result_rel);

	/* Parse syntax tree and extract partition ranges */
	InitWalkerContext(&context, prel_expr, prel, NULL);
	wrap = walk_expr_tree(expr, &context);

	ranges = irange_list_intersection(ranges, wrap->rangeset);
	num_selected = irange_list_length(ranges);

	/* Special case #1: only one partition is affected */
	if (num_selected == 1)
	{
		IndexRange irange = linitial_irange(ranges);

		/* Exactly one partition (bounds are equal) */
		if (irange_lower(irange) == irange_upper(irange))
		{
			Oid		   *children	= PrelGetChildrenArray(prel),
						child		= children[irange_lower(irange)],
						parent		= rte->relid;

			Relation	child_rel,
						parent_rel;

			void	   *tuple_map; /* we don't need the map itself */

			LOCKMODE	lockmode = RowExclusiveLock; /* UPDATE | DELETE */

			HeapTuple	syscache_htup;
			char		child_relkind;

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

			/* Both tables are already locked */
			child_rel = heap_open(child, NoLock);
			parent_rel = heap_open(parent, NoLock);

			/* Build a conversion map (may be trivial, i.e. NULL) */
			tuple_map = build_part_tuple_map(parent_rel, child_rel);
			if (tuple_map)
				free_conversion_map((TupleConversionMap *) tuple_map);

			/* Close relations (should remain locked, though) */
			heap_close(child_rel, NoLock);
			heap_close(parent_rel, NoLock);

			/* Exit if tuple map was NOT trivial */
			if (tuple_map) /* just checking the pointer! */
				return;

			/* Update RTE's relid and relkind (for FDW) */
			rte->relid = child;
			rte->relkind = child_relkind;

			/* HACK: unset the 'inh' flag (no children) */
			rte->inh = false;
		}
	}

	/* Special case #2: no partitions are affected */
	else if (num_selected == 0)
	{
		/* HACK: unset the 'inh' flag (no children) */
		rte->inh = false;
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
 * -----------------------------------------------
 *  Parenthood safety checks (SELECT * FROM ONLY)
 * -----------------------------------------------
 */

/* Set parenthood status (per query level) */
void
assign_rel_parenthood_status(uint32 query_id,
							 RangeTblEntry *rte,
							 rel_parenthood_status new_status)
{

	List *old_relation_tag;

	old_relation_tag = rte_attach_tag(query_id, rte,
									  make_rte_tag_int(PARENTHOOD_TAG,
													   new_status));

	/* We already have a PARENTHOOD_TAG, examine it's value */
	if (old_relation_tag &&
		tag_extract_parenthood_status(old_relation_tag) != new_status)
	{
		elog(ERROR,
			 "it is prohibited to apply ONLY modifier to partitioned "
			 "tables which have already been mentioned without ONLY");
	}
}

/* Get parenthood status (per query level) */
rel_parenthood_status
get_rel_parenthood_status(uint32 query_id, RangeTblEntry *rte)
{
	List *relation_tag;

	relation_tag = rte_fetch_tag(query_id, rte, PARENTHOOD_TAG);
	if (relation_tag)
		return tag_extract_parenthood_status(relation_tag);

	/* Not found, return stub value */
	return PARENTHOOD_NOT_SET;
}

static rel_parenthood_status
tag_extract_parenthood_status(List *relation_tag)
{
	const Value			   *value;
	rel_parenthood_status	status;

	rte_deconstruct_tag(relation_tag, NULL, &value);
	Assert(value && IsA(value, Integer));

	status = (rel_parenthood_status) intVal(value);
	Assert(status >= PARENTHOOD_NOT_SET &&
		   status <= PARENTHOOD_ALLOWED);

	return status;
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
			ParamExternData *prm = &params->params[param->paramid - 1];

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

	return expression_tree_mutator(node, eval_extern_params_mutator,
								   (void *) params);
}
