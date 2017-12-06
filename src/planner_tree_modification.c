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

#include "compat/rowmarks_fix.h"

#include "partition_filter.h"
#include "planner_tree_modification.h"
#include "rewrite/rewriteManip.h"

#include "access/htup_details.h"
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

	/* CommonTableExpr that might containt an examined query */
	CommonTableExpr	   *parent_cte;
} transform_query_cxt;



static bool pathman_transform_query_walker(Node *node, void *context);

static void disable_standard_inheritance(Query *parse, transform_query_cxt *context);
static void handle_modification_query(Query *parse, transform_query_cxt *context);

static void partition_filter_visitor(Plan *plan, void *context);

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
			Assert(offsetof(Append, appendplans) ==
				   offsetof(MergeAppend, mergeplans));
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
	transform_query_cxt context;

	/* Initialize context */
	memset((void *) &context, 0, sizeof(context));
	context.query_params = params;

	pathman_transform_query_walker((Node *) parse, (void *) &context);
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
		next_context.parent_sublink = NULL;
		next_context.parent_cte = NULL;

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
			const PartRelationInfo *prel;

#ifdef LEGACY_ROWMARKS_95
			/* Don't process queries with RowMarks on 9.5 */
			if (get_parse_rowmark(parse, current_rti))
				continue;
#endif

			/* Proceed if table is partitioned by pg_pathman */
			if ((prel = get_pathman_relation_info(rte->relid)) != NULL)
			{
				/*
				 * HACK: unset the 'inh' flag to disable standard
				 * planning. We'll set it again later.
				 */
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
	const PartRelationInfo *prel;
	Node				   *prel_expr;
	List				   *ranges;
	RangeTblEntry		   *rte;
	WrapperNode			   *wrap;
	Expr				   *expr;
	WalkerContext			wcxt;
	Index					result_rel;
	int						num_selected;
	ParamListInfo			params;

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

	params = context->query_params;

	/* Check if we can replace PARAMs with CONSTs */
	if (params && clause_contains_params((Node *) expr))
		expr = (Expr *) eval_extern_params_mutator((Node *) expr, params);

	/* Prepare partitioning expression */
	prel_expr = PrelExpressionForRelid(prel, result_rel);

	/* Parse syntax tree and extract partition ranges */
	InitWalkerContext(&wcxt, prel_expr, prel, NULL);
	wrap = walk_expr_tree(expr, &wcxt);

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

			lfirst(lc1) = make_partition_filter((Plan *) lfirst(lc1), relid,
												modify_table->nominalRelation,
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


/*
 * -----------------------------------------------
 *  Count number of times we've visited planner()
 * -----------------------------------------------
 */

static int32 planner_calls = 0;

void
incr_planner_calls_count(void)
{
	Assert(planner_calls < INT32_MAX);

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
