/* ------------------------------------------------------------------------
 *
 * utils.c
 *		definitions of various support functions
 *
 * Copyright (c) 2016, Postgres Professional
 *
 * ------------------------------------------------------------------------
 */
#include "utils.h"
#include "nodes/nodeFuncs.h"
#include "nodes/makefuncs.h"
#include "optimizer/var.h"
#include "optimizer/restrictinfo.h"
#include "parser/parse_param.h"
#include "utils/builtins.h"
#include "utils/memutils.h"
#include "rewrite/rewriteManip.h"
#include "catalog/heap.h"


static bool clause_contains_params_walker(Node *node, void *context);
static void change_varnos_in_restrinct_info(RestrictInfo *rinfo, change_varno_context *context);
static bool change_varno_walker(Node *node, change_varno_context *context);


/*
 * Execute 'cb_proc' on 'xact_context' reset.
 */
void
execute_on_xact_mcxt_reset(MemoryContext xact_context,
						   MemoryContextCallbackFunction cb_proc,
						   void *arg)
{
	MemoryContextCallback *mcxt_cb = MemoryContextAlloc(xact_context,
														sizeof(MemoryContextCallback));

	/* Initialize MemoryContextCallback */
	mcxt_cb->arg = arg;
	mcxt_cb->func = cb_proc;
	mcxt_cb->next = NULL;

	MemoryContextRegisterResetCallback(xact_context, mcxt_cb);
}


/*
 * Returns the same list in reversed order.
 */
List *
list_reverse(List *l)
{
	List *result = NIL;
	ListCell *lc;

	foreach (lc, l)
	{
		result = lcons(lfirst(lc), result);
	}
	return result;
}

bool
clause_contains_params(Node *clause)
{
	return expression_tree_walker(clause,
								  clause_contains_params_walker,
								  NULL);
}

static bool
clause_contains_params_walker(Node *node, void *context)
{
	if (node == NULL)
		return false;
	if (IsA(node, Param))
		return true;
	return expression_tree_walker(node,
								  clause_contains_params_walker,
								  context);
}

/* NOTE: Used for debug */
#ifdef __GNUC__
__attribute__((unused))
#endif
static char *
bms_print(Bitmapset *bms)
{
  StringInfoData str;
  int x;

  initStringInfo(&str);
  x = -1;
  while ((x = bms_next_member(bms, x)) >= 0)
	appendStringInfo(&str, " %d", x);

  return str.data;
}

List *
build_index_tlist(PlannerInfo *root, IndexOptInfo *index,
				  Relation heapRelation)
{
	List	   *tlist = NIL;
	Index		varno = index->rel->relid;
	ListCell   *indexpr_item;
	int			i;

	indexpr_item = list_head(index->indexprs);
	for (i = 0; i < index->ncolumns; i++)
	{
		int			indexkey = index->indexkeys[i];
		Expr	   *indexvar;

		if (indexkey != 0)
		{
			/* simple column */
			Form_pg_attribute att_tup;

			if (indexkey < 0)
				att_tup = SystemAttributeDefinition(indexkey,
										   heapRelation->rd_rel->relhasoids);
			else
				att_tup = heapRelation->rd_att->attrs[indexkey - 1];

			indexvar = (Expr *) makeVar(varno,
										indexkey,
										att_tup->atttypid,
										att_tup->atttypmod,
										att_tup->attcollation,
										0);
		}
		else
		{
			/* expression column */
			if (indexpr_item == NULL)
				elog(ERROR, "wrong number of index expressions");
			indexvar = (Expr *) lfirst(indexpr_item);
			indexpr_item = lnext(indexpr_item);
		}

		tlist = lappend(tlist,
						makeTargetEntry(indexvar,
										i + 1,
										NULL,
										false));
	}
	if (indexpr_item != NULL)
		elog(ERROR, "wrong number of index expressions");

	return tlist;
}

/*
 * We should ensure that 'rel->baserestrictinfo' or 'ppi->ppi_clauses' contain
 * Var which corresponds to partition attribute before creating RuntimeXXX
 * paths since they are used by create_scan_plan() to form 'scan_clauses'
 * that are passed to create_customscan_plan().
 */
bool
check_rinfo_for_partitioned_attr(List *rinfo, Index varno, AttrNumber varattno)
{
	List	   *vars;
	List	   *clauses;
	ListCell   *lc;

	clauses = get_actual_clauses(rinfo);

	vars = pull_var_clause((Node *) clauses,
						   PVC_REJECT_AGGREGATES,
						   PVC_REJECT_PLACEHOLDERS);

	foreach (lc, vars)
	{
		Var *var = (Var *) lfirst(lc);

		if (var->varno == varno && var->varoattno == varattno)
			return true;
	}

	return false;
}

/*
 * Changes varno attribute in all variables nested in the node
 */
void
change_varnos(Node *node, Oid old_varno, Oid new_varno)
{
	change_varno_context context;
	context.old_varno = old_varno;
	context.new_varno = new_varno;

	change_varno_walker(node, &context);
}

static bool
change_varno_walker(Node *node, change_varno_context *context)
{
	ListCell   *lc;
	Var		   *var;
	EquivalenceClass *ec;
	EquivalenceMember *em;

	if (node == NULL)
		return false;

	switch(node->type)
	{
		case T_Var:
			var = (Var *) node;
			if (var->varno == context->old_varno)
			{
				var->varno = context->new_varno;
				var->varnoold = context->new_varno;
			}
			return false;

		case T_RestrictInfo:
			change_varnos_in_restrinct_info((RestrictInfo *) node, context);
			return false;

		case T_PathKey:
			change_varno_walker((Node *) ((PathKey *) node)->pk_eclass, context);
			return false;

		case T_EquivalenceClass:
			ec = (EquivalenceClass *) node;

			foreach(lc, ec->ec_members)
				change_varno_walker((Node *) lfirst(lc), context);
			foreach(lc, ec->ec_derives)
				change_varno_walker((Node *) lfirst(lc), context);
			return false;

		case T_EquivalenceMember:
			em = (EquivalenceMember *) node;
			change_varno_walker((Node *) em->em_expr, context);
			if (bms_is_member(context->old_varno, em->em_relids))
			{
				em->em_relids = bms_del_member(em->em_relids, context->old_varno);
				em->em_relids = bms_add_member(em->em_relids, context->new_varno);
			}
			return false;

		case T_TargetEntry:
			change_varno_walker((Node *) ((TargetEntry *) node)->expr, context);
			return false;

		case T_List:
			foreach(lc, (List *) node)
				change_varno_walker((Node *) lfirst(lc), context);
			return false;

		default:
			break;
	}

	/* Should not find an unplanned subquery */
	Assert(!IsA(node, Query));

	return expression_tree_walker(node, change_varno_walker, (void *) context);
}

static void
change_varnos_in_restrinct_info(RestrictInfo *rinfo, change_varno_context *context)
{
	ListCell *lc;

	change_varno_walker((Node *) rinfo->clause, context);
	if (rinfo->left_em)
		change_varno_walker((Node *) rinfo->left_em->em_expr, context);

	if (rinfo->right_em)
		change_varno_walker((Node *) rinfo->right_em->em_expr, context);

	if (rinfo->orclause)
		foreach(lc, ((BoolExpr *) rinfo->orclause)->args)
		{
			Node *node = (Node *) lfirst(lc);
			change_varno_walker(node, context);
		}

	/* TODO: find some elegant way to do this */
	if (bms_is_member(context->old_varno, rinfo->clause_relids))
	{
		rinfo->clause_relids = bms_del_member(rinfo->clause_relids, context->old_varno);
		rinfo->clause_relids = bms_add_member(rinfo->clause_relids, context->new_varno);
	}
	if (bms_is_member(context->old_varno, rinfo->left_relids))
	{
		rinfo->left_relids = bms_del_member(rinfo->left_relids, context->old_varno);
		rinfo->left_relids = bms_add_member(rinfo->left_relids, context->new_varno);
	}
	if (bms_is_member(context->old_varno, rinfo->right_relids))
	{
		rinfo->right_relids = bms_del_member(rinfo->right_relids, context->old_varno);
		rinfo->right_relids = bms_add_member(rinfo->right_relids, context->new_varno);
	}
}
