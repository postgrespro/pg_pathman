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
#include "access/nbtree.h"
#include "executor/spi.h"
#include "nodes/nodeFuncs.h"
#include "nodes/makefuncs.h"
#include "optimizer/var.h"
#include "optimizer/restrictinfo.h"
#include "utils/snapmgr.h"
#include "utils/lsyscache.h"
#include "catalog/heap.h"


static bool clause_contains_params_walker(Node *node, void *context);
static void change_varnos_in_restrinct_info(RestrictInfo *rinfo, change_varno_context *context);
static bool change_varno_walker(Node *node, change_varno_context *context);


/*
 * Check whether clause contains PARAMs or not
 */
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

/*
 * Copied from util/plancat.c
 *
 * Build a targetlist representing the columns of the specified index.
 */
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
 * Append trigger info contained in 'more' to 'src', both remain unmodified.
 *
 * This allows us to execute some of main table's triggers on children.
 * See ExecInsert() for more details.
 */
TriggerDesc *
append_trigger_descs(TriggerDesc *src, TriggerDesc *more, bool *grown_up)
{
#define CopyToTriggerDesc(bool_field_name) \
	( new_desc->bool_field_name |= (src->bool_field_name || more->bool_field_name) )

	TriggerDesc    *new_desc = (TriggerDesc *) palloc0(sizeof(TriggerDesc));
	Trigger		   *cur_trigger;
	int				i;

	/* Quick choices */
	if (!src && !more)
	{
		*grown_up = false;
		return NULL;
	}
	else if (!src)
	{
		*grown_up = true; /* expand space for new triggers */
		return more;
	}
	else if (!more)
	{
		*grown_up = false; /* no new triggers will be added */
		return src;
	}

	*grown_up = true;
	new_desc->numtriggers = src->numtriggers + more->numtriggers;
	new_desc->triggers = (Trigger *) palloc(new_desc->numtriggers * sizeof(Trigger));

	cur_trigger = new_desc->triggers;

	/* Copy triggers from 'a' */
	for (i = 0; i < src->numtriggers; i++)
		memcpy(cur_trigger++, &(src->triggers[i]), sizeof(Trigger));

	/* Copy triggers from 'b' */
	for (i = 0; i < more->numtriggers; i++)
		memcpy(cur_trigger++, &(more->triggers[i]), sizeof(Trigger));

	/* Copy insert bool flags */
	CopyToTriggerDesc(trig_insert_before_row);
	CopyToTriggerDesc(trig_insert_after_row);
	CopyToTriggerDesc(trig_insert_instead_row);
	CopyToTriggerDesc(trig_insert_before_statement);
	CopyToTriggerDesc(trig_insert_after_statement);

	/* Copy update bool flags */
	CopyToTriggerDesc(trig_update_before_row);
	CopyToTriggerDesc(trig_update_after_row);
	CopyToTriggerDesc(trig_update_instead_row);
	CopyToTriggerDesc(trig_update_before_statement);
	CopyToTriggerDesc(trig_update_after_statement);

	/* Copy delete bool flags */
	CopyToTriggerDesc(trig_delete_before_row);
	CopyToTriggerDesc(trig_delete_after_row);
	CopyToTriggerDesc(trig_delete_instead_row);
	CopyToTriggerDesc(trig_delete_before_statement);
	CopyToTriggerDesc(trig_delete_after_statement);

	/* Copy truncate bool flags */
	CopyToTriggerDesc(trig_truncate_before_statement);
	CopyToTriggerDesc(trig_truncate_after_statement);

	return new_desc;
}

/*
 * Get BTORDER_PROC for two types described by Oids
 */
void
fill_type_cmp_fmgr_info(FmgrInfo *finfo, Oid type1, Oid type2)
{
	Oid				cmp_proc_oid;
	TypeCacheEntry *tce;

	tce = lookup_type_cache(type1, TYPECACHE_BTREE_OPFAMILY);

	cmp_proc_oid = get_opfamily_proc(tce->btree_opf,
									 type1,
									 type2,
									 BTORDER_PROC);
	fmgr_info(cmp_proc_oid, finfo);

	return;
}

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
