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
#include "access/sysattr.h"
#include "executor/spi.h"
#include "miscadmin.h"
#include "nodes/nodeFuncs.h"
#include "nodes/makefuncs.h"
#include "optimizer/var.h"
#include "optimizer/restrictinfo.h"
#include "utils/builtins.h"
#include "utils/memutils.h"
#include "utils/lsyscache.h"
#include "rewrite/rewriteManip.h"
#include "catalog/heap.h"
#include "catalog/pg_type.h"


#define TABLEOID_STR(subst) ( "pathman_tableoid" subst )
#define TABLEOID_STR_BASE_LEN ( sizeof(TABLEOID_STR("")) - 1 )


static bool clause_contains_params_walker(Node *node, void *context);
static void change_varnos_in_restrinct_info(RestrictInfo *rinfo,
											change_varno_context *context);
static bool change_varno_walker(Node *node, change_varno_context *context);
static List *get_tableoids_list(List *tlist);
static void lock_rows_visitor(Plan *plan, void *context);


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

		if (strlen(te->resname) > TABLEOID_STR_BASE_LEN &&
			0 == strncmp(te->resname, TABLEOID_STR(""), TABLEOID_STR_BASE_LEN) &&
			var->varoattno == TableOidAttributeNumber)
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
			Oid				cur_oid;

			cur_oid = str_to_oid(&(te->resname[TABLEOID_STR_BASE_LEN]));

			if (cur_oid == parent_oid)
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

Oid
str_to_oid(const char *cstr)
{
	Datum result = DirectFunctionCall1(oidin, CStringGetDatum(cstr));

	return DatumGetObjectId(result);
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
 * Add missing 'TABLEOID_STR%u' junk attributes for inherited partitions
 *
 * This is necessary since preprocess_targetlist() heavily
 * depends on the 'inh' flag which we have to unset.
 *
 * postprocess_lock_rows() will later transform 'TABLEOID_STR:Oid'
 * relnames into 'tableoid:rowmarkId'.
 */
void
rowmark_add_tableoids(Query *parse)
{
	ListCell *lc;

	check_stack_depth();

	foreach(lc, parse->rtable)
	{
		RangeTblEntry *rte = (RangeTblEntry *) lfirst(lc);

		switch(rte->rtekind)
		{
			case RTE_SUBQUERY:
				rowmark_add_tableoids(rte->subquery);
				break;

			default:
				break;
		}
	}

	/* Generate 'tableoid' for partitioned table rowmark */
	foreach (lc, parse->rowMarks)
	{
		RowMarkClause  *rc = (RowMarkClause *) lfirst(lc);
		Oid				parent = getrelid(rc->rti, parse->rtable);
		Var			   *var;
		TargetEntry	   *tle;
		char			resname[64];

		if (!get_pathman_relation_info(parent, NULL))
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

/*
 * Final rowmark processing for partitioned tables
 */
void
postprocess_lock_rows(List *rtable, Plan *plan)
{
	plan_tree_walker(plan, lock_rows_visitor, rtable);
}
