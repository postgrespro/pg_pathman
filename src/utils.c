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
#include "parser/parse_param.h"
#include "utils/builtins.h"
#include "utils/snapmgr.h"
#include "utils/lsyscache.h"
#include "rewrite/rewriteManip.h"
#include "catalog/heap.h"


static bool clause_contains_params_walker(Node *node, void *context);

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

Oid
add_missing_partition(Oid partitioned_table, Const *value)
{
	bool	crashed;
	Oid		result = InvalidOid;

	SPI_connect();
	PushActiveSnapshot(GetTransactionSnapshot());

	/* Create partitions */
	result = create_partitions(partitioned_table,
							   value->constvalue,
							   value->consttype,
							   &crashed);

	/* Cleanup */
	SPI_finish();
	PopActiveSnapshot();

	return result;
}

void
fill_type_cmp_fmgr_info(FmgrInfo *finfo, Oid type1, Oid type2)
{
	Oid				cmp_proc_oid;
	TypeCacheEntry *tce;

	tce = lookup_type_cache(type1,
							TYPECACHE_BTREE_OPFAMILY |
								TYPECACHE_CMP_PROC |
								TYPECACHE_CMP_PROC_FINFO);

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
