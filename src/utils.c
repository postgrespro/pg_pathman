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
