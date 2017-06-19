/* ------------------------------------------------------------------------
 *
 * rowmarks_fix.h
 *		Hack incorrect RowMark generation due to unset 'RTE->inh' flag
 *		NOTE: this code is only useful for vanilla
 *
 * Copyright (c) 2017, Postgres Professional
 *
 * ------------------------------------------------------------------------
 */

#include "compat/rowmarks_fix.h"
#include "planner_tree_modification.h"

#include "access/sysattr.h"
#include "catalog/pg_type.h"
#include "nodes/nodeFuncs.h"
#include "optimizer/planmain.h"
#include "utils/builtins.h"
#include "utils/rel.h"


#if PG_VERSION_NUM >= 90600


/* Add missing "tableoid" column for partitioned table */
void
append_tle_for_rowmark(PlannerInfo *root, PlanRowMark *rc)
{
	Var			   *var;
	char			resname[32];
	TargetEntry	   *tle;

	var = makeVar(rc->rti,
				  TableOidAttributeNumber,
				  OIDOID,
				  -1,
				  InvalidOid,
				  0);

	snprintf(resname, sizeof(resname), "tableoid%u", rc->rowmarkId);

	tle = makeTargetEntry((Expr *) var,
						  list_length(root->processed_tlist) + 1,
						  pstrdup(resname),
						  true);

	root->processed_tlist = lappend(root->processed_tlist, tle);

	add_vars_to_targetlist(root, list_make1(var), bms_make_singleton(0), true);
}


#else


/* Special column name for rowmarks */
#define TABLEOID_STR(subst)		( "pathman_tableoid" subst )
#define TABLEOID_STR_BASE_LEN	( sizeof(TABLEOID_STR("")) - 1 )


static void lock_rows_visitor(Plan *plan, void *context);
static List *get_tableoids_list(List *tlist);


/* Final rowmark processing for partitioned tables */
void
postprocess_lock_rows(List *rtable, Plan *plan)
{
	plan_tree_walker(plan, lock_rows_visitor, rtable);
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


#endif
