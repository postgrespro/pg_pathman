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

	add_vars_to_targetlist_compat(root, list_make1(var), bms_make_singleton(0));
}


#endif
