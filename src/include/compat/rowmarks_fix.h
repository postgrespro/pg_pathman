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

#ifndef ROWMARKS_FIX_H
#define ROWMARKS_FIX_H

#include "compat/debug_compat_features.h"

#include "postgres.h"
#include "nodes/parsenodes.h"
#include "nodes/plannodes.h"
#include "nodes/relation.h"


#if PG_VERSION_NUM >= 90600

void append_tle_for_rowmark(PlannerInfo *root, PlanRowMark *rc);

#define postprocess_lock_rows(rtable, plan)	( (void) true )
#define rowmark_add_tableoids(parse)		( (void) true )

#else

#define LEGACY_ROWMARKS_95 /* NOTE: can't fix 9.5, see PlannerInfo->processed_tlist */

#define append_tle_for_rowmark(root, rc)	( (void) true )

void postprocess_lock_rows(List *rtable, Plan *plan);
void rowmark_add_tableoids(Query *parse);

#endif


#endif /* ROWMARKS_FIX_H */
