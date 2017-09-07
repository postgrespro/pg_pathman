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

/*
 * Starting from 9.6, it's possible to append junk
 * tableoid columns using the PlannerInfo->processed_tlist.
 * This is absolutely crucial for UPDATE and DELETE queries,
 * so we had to add some special fixes for 9.5:
 *
 *		1) provide legacy code for RowMarks (tableoids);
 *		2) disable dangerous UPDATE & DELETE optimizations.
 */
#define LEGACY_ROWMARKS_95

#define append_tle_for_rowmark(root, rc)	( (void) true )

void postprocess_lock_rows(List *rtable, Plan *plan);
void rowmark_add_tableoids(Query *parse);

#endif


#endif /* ROWMARKS_FIX_H */
