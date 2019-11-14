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
#if PG_VERSION_NUM < 120000
#include "nodes/relation.h"
#else
#include "optimizer/optimizer.h"
#endif


#if PG_VERSION_NUM >= 90600

void append_tle_for_rowmark(PlannerInfo *root, PlanRowMark *rc);

#else

/*
 * Starting from 9.6, it's possible to append junk
 * tableoid columns using the PlannerInfo->processed_tlist.
 * This is absolutely crucial for UPDATE and DELETE queries,
 * so we had to add some special fixes for 9.5:
 *
 *		1) disable dangerous UPDATE & DELETE optimizations.
 *		2) disable optimizations for SELECT .. FOR UPDATE etc.
 */
#define LEGACY_ROWMARKS_95

#define append_tle_for_rowmark(root, rc)	( (void) true )

#endif


#endif /* ROWMARKS_FIX_H */
