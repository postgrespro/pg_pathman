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
#include "compat/expand_rte_hook.h"

#include "postgres.h"
#include "nodes/parsenodes.h"
#include "nodes/plannodes.h"
#include "nodes/relation.h"


/*
 * If PostgreSQL supports 'expand_inherited_rtentry_hook',
 * our hacks are completely unnecessary.
 */
#if defined(ENABLE_PGPRO_PATCHES) && \
	defined(ENABLE_ROWMARKS_FIX) && \
	defined(NATIVE_EXPAND_RTE_HOOK) /* dependency */
#define NATIVE_PARTITIONING_ROWMARKS
#endif


#ifndef NATIVE_PARTITIONING_ROWMARKS

void append_tle_for_rowmark(PlannerInfo *root, PlanRowMark *rc);

#else

#define append_tle_for_rowmark(root, rc) ( (void) true )

#endif /* NATIVE_PARTITIONING_ROWMARKS */


#endif /* ROWMARKS_FIX_H */
