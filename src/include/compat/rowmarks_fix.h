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


/*
 * If PostgreSQL supports 'expand_inherited_rtentry_hook',
 * our hacks are completely unnecessary.
 */
#if defined(ENABLE_PGPRO_PATCHES) && \
	defined(ENABLE_ROWMARKS_FIX) && \
	defined(NATIVE_EXPAND_RTE_HOOK) /* dependency */
#define NATIVE_PARTITIONING_ROWMARKS
#endif


#ifdef NATIVE_PARTITIONING_ROWMARKS
#define postprocess_lock_rows(rtable, plan)	( (void) true )
#define rowmark_add_tableoids(parse)		( (void) true )
#else
void postprocess_lock_rows(List *rtable, Plan *plan);
void rowmark_add_tableoids(Query *parse);
#endif /* NATIVE_PARTITIONING_ROWMARKS */


#endif /* ROWMARKS_FIX_H */
