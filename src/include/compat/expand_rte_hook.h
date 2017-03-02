/* ------------------------------------------------------------------------
 *
 * expand_rte_hook.h
 *		Fix rowmarks etc using the 'expand_inherited_rtentry_hook'
 *		NOTE: this hook exists in PostgresPro
 *
 * Copyright (c) 2017, Postgres Professional
 *
 * ------------------------------------------------------------------------
 */

#ifndef EXPAND_RTE_HOOK_H
#define EXPAND_RTE_HOOK_H

#include "compat/debug_compat_features.h"


/* Does PostgreSQL have 'expand_inherited_rtentry_hook'? */
/* TODO: fix this definition once PgPro contains 'expand_rte_hook' patch */
#if defined(ENABLE_PGPRO_PATCHES) && \
	defined(ENABLE_EXPAND_RTE_HOOK) /* && ... */
#define NATIVE_EXPAND_RTE_HOOK
#endif


#ifdef NATIVE_EXPAND_RTE_HOOK

void init_expand_rte_hook(void);

#else

#define init_expand_rte_hook() ( (void) true )

#endif /* NATIVE_EXPAND_RTE_HOOK */


#endif /* EXPAND_RTE_HOOK_H */
