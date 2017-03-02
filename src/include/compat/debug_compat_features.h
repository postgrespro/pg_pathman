/* ------------------------------------------------------------------------
 *
 * debug_custom_features.h
 *		Macros to control PgPro-related features etc
 *
 * Copyright (c) 2017, Postgres Professional
 *
 * ------------------------------------------------------------------------
 */

/* Main toggle */
#define ENABLE_PGPRO_PATCHES

/* PgPro exclusive features */
//#define ENABLE_EXPAND_RTE_HOOK
//#define ENABLE_RELATION_TAGS
#define ENABLE_PATHMAN_AWARE_COPY_WIN32

/* Hacks for vanilla */
#define ENABLE_ROWMARKS_FIX
