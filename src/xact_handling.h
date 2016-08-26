/* ------------------------------------------------------------------------
 *
 * xact_handling.h
 *		Transaction-specific locks and other function
 *
 * Copyright (c) 2016, Postgres Professional
 *
 * ------------------------------------------------------------------------
 */

#ifndef XACT_HANDLING
#define XACT_HANDLING

#include "pathman.h"


/*
 * List of partitioned relations locked by this backend (plain Oids).
 */
extern List	   *locked_by_me;

/*
 * Transaction locks.
 */
void xact_lock_partitioned_rel(Oid relid);
void xact_unlock_partitioned_rel(Oid relid);

#endif
