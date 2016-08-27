/* ------------------------------------------------------------------------
 *
 * xact_handling.h
 *		Transaction-specific locks and other functions
 *
 * Copyright (c) 2016, Postgres Professional
 *
 * ------------------------------------------------------------------------
 */

#ifndef XACT_HANDLING_H
#define XACT_HANDLING_H

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
bool xact_conflicting_lock_exists(Oid relid);

#endif
