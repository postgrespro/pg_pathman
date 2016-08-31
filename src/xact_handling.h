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
 * Transaction locks.
 */
void xact_lock_partitioned_rel(Oid relid);
void xact_unlock_partitioned_rel(Oid relid);

void xact_lock_rel_data(Oid relid);
void xact_unlock_rel_data(Oid relid);

/*
 * Utility checks.
 */
bool xact_bgw_conflicting_lock_exists(Oid relid);
bool xact_is_table_being_modified(Oid relid);
bool xact_is_level_read_committed(void);

#endif
