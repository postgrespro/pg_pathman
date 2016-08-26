/* ------------------------------------------------------------------------
 *
 * xact_handling.c
 *		Transaction-specific locks and other function
 *
 * Copyright (c) 2016, Postgres Professional
 *
 * ------------------------------------------------------------------------
 */

#include "xact_handling.h"

#include "postgres.h"
#include "storage/lmgr.h"


/*
 * Lock certain partitioned relation to disable concurrent access.
 */
void
xact_lock_partitioned_rel(Oid relid)
{
	/* Share exclusive lock conflicts with itself */
	LockRelationOid(relid, ShareUpdateExclusiveLock);
}

/*
 * Unlock partitioned relation.
 */
void
xact_unlock_partitioned_rel(Oid relid)
{
	UnlockRelationOid(relid, ShareUpdateExclusiveLock);
}
