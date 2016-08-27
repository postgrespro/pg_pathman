/* ------------------------------------------------------------------------
 *
 * xact_handling.c
 *		Transaction-specific locks and other functions
 *
 * Copyright (c) 2016, Postgres Professional
 *
 * ------------------------------------------------------------------------
 */

#include "xact_handling.h"

#include "postgres.h"
#include "catalog/catalog.h"
#include "miscadmin.h"
#include "storage/lmgr.h"


static inline void SetLocktagRelationOid(LOCKTAG *tag, Oid relid);
static inline bool do_we_hold_the_lock(Oid relid, LOCKMODE lockmode);


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

/*
 * Check whether we already hold a lock that
 * might conflict with partition spawning BGW.
 */
bool
xact_conflicting_lock_exists(Oid relid)
{
	LOCKMODE	lockmode;

	/* Try each lock >= ShareUpdateExclusiveLock */
	for (lockmode = ShareUpdateExclusiveLock;
		 lockmode <= AccessExclusiveLock;
		 lockmode++)
	{
		if (do_we_hold_the_lock(relid, lockmode))
			return true;
	}

	return false;
}


/*
 * Do we hold the specified lock?
 */
static inline bool
do_we_hold_the_lock(Oid relid, LOCKMODE lockmode)
{
	LOCKTAG		tag;

	/* Create a tag for lock */
	SetLocktagRelationOid(&tag, relid);

	/* If lock is alredy held, release it one time (decrement) */
	switch (LockAcquire(&tag, lockmode, false, true))
	{
		case LOCKACQUIRE_ALREADY_HELD:
			LockRelease(&tag, lockmode, false);
			return true;

		case LOCKACQUIRE_OK:
			LockRelease(&tag, lockmode, false);
			return false;

		default:
			return false; /* should not happen */
	}
}

/*
 * SetLocktagRelationOid
 *		Set up a locktag for a relation, given only relation OID
 */
static inline void
SetLocktagRelationOid(LOCKTAG *tag, Oid relid)
{
	Oid			dbid;

	if (IsSharedRelation(relid))
		dbid = InvalidOid;
	else
		dbid = MyDatabaseId;

	SET_LOCKTAG_RELATION(*tag, dbid, relid);
}
