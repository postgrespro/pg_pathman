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
#include "access/xact.h"
#include "catalog/catalog.h"
#include "miscadmin.h"
#include "storage/lmgr.h"


static inline void SetLocktagRelationOid(LOCKTAG *tag, Oid relid);
static inline bool do_we_hold_the_lock(Oid relid, LOCKMODE lockmode);


/*
 * Lock certain partitioned relation to disable concurrent access.
 */
bool
xact_lock_partitioned_rel(Oid relid, bool nowait)
{
	if (nowait)
	{
		if (ConditionalLockRelationOid(relid, ShareUpdateExclusiveLock))
			return true;
		return false;
	}
	else
		LockRelationOid(relid, ShareUpdateExclusiveLock);

	return true;
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
 * Lock relation exclusively (SELECTs are possible).
 */
bool
xact_lock_rel_exclusive(Oid relid, bool nowait)
{
	if (nowait)
	{
		if (ConditionalLockRelationOid(relid, ExclusiveLock))
			return true;
		return false;
	}
	else
		LockRelationOid(relid, ExclusiveLock);

	return true;
}

/*
 * Unlock relation (exclusive lock).
 */
void
xact_unlock_rel_exclusive(Oid relid)
{
	UnlockRelationOid(relid, ExclusiveLock);
}

/*
 * Check whether we already hold a lock that
 * might conflict with partition spawning BGW.
 */
bool
xact_bgw_conflicting_lock_exists(Oid relid)
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
 * Check if current transaction's level is READ COMMITTED.
 */
bool
xact_is_level_read_committed(void)
{
	if (XactIsoLevel <= XACT_READ_COMMITTED)
		return true;

	return false;
}

/*
 * Check if 'stmt' is BEGIN\ROLLBACK etc transaction statement.
 */
bool
xact_is_transaction_stmt(Node *stmt)
{
	if (!stmt)
		return false;

	if (IsA(stmt, TransactionStmt))
		return true;

	return false;
}

/*
 * Check if 'stmt' is SET TRANSACTION statement.
 */
bool
xact_is_set_transaction_stmt(Node *stmt)
{
	if (!stmt)
		return false;

	if (IsA(stmt, VariableSetStmt))
	{
		VariableSetStmt *var_set_stmt = (VariableSetStmt *) stmt;

		/* special case for SET TRANSACTION ... */
		if (var_set_stmt->kind == VAR_SET_MULTI)
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
			return false;
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
