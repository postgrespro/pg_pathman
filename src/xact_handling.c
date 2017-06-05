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
#include "utils.h"

#include "postgres.h"
#include "access/transam.h"
#include "access/xact.h"
#include "catalog/catalog.h"
#include "miscadmin.h"
#include "storage/lmgr.h"
#include "utils/inval.h"


static inline void SetLocktagRelationOid(LOCKTAG *tag, Oid relid);
static inline bool do_we_hold_the_lock(Oid relid, LOCKMODE lockmode);



static LockAcquireResult
LockAcquireOid(Oid relid, LOCKMODE lockmode, bool sessionLock, bool dontWait)
{
	LOCKTAG				tag;
	LockAcquireResult	res;

	/* Create a tag for lock */
	SetLocktagRelationOid(&tag, relid);

	res = LockAcquire(&tag, lockmode, sessionLock, dontWait);

	/*
	 * Now that we have the lock, check for invalidation messages;
	 * see notes in LockRelationOid.
	 */
	if (res != LOCKACQUIRE_ALREADY_HELD)
		AcceptInvalidationMessages();

	return res;
}


/*
 * Acquire lock and return LockAcquireResult.
 */
LockAcquireResult
xact_lock_rel(Oid relid, LOCKMODE lockmode, bool nowait)
{
	return LockAcquireOid(relid, lockmode, false, nowait);
}

/*
 * Check whether we already hold a lock that
 * might conflict with partition spawning BGW.
 */
bool
xact_bgw_conflicting_lock_exists(Oid relid)
{
#if PG_VERSION_NUM >= 90600
	/* We use locking groups for 9.6+ */
	return false;
#else
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
#endif
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
 * Check if 'stmt' is SET [TRANSACTION] statement.
 */
bool
xact_is_set_stmt(Node *stmt)
{
	/* Check that SET TRANSACTION is implemented via VariableSetStmt */
	Assert(VAR_SET_MULTI > 0);

	if (!stmt)
		return false;

	if (IsA(stmt, VariableSetStmt))
		return true;

	return false;
}

/*
 * Check if 'stmt' is ALTER EXTENSION pg_pathman.
 */
bool
xact_is_alter_pathman_stmt(Node *stmt)
{
	if (!stmt)
		return false;

	if (IsA(stmt, AlterExtensionStmt) &&
		0 == strcmp(((AlterExtensionStmt *) stmt)->extname,
					"pg_pathman"))
		return true;

	return false;
}

/*
 * Check if object is visible in newer transactions.
 */
bool
xact_object_is_visible(TransactionId obj_xmin)
{
	return TransactionIdPrecedes(obj_xmin, GetCurrentTransactionId()) ||
		   TransactionIdEquals(obj_xmin, FrozenTransactionId);
}

/*
 * Do we hold the specified lock?
 */
#ifdef __GNUC__
__attribute__((unused))
#endif
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


/*
 * Lock relation exclusively & check for current isolation level.
 */
void
prevent_data_modification_internal(Oid relid)
{
	/*
	 * Check that isolation level is READ COMMITTED.
	 * Else we won't be able to see new rows
	 * which could slip through locks.
	 */
	if (!xact_is_level_read_committed())
		ereport(ERROR,
				(errmsg("Cannot perform blocking partitioning operation"),
				 errdetail("Expected READ COMMITTED isolation level")));

	LockRelationOid(relid, AccessExclusiveLock);
}
