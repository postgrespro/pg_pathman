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
 * Lock certain partitioned relation to disable concurrent access.
 */
LockAcquireResult
xact_lock_partitioned_rel(Oid relid, bool nowait)
{
	return LockAcquireOid(relid, ShareUpdateExclusiveLock, false, nowait);
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
LockAcquireResult
xact_lock_rel_exclusive(Oid relid, bool nowait)
{
	return LockAcquireOid(relid, ExclusiveLock, false, nowait);
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
prevent_relation_modification_internal(Oid relid)
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

	/*
	 * Check if table is being modified
	 * concurrently in a separate transaction.
	 */
	if (!xact_lock_rel_exclusive(relid, true))
		ereport(ERROR,
				(errmsg("Cannot perform blocking partitioning operation"),
				 errdetail("Table \"%s\" is being modified concurrently",
						   get_rel_name_or_relid(relid))));
}
