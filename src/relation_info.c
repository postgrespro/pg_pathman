/* ------------------------------------------------------------------------
 *
 * relation_info.c
 *		Data structures describing partitioned relations
 *
 * Copyright (c) 2016, Postgres Professional
 *
 * ------------------------------------------------------------------------
 */

#include "relation_info.h"
#include "init.h"
#include "utils.h"

#include "access/htup_details.h"
#include "access/xact.h"
#include "catalog/indexing.h"
#include "catalog/pg_inherits.h"
#include "miscadmin.h"
#include "storage/lmgr.h"
#include "utils/builtins.h"
#include "utils/hsearch.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "utils/snapmgr.h"


static Oid try_syscache_parent_search(Oid partition, PartParentSearch *status);
static Oid get_parent_of_partition_internal(Oid partition,
											PartParentSearch *status,
											HASHACTION action);

#define FreeChildrenArray(prel) \
	do { \
		uint32	i; \
		/* Remove relevant PartParentInfos */ \
		if ((prel)->children) \
		{ \
			for (i = 0; i < PrelChildrenCount(prel); i++) \
			{ \
				Oid child = (prel)->children[i]; \
				/* If it's *always been* relid's partition, free cache */ \
				if (relid == get_parent_of_partition(child, NULL)) \
					forget_parent_of_partition(child, NULL); \
			} \
			pfree((prel)->children); \
			(prel)->children = NULL; \
		} \
	} while (0)

#define FreeRangesArray(prel) \
	do { \
		if ((prel)->ranges) pfree((prel)->ranges); \
		(prel)->ranges = NULL; \
	} while (0)


/*
 * refresh\invalidate\get\remove PartRelationInfo functions.
 */

/* Create or update PartRelationInfo in local cache. */
PartRelationInfo *
refresh_pathman_relation_info(Oid relid,
							  PartType partitioning_type,
							  const char *part_column_name)
{
	const LOCKMODE			lockmode = AccessShareLock;
	const TypeCacheEntry   *typcache;
	Oid					   *prel_children;
	uint32					prel_children_count = 0,
							i;
	bool					found;
	PartRelationInfo	   *prel;

	prel = (PartRelationInfo *) hash_search(partitioned_rels,
											(const void *) &relid,
											HASH_ENTER, &found);
	elog(DEBUG2,
		 found ?
			 "Refreshing record for relation %u in pg_pathman's cache [%u]" :
			 "Creating new record for relation %u in pg_pathman's cache [%u]",
		 relid, MyProcPid);

	/* First we assume that this entry is invalid */
	prel->valid = false;

	/* Clear outdated resources */
	if (found)
	{
		/* Free these arrays iff they're not NULL */
		FreeChildrenArray(prel);
		FreeRangesArray(prel);
	}

	/* Make both arrays point to NULL */
	prel->children = NULL;
	prel->ranges = NULL;

	/* Set partitioning type */
	prel->parttype = partitioning_type;

	/* Initialize PartRelationInfo using syscache & typcache */
	prel->attnum	= get_attnum(relid, part_column_name);
	prel->atttype	= get_atttype(relid, prel->attnum);
	prel->atttypmod	= get_atttypmod(relid, prel->attnum);
	prel->attbyval	= get_typbyval(prel->atttype);

	/* Fetch HASH & CMP fuctions for atttype */
	typcache = lookup_type_cache(prel->atttype,
								 TYPECACHE_CMP_PROC | TYPECACHE_HASH_PROC);

	prel->cmp_proc	= typcache->cmp_proc;
	prel->hash_proc	= typcache->hash_proc;

	LockRelationOid(relid, lockmode);
	prel_children = find_inheritance_children_array(relid, lockmode,
													&prel_children_count);
	UnlockRelationOid(relid, lockmode);

	/* If there's no children at all, remove this entry */
	if (prel_children_count == 0)
		remove_pathman_relation_info(relid);

	/*
	 * Fill 'prel' with partition info, raise ERROR if anything is wrong.
	 * This way PartRelationInfo will remain 'invalid', and 'get' procedure
	 * will try to refresh it again (and again), until the error is fixed
	 * by user manually (i.e. invalid check constraints etc).
	 */
	fill_prel_with_partitions(prel_children, prel_children_count, prel);

	/* Add "partition+parent" tuple to cache */
	for (i = 0; i < prel_children_count; i++)
		cache_parent_of_partition(prel_children[i], relid);

	pfree(prel_children);

	/* We've successfully built a cache entry */
	prel->valid = true;

	return prel;
}

/* Invalidate PartRelationInfo cache entry. */
void
invalidate_pathman_relation_info(Oid relid, bool *found)
{
	bool				found_prel;
	PartRelationInfo   *prel = hash_search(partitioned_rels,
										   (const void *) &relid,
										   HASH_ENTER, &found_prel);

	/* We should create entry if it doesn't exist */
	if (!found_prel)
	{
		prel->children = NULL;
		prel->ranges = NULL;
	}

	prel->valid = false; /* now cache entry is invalid */

	/* Set 'found' if needed */
	if (found) *found = found_prel;

	elog(DEBUG2,
		 "Invalidating record for relation %u in pg_pathman's cache [%u]",
		 relid, MyProcPid);
}

/* Get PartRelationInfo from local cache. */
PartRelationInfo *
get_pathman_relation_info(Oid relid, bool *found)
{
	PartRelationInfo *prel = hash_search(partitioned_rels,
										 (const void *) &relid,
										 HASH_FIND, found);

	/* Refresh PartRelationInfo if needed */
	if (prel && !PrelIsValid(prel))
	{
		Datum	values[Natts_pathman_config];
		bool	isnull[Natts_pathman_config];

		/* Check that PATHMAN_CONFIG table contains this relation */
		if (pathman_config_contains_relation(relid, values, isnull, NULL))
		{
			PartType		part_type;
			const char 	   *attname;

			/* We can't use 'part_type' & 'attname' from invalid prel */
			part_type = DatumGetPartType(values[Anum_pathman_config_parttype - 1]);
			attname = TextDatumGetCString(values[Anum_pathman_config_attname - 1]);

			/* Refresh partitioned table cache entry */
			refresh_pathman_relation_info(relid, part_type, attname);
		}
		/* Else clear remaining cache entry */
		else remove_pathman_relation_info(relid);
	}

	elog(DEBUG2,
		 "Fetching %s record for relation %u from pg_pathman's cache [%u]",
		 (prel ? "live" : "NULL"), relid, MyProcPid);

	return prel;
}

/* Remove PartRelationInfo from local cache. */
void
remove_pathman_relation_info(Oid relid)
{
	PartRelationInfo *prel = hash_search(partitioned_rels,
										 (const void *) &relid,
										 HASH_REMOVE, NULL);

	if (prel)
	{
		/* Free these arrays iff they're not NULL */
		FreeChildrenArray(prel);
		FreeRangesArray(prel);
	}

	elog(DEBUG2,
		 "Removing record for relation %u in pg_pathman's cache [%u]",
		 relid, MyProcPid);
}


/*
 * cache\forget\get PartParentInfo functions.
 */

void
cache_parent_of_partition(Oid partition, Oid parent)
{
	bool			found;
	PartParentInfo *ppar;

	ppar = hash_search(parent_cache,
					   (const void *) &partition,
					   HASH_ENTER, &found);

	elog(DEBUG2,
		 found ?
			 "Refreshing record for child %u in pg_pathman's cache [%u]" :
			 "Creating new record for child %u in pg_pathman's cache [%u]",
		 partition, MyProcPid);

	ppar->child_rel = partition;
	ppar->parent_rel = parent;
}

/* Remove "partition+parent" pair from cache & return parent's Oid */
Oid
forget_parent_of_partition(Oid partition, PartParentSearch *status)
{
	return get_parent_of_partition_internal(partition, status, HASH_REMOVE);
}

/* Peturn partition parent's Oid */
Oid
get_parent_of_partition(Oid partition, PartParentSearch *status)
{
	return get_parent_of_partition_internal(partition, status, HASH_FIND);
}

/*
 * Get [and remove] "partition+parent" pair from cache,
 * also check syscache if 'status' is provided.
 *
 * "status == NULL" implies that we don't care about
 * neither syscache nor PATHMAN_CONFIG table contents.
 */
static Oid
get_parent_of_partition_internal(Oid partition,
								 PartParentSearch *status,
								 HASHACTION action)
{
	const char	   *action_str; /* "Fetching"\"Resetting" */
	Oid				parent;
	PartParentInfo *ppar = hash_search(parent_cache,
									   (const void *) &partition,
									   action, NULL);

	/* Set 'action_str' */
	switch (action)
	{
		case HASH_REMOVE:
			action_str = "Resetting";
			break;

		case HASH_FIND:
			action_str = "Fetching";
			break;

		default:
			elog(ERROR, "Unexpected HTAB action %u", action);
	}

	elog(DEBUG2,
		 "%s %s record for child %u from pg_pathman's cache [%u]",
		 action_str, (ppar ? "live" : "NULL"), partition, MyProcPid);

	if (ppar)
	{
		if (status) *status = PPS_ENTRY_FOUND;
		parent = ppar->parent_rel;
	}
	/* Try fetching parent from syscache if 'status' is provided */
	else if (status)
		parent = try_syscache_parent_search(partition, status);
	else
		parent = InvalidOid; /* we don't have to set status */

	return parent;
}

/* Try to find parent of a partition using syscache & PATHMAN_CONFIG */
static Oid
try_syscache_parent_search(Oid partition, PartParentSearch *status)
{
	if (!IsTransactionState())
	{
		/* We could not perform search */
		if (status) *status = PPS_NOT_SURE;

		return InvalidOid;
	}
	else
	{
		Relation		relation;
		Snapshot		snapshot;
		ScanKeyData		key[1];
		SysScanDesc		scan;
		HeapTuple		inheritsTuple;
		Oid				parent = InvalidOid;

		/* At first we assume parent does not exist (not a partition) */
		if (status) *status = PPS_ENTRY_NOT_FOUND;

		relation = heap_open(InheritsRelationId, AccessShareLock);

		ScanKeyInit(&key[0],
					Anum_pg_inherits_inhrelid,
					BTEqualStrategyNumber, F_OIDEQ,
					ObjectIdGetDatum(partition));

		snapshot = RegisterSnapshot(GetLatestSnapshot());
		scan = systable_beginscan(relation, InheritsRelidSeqnoIndexId,
								  true, NULL, 1, key);

		while ((inheritsTuple = systable_getnext(scan)) != NULL)
		{
			parent = ((Form_pg_inherits) GETSTRUCT(inheritsTuple))->inhparent;

			/* Check that PATHMAN_CONFIG contains this table */
			if (pathman_config_contains_relation(parent, NULL, NULL, NULL))
			{
				/* We've found the entry, update status */
				if (status) *status = PPS_ENTRY_FOUND;
			}
			else parent = InvalidOid; /* invalidate 'parent' */

			break; /* there should be no more rows */
		}

		systable_endscan(scan);
		UnregisterSnapshot(snapshot);
		heap_close(relation, AccessShareLock);

		return parent;
	}
}

/*
 * Safe PartType wrapper.
 */
PartType
DatumGetPartType(Datum datum)
{
	uint32 val = DatumGetUInt32(datum);

	if (val < 1 || val > 2)
		elog(ERROR, "Unknown partitioning type %u", val);

	return (PartType) val;
}
