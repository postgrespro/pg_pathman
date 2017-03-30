/* ------------------------------------------------------------------------
 *
 * relation_info.c
 *		Data structures describing partitioned relations
 *
 * Copyright (c) 2016, Postgres Professional
 *
 * ------------------------------------------------------------------------
 */

#include "compat/pg_compat.h"

#include "relation_info.h"
#include "partition_creation.h"
#include "init.h"
#include "utils.h"
#include "xact_handling.h"

#include "access/htup_details.h"
#include "access/xact.h"
#include "catalog/catalog.h"
#include "catalog/indexing.h"
#include "catalog/pg_constraint.h"
#include "catalog/pg_inherits.h"
#include "catalog/pg_type.h"
#include "miscadmin.h"
#include "optimizer/clauses.h"
#include "storage/lmgr.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/hsearch.h"
#include "utils/memutils.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"
#include "utils/lsyscache.h"
#include "utils/typcache.h"

#if PG_VERSION_NUM >= 90600
#include "catalog/pg_constraint_fn.h"
#endif


/*
 * For pg_pathman.enable_bounds_cache GUC.
 */
bool			pg_pathman_enable_bounds_cache = true;


/*
 * We delay all invalidation jobs received in relcache hook.
 */
static List	   *delayed_invalidation_parent_rels = NIL;
static List	   *delayed_invalidation_vague_rels = NIL;
static bool		delayed_shutdown = false; /* pathman was dropped */


/* Add unique Oid to list, allocate in TopPathmanContext */
#define list_add_unique(list, oid) \
	do { \
		MemoryContext old_mcxt = MemoryContextSwitchTo(TopPathmanContext); \
		list = list_append_unique_oid(list, ObjectIdGetDatum(oid)); \
		MemoryContextSwitchTo(old_mcxt); \
	} while (0)

#define free_invalidation_list(list) \
	do { \
		list_free(list); \
		list = NIL; \
	} while (0)

static bool try_perform_parent_refresh(Oid parent);
static Oid try_syscache_parent_search(Oid partition, PartParentSearch *status);
static Oid get_parent_of_partition_internal(Oid partition,
											PartParentSearch *status,
											HASHACTION action);

static Expr *get_partition_constraint_expr(Oid partition);

static void fill_prel_with_partitions(PartRelationInfo *prel,
									  const Oid *partitions,
									  const uint32 parts_count);

static void fill_pbin_with_bounds(PartBoundInfo *pbin,
								  const PartRelationInfo *prel,
								  const Expr *constraint_expr);

static int cmp_range_entries(const void *p1, const void *p2, void *arg);


void
init_relation_info_static_data(void)
{
	DefineCustomBoolVariable("pg_pathman.enable_bounds_cache",
							 "Make updates of partition dispatch cache faster",
							 NULL,
							 &pg_pathman_enable_bounds_cache,
							 true,
							 PGC_USERSET,
							 0,
							 NULL,
							 NULL,
							 NULL);
}


/*
 * refresh\invalidate\get\remove PartRelationInfo functions.
 */

/* Create or update PartRelationInfo in local cache. Might emit ERROR. */
const PartRelationInfo *
refresh_pathman_relation_info(Oid relid,
							  Datum *values,
							  bool allow_incomplete)
{
	const LOCKMODE			lockmode = AccessShareLock;
	const TypeCacheEntry   *typcache;
	Oid					   *prel_children;
	uint32					prel_children_count = 0,
							i;
	bool					found_entry;
	PartRelationInfo	   *prel;
	Datum					param_values[Natts_pathman_config_params];
	bool					param_isnull[Natts_pathman_config_params];
	char				   *expr;
	HeapTuple				tp;
	MemoryContext			oldcontext;

	AssertTemporaryContext();

	prel = (PartRelationInfo *) pathman_cache_search_relid(partitioned_rels,
														   relid, HASH_ENTER,
														   &found_entry);
	elog(DEBUG2,
		 found_entry ?
			 "Refreshing record for relation %u in pg_pathman's cache [%u]" :
			 "Creating new record for relation %u in pg_pathman's cache [%u]",
		 relid, MyProcPid);

	/*
	 * NOTE: Trick clang analyzer (first access without NULL pointer check).
	 * Access to field 'valid' results in a dereference of a null pointer.
	 */
	prel->cmp_proc	= InvalidOid;

	/* Clear outdated resources */
	if (found_entry && PrelIsValid(prel))
	{
		FreeChildrenArray(prel);
		FreeRangesArray(prel);
		FreeIfNotNull(prel->attname);
	}

	/* First we assume that this entry is invalid */
	prel->valid		= false;

	/* Try locking parent, exit fast if 'allow_incomplete' */
	if (allow_incomplete)
	{
		if (!ConditionalLockRelationOid(relid, lockmode))
			return NULL; /* leave an invalid entry */
	}
	else LockRelationOid(relid, lockmode);

	/* Check if parent exists */
	if (!SearchSysCacheExists1(RELOID, ObjectIdGetDatum(relid)))
	{
		/* Nope, it doesn't, remove this entry and exit */
		UnlockRelationOid(relid, lockmode);
		remove_pathman_relation_info(relid);
		return NULL; /* exit */
	}

	/* Make both arrays point to NULL */
	prel->children	= NULL;
	prel->ranges	= NULL;

	/* Set partitioning type */
	prel->parttype	= DatumGetPartType(values[Anum_pathman_config_parttype - 1]);

	/* Read config values */
	prel->atttype = DatumGetObjectId(values[Anum_pathman_config_atttype - 1]);
	expr = TextDatumGetCString(values[Anum_pathman_config_expression_p - 1]);

	/* Expression and attname should be saved in cache context */
	oldcontext = MemoryContextSwitchTo(PathmanRelationCacheContext);

	prel->attname = TextDatumGetCString(values[Anum_pathman_config_expression - 1]);
	prel->expr = (Node *) stringToNode(expr);
	fix_opfuncids(prel->expr);

	MemoryContextSwitchTo(oldcontext);

	tp = SearchSysCache1(TYPEOID, values[Anum_pathman_config_atttype - 1]);
	if (HeapTupleIsValid(tp))
	{
		Form_pg_type typtup = (Form_pg_type) GETSTRUCT(tp);
		prel->atttypmod = typtup->typtypmod;
		prel->attcollid = typtup->typcollation;
		ReleaseSysCache(tp);
	}
	else
		elog(ERROR, "Something went wrong while getting type information");

	/* Fetch HASH & CMP fuctions and other stuff from type cache */
	typcache = lookup_type_cache(prel->atttype,
								 TYPECACHE_CMP_PROC | TYPECACHE_HASH_PROC);

	prel->attbyval	= typcache->typbyval;
	prel->attlen	= typcache->typlen;
	prel->attalign	= typcache->typalign;

	prel->cmp_proc	= typcache->cmp_proc;
	prel->hash_proc	= typcache->hash_proc;

	/* Try searching for children (don't wait if we can't lock) */
	switch (find_inheritance_children_array(relid, lockmode,
											allow_incomplete,
											&prel_children_count,
											&prel_children))
	{
		/* If there's no children at all, remove this entry */
		case FCS_NO_CHILDREN:
			elog(DEBUG2, "refresh: relation %u has no children [%u]",
						 relid, MyProcPid);

			UnlockRelationOid(relid, lockmode);
			remove_pathman_relation_info(relid);
			return NULL; /* exit */

		/* If can't lock children, leave an invalid entry */
		case FCS_COULD_NOT_LOCK:
			elog(DEBUG2, "refresh: cannot lock children of relation %u [%u]",
						 relid, MyProcPid);

			UnlockRelationOid(relid, lockmode);
			return NULL; /* exit */

		/* Found some children, just unlock parent */
		case FCS_FOUND:
			elog(DEBUG2, "refresh: found children of relation %u [%u]",
						 relid, MyProcPid);

			UnlockRelationOid(relid, lockmode);
			break; /* continue */

		/* Error: unknown result code */
		default:
			elog(ERROR, "error in function "
						CppAsString(find_inheritance_children_array));
	}

	/*
	 * Fill 'prel' with partition info, raise ERROR if anything is wrong.
	 * This way PartRelationInfo will remain 'invalid', and 'get' procedure
	 * will try to refresh it again (and again), until the error is fixed
	 * by user manually (i.e. invalid check constraints etc).
	 */
	PG_TRY();
	{
		fill_prel_with_partitions(prel, prel_children, prel_children_count);
	}
	PG_CATCH();
	{
		/* Free remaining resources */
		FreeChildrenArray(prel);
		FreeRangesArray(prel);
		FreeIfNotNull(prel->attname);
		FreeIfNotNull(prel->expr);

		/* Rethrow ERROR further */
		PG_RE_THROW();
	}
	PG_END_TRY();

	/* Peform some actions for each child */
	for (i = 0; i < prel_children_count; i++)
	{
		/* Add "partition+parent" pair to cache */
		cache_parent_of_partition(prel_children[i], relid);

		/* Now it's time to unlock this child */
		UnlockRelationOid(prel_children[i], lockmode);
	}

	if (prel_children)
		pfree(prel_children);

	/* Read additional parameters ('enable_parent' at the moment) */
	if (read_pathman_params(relid, param_values, param_isnull))
	{
		prel->enable_parent = param_values[Anum_pathman_config_params_enable_parent - 1];
	}
	/* Else set default values if they cannot be found */
	else
	{
		prel->enable_parent = DEFAULT_ENABLE_PARENT;
	}

	/* We've successfully built a cache entry */
	prel->valid = true;

	return prel;
}

/* Invalidate PartRelationInfo cache entry. Create new entry if 'found' is NULL. */
void
invalidate_pathman_relation_info(Oid relid, bool *found)
{
	bool				prel_found;
	HASHACTION			action = found ? HASH_FIND : HASH_ENTER;
	PartRelationInfo   *prel;

	prel = pathman_cache_search_relid(partitioned_rels,
									  relid, action,
									  &prel_found);

	if ((action == HASH_FIND ||
		(action == HASH_ENTER && prel_found)) && PrelIsValid(prel))
	{
		FreeChildrenArray(prel);
		FreeRangesArray(prel);
		FreeIfNotNull(prel->attname);

		prel->valid = false; /* now cache entry is invalid */
	}
	/* Handle invalid PartRelationInfo */
	else if (prel)
	{
		prel->children = NULL;
		prel->ranges = NULL;

		prel->valid = false; /* now cache entry is invalid */
	}

	/* Set 'found' if necessary */
	if (found) *found = prel_found;

	elog(DEBUG2,
		 "Invalidating record for relation %u in pg_pathman's cache [%u]",
		 relid, MyProcPid);
}

/* Get PartRelationInfo from local cache. */
const PartRelationInfo *
get_pathman_relation_info(Oid relid)
{
	const PartRelationInfo *prel = pathman_cache_search_relid(partitioned_rels,
															  relid, HASH_FIND,
															  NULL);
	/* Refresh PartRelationInfo if needed */
	if (prel && !PrelIsValid(prel))
	{
		Datum	values[Natts_pathman_config];
		bool	isnull[Natts_pathman_config];

		/* Check that PATHMAN_CONFIG table contains this relation */
		if (pathman_config_contains_relation(relid, values, isnull, NULL))
		{
			/* Refresh partitioned table cache entry (might turn NULL) */
			/* TODO: possible refactoring, pass found 'prel' instead of searching */
			prel = refresh_pathman_relation_info(relid, values, false);
		}

		/* Else clear remaining cache entry */
		else
		{
			remove_pathman_relation_info(relid);
			prel = NULL; /* don't forget to reset 'prel' */
		}
	}

	elog(DEBUG2,
		 "Fetching %s record for relation %u from pg_pathman's cache [%u]",
		 (prel ? "live" : "NULL"), relid, MyProcPid);

	/* Make sure that 'prel' is valid */
	Assert(!prel || PrelIsValid(prel));

	return prel;
}

/* Acquire lock on a table and try to get PartRelationInfo */
const PartRelationInfo *
get_pathman_relation_info_after_lock(Oid relid,
									 bool unlock_if_not_found,
									 LockAcquireResult *lock_result)
{
	const PartRelationInfo *prel;
	LockAcquireResult		acquire_result;

	/* Restrict concurrent partition creation (it's dangerous) */
	acquire_result = xact_lock_partitioned_rel(relid, false);

	/* Invalidate cache entry (see AcceptInvalidationMessages()) */
	invalidate_pathman_relation_info(relid, NULL);

	/* Set 'lock_result' if asked to */
	if (lock_result)
		*lock_result = acquire_result;

	prel = get_pathman_relation_info(relid);
	if (!prel && unlock_if_not_found)
		xact_unlock_partitioned_rel(relid);

	return prel;
}

/* Remove PartRelationInfo from local cache. */
void
remove_pathman_relation_info(Oid relid)
{
	PartRelationInfo *prel = pathman_cache_search_relid(partitioned_rels,
														relid, HASH_FIND,
														NULL);
	if (PrelIsValid(prel))
	{
		FreeChildrenArray(prel);
		FreeRangesArray(prel);
		FreeIfNotNull(prel->attname);
	}

	/* Now let's remove the entry completely */
	pathman_cache_search_relid(partitioned_rels, relid,
							   HASH_REMOVE, NULL);

	elog(DEBUG2,
		 "Removing record for relation %u in pg_pathman's cache [%u]",
		 relid, MyProcPid);
}

/* Fill PartRelationInfo with partition-related info */
static void
fill_prel_with_partitions(PartRelationInfo *prel,
						  const Oid *partitions,
						  const uint32 parts_count)
{
/* Allocate array if partitioning type matches 'prel' (or "ANY") */
#define AllocZeroArray(part_type, context, elem_num, elem_type) \
	( \
		((part_type) == PT_ANY || (part_type) == prel->parttype) ? \
			MemoryContextAllocZero((context), (elem_num) * sizeof(elem_type)) : \
			NULL \
	)

	uint32			i;
	MemoryContext	cache_mcxt = PathmanRelationCacheContext,
					temp_mcxt,	/* reference temporary mcxt */
					old_mcxt;	/* reference current mcxt */

	AssertTemporaryContext();

	/* Allocate memory for 'prel->children' & 'prel->ranges' (if needed) */
	prel->children	= AllocZeroArray(PT_ANY,   cache_mcxt, parts_count, Oid);
	prel->ranges	= AllocZeroArray(PT_RANGE, cache_mcxt, parts_count, RangeEntry);

	/* Set number of children */
	PrelChildrenCount(prel) = parts_count;

	/* Create temporary memory context for loop */
	temp_mcxt = AllocSetContextCreate(CurrentMemoryContext,
									  CppAsString(fill_prel_with_partitions),
									  ALLOCSET_DEFAULT_SIZES);

	/* Initialize bounds of partitions */
	for (i = 0; i < PrelChildrenCount(prel); i++)
	{
		PartBoundInfo *bound_info;

		/* Clear all previous allocations */
		MemoryContextReset(temp_mcxt);

		/* Switch to the temporary memory context */
		old_mcxt = MemoryContextSwitchTo(temp_mcxt);
		{
			/* Fetch constraint's expression tree */
			bound_info = get_bounds_of_partition(partitions[i], prel);
		}
		MemoryContextSwitchTo(old_mcxt);

		/* Copy bounds from bound cache */
		switch (prel->parttype)
		{
			case PT_HASH:
				prel->children[bound_info->hash] = bound_info->child_rel;
				break;

			case PT_RANGE:
				{
					/* Copy child's Oid */
					prel->ranges[i].child_oid = bound_info->child_rel;

					/* Copy all min & max Datums to the persistent mcxt */
					old_mcxt = MemoryContextSwitchTo(cache_mcxt);
					{
						prel->ranges[i].min = CopyBound(&bound_info->range_min,
														prel->attbyval,
														prel->attlen);

						prel->ranges[i].max = CopyBound(&bound_info->range_max,
														prel->attbyval,
														prel->attlen);
					}
					MemoryContextSwitchTo(old_mcxt);
				}
				break;

			default:
				{
					DisablePathman(); /* disable pg_pathman since config is broken */
					ereport(ERROR,
							(errmsg("Unknown partitioning type for relation \"%s\"",
									get_rel_name_or_relid(PrelParentRelid(prel))),
							 errhint(INIT_ERROR_HINT)));
				}
				break;
		}
	}

	/* Drop temporary memory context */
	MemoryContextDelete(temp_mcxt);

	/* Finalize 'prel' for a RANGE-partitioned table */
	if (prel->parttype == PT_RANGE)
	{
		FmgrInfo flinfo;

		/* Prepare function info */
		fmgr_info(prel->cmp_proc, &flinfo);

		/* Sort partitions by RangeEntry->min asc */
		qsort_arg((void *) prel->ranges, PrelChildrenCount(prel),
				  sizeof(RangeEntry), cmp_range_entries,
				  (void *) &flinfo);

		/* Initialize 'prel->children' array */
		for (i = 0; i < PrelChildrenCount(prel); i++)
			prel->children[i] = prel->ranges[i].child_oid;
	}

#ifdef USE_ASSERT_CHECKING
	/* Check that each partition Oid has been assigned properly */
	if (prel->parttype == PT_HASH)
		for (i = 0; i < PrelChildrenCount(prel); i++)
		{
			if (!OidIsValid(prel->children[i]))
			{
				DisablePathman(); /* disable pg_pathman since config is broken */
				elog(ERROR, "pg_pathman's cache for relation \"%s\" "
							"has not been properly initialized",
					 get_rel_name_or_relid(PrelParentRelid(prel)));
			}
		}
#endif
}


/*
 * Functions for delayed invalidation.
 */

/* Add new delayed pathman shutdown job (DROP EXTENSION) */
void
delay_pathman_shutdown(void)
{
	delayed_shutdown = true;
}

/* Add new delayed invalidation job for a [ex-]parent relation */
void
delay_invalidation_parent_rel(Oid parent)
{
	list_add_unique(delayed_invalidation_parent_rels, parent);
}

/* Add new delayed invalidation job for a vague relation */
void
delay_invalidation_vague_rel(Oid vague_rel)
{
	list_add_unique(delayed_invalidation_vague_rels, vague_rel);
}

/* Finish all pending invalidation jobs if possible */
void
finish_delayed_invalidation(void)
{
	/* Exit early if there's nothing to do */
	if (delayed_invalidation_parent_rels == NIL &&
		delayed_invalidation_vague_rels == NIL &&
		delayed_shutdown == false)
	{
		return;
	}

	/* Check that current state is transactional */
	if (IsTransactionState())
	{
		ListCell   *lc;

		/* Handle the probable 'DROP EXTENSION' case */
		if (delayed_shutdown)
		{
			Oid		cur_pathman_config_relid;

			/* Unset 'shutdown' flag */
			delayed_shutdown = false;

			/* Get current PATHMAN_CONFIG relid */
			cur_pathman_config_relid = get_relname_relid(PATHMAN_CONFIG,
														 get_pathman_schema());

			/* Check that PATHMAN_CONFIG table has indeed been dropped */
			if (cur_pathman_config_relid == InvalidOid ||
				cur_pathman_config_relid != get_pathman_config_relid(true))
			{
				/* Ok, let's unload pg_pathman's config */
				unload_config();

				/* Disregard all remaining invalidation jobs */
				free_invalidation_list(delayed_invalidation_parent_rels);
				free_invalidation_list(delayed_invalidation_vague_rels);

				/* No need to continue, exit */
				return;
			}
		}

		/* Process relations that are (or were) definitely partitioned */
		foreach (lc, delayed_invalidation_parent_rels)
		{
			Oid		parent = lfirst_oid(lc);

			/* Skip if it's a TOAST table */
			if (IsToastNamespace(get_rel_namespace(parent)))
				continue;

			if (!pathman_config_contains_relation(parent, NULL, NULL, NULL))
				remove_pathman_relation_info(parent);
			else
				/* get_pathman_relation_info() will refresh this entry */
				invalidate_pathman_relation_info(parent, NULL);
		}

		/* Process all other vague cases */
		foreach (lc, delayed_invalidation_vague_rels)
		{
			Oid		vague_rel = lfirst_oid(lc);

			/* Skip if it's a TOAST table */
			if (IsToastNamespace(get_rel_namespace(vague_rel)))
				continue;

			/* It might be a partitioned table or a partition */
			if (!try_perform_parent_refresh(vague_rel))
			{
				PartParentSearch	search;
				Oid					parent;

				parent = get_parent_of_partition(vague_rel, &search);

				switch (search)
				{
					/* It's still parent */
					case PPS_ENTRY_PART_PARENT:
						try_perform_parent_refresh(parent);
						break;

					/* It *might have been* parent before (not in PATHMAN_CONFIG) */
					case PPS_ENTRY_PARENT:
						remove_pathman_relation_info(parent);
						break;

					/* How come we still don't know?? */
					case PPS_NOT_SURE:
						elog(ERROR, "Unknown table status, this should never happen");
						break;

					default:
						break;
				}
			}
		}

		free_invalidation_list(delayed_invalidation_parent_rels);
		free_invalidation_list(delayed_invalidation_vague_rels);
	}
}


/*
 * cache\forget\get PartParentInfo functions.
 */

/* Create "partition+parent" pair in local cache */
void
cache_parent_of_partition(Oid partition, Oid parent)
{
	bool			found;
	PartParentInfo *ppar;

	ppar = pathman_cache_search_relid(parent_cache,
									  partition,
									  HASH_ENTER,
									  &found);
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

/* Return partition parent's Oid */
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
	PartParentInfo *ppar = pathman_cache_search_relid(parent_cache,
													  partition,
													  HASH_FIND,
													  NULL);
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
		if (status) *status = PPS_ENTRY_PART_PARENT;
		parent = ppar->parent_rel;

		/* Remove entry if necessary */
		if (action == HASH_REMOVE)
			pathman_cache_search_relid(parent_cache, partition,
									   HASH_REMOVE, NULL);
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

			/*
			 * NB: don't forget that 'inh' flag does not immediately
			 * mean that this is a pg_pathman's partition. It might
			 * be just a casual inheriting table.
			 */
			if (status) *status = PPS_ENTRY_PARENT;

			/* Check that PATHMAN_CONFIG contains this table */
			if (pathman_config_contains_relation(parent, NULL, NULL, NULL))
			{
				/* We've found the entry, update status */
				if (status) *status = PPS_ENTRY_PART_PARENT;
			}

			break; /* there should be no more rows */
		}

		systable_endscan(scan);
		UnregisterSnapshot(snapshot);
		heap_close(relation, AccessShareLock);

		return parent;
	}
}

/*
 * Try to refresh cache entry for relation 'parent'.
 *
 * Return true on success.
 */
static bool
try_perform_parent_refresh(Oid parent)
{
	Datum	values[Natts_pathman_config];
	bool	isnull[Natts_pathman_config];

	if (pathman_config_contains_relation(parent, values, isnull, NULL))
	{
		/* If anything went wrong, return false (actually, it might emit ERROR) */
		refresh_pathman_relation_info(parent,
									  values,
									  true); /* allow lazy */
	}
	/* Not a partitioned relation */
	else return false;

	return true;
}


/*
 * forget\get constraint functions.
 */

/* Remove partition's constraint from cache */
void
forget_bounds_of_partition(Oid partition)
{
	PartBoundInfo *pbin;

	/* Should we search in bounds cache? */
	pbin = pg_pathman_enable_bounds_cache ?
				pathman_cache_search_relid(bound_cache,
										   partition,
										   HASH_FIND,
										   NULL) :
				NULL; /* don't even bother */

	/* Free this entry */
	if (pbin)
	{
		/* Call pfree() if it's RANGE bounds */
		if (pbin->parttype == PT_RANGE)
		{
			FreeBound(&pbin->range_min, pbin->byval);
			FreeBound(&pbin->range_max, pbin->byval);
		}

		/* Finally remove this entry from cache */
		pathman_cache_search_relid(bound_cache,
								   partition,
								   HASH_REMOVE,
								   NULL);
	}
}

/* Return partition's constraint as expression tree */
PartBoundInfo *
get_bounds_of_partition(Oid partition, const PartRelationInfo *prel)
{
	PartBoundInfo *pbin;

	/*
	 * We might end up building the constraint
	 * tree that we wouldn't want to keep.
	 */
	AssertTemporaryContext();

	/* Should we search in bounds cache? */
	pbin = pg_pathman_enable_bounds_cache ?
				pathman_cache_search_relid(bound_cache,
										   partition,
										   HASH_FIND,
										   NULL) :
				NULL; /* don't even bother */

	/* Build new entry */
	if (!pbin)
	{
		PartBoundInfo	pbin_local;
		Expr		   *con_expr;

		/* Initialize other fields */
		pbin_local.child_rel = partition;
		pbin_local.byval = prel->attbyval;

		/* Try to build constraint's expression tree (may emit ERROR) */
		con_expr = get_partition_constraint_expr(partition);

		/* Grab bounds/hash and fill in 'pbin_local' (may emit ERROR) */
		fill_pbin_with_bounds(&pbin_local, prel, con_expr);

		/* We strive to delay the creation of cache's entry */
		pbin = pg_pathman_enable_bounds_cache ?
					pathman_cache_search_relid(bound_cache,
											   partition,
											   HASH_ENTER,
											   NULL) :
					palloc(sizeof(PartBoundInfo));

		/* Copy data from 'pbin_local' */
		memcpy(pbin, &pbin_local, sizeof(PartBoundInfo));
	}

	return pbin;
}

/*
 * Get constraint expression tree of a partition.
 *
 * build_check_constraint_name_internal() is used to build conname.
 */
static Expr *
get_partition_constraint_expr(Oid partition)
{
	Oid			conid;			/* constraint Oid */
	char	   *conname;		/* constraint name */
	HeapTuple	con_tuple;
	Datum		conbin_datum;
	bool		conbin_isnull;
	Expr	   *expr;			/* expression tree for constraint */

	conname = build_check_constraint_name_relid_internal(partition);
	conid = get_relation_constraint_oid(partition, conname, true);

	if (!OidIsValid(conid))
	{
		DisablePathman(); /* disable pg_pathman since config is broken */
		ereport(ERROR,
				(errmsg("constraint \"%s\" of partition \"%s\" does not exist",
						conname, get_rel_name_or_relid(partition)),
				 errhint(INIT_ERROR_HINT)));
	}

	con_tuple = SearchSysCache1(CONSTROID, ObjectIdGetDatum(conid));
	conbin_datum = SysCacheGetAttr(CONSTROID, con_tuple,
								   Anum_pg_constraint_conbin,
								   &conbin_isnull);
	if (conbin_isnull)
	{
		DisablePathman(); /* disable pg_pathman since config is broken */
		ereport(WARNING,
				(errmsg("constraint \"%s\" of partition \"%s\" has NULL conbin",
						conname, get_rel_name_or_relid(partition)),
				 errhint(INIT_ERROR_HINT)));
		pfree(conname);

		return NULL; /* could not parse */
	}
	pfree(conname);

	/* Finally we get a constraint expression tree */
	expr = (Expr *) stringToNode(TextDatumGetCString(conbin_datum));

	/* Don't foreget to release syscache tuple */
	ReleaseSysCache(con_tuple);

	return expr;
}

/* Fill PartBoundInfo with bounds/hash */
static void
fill_pbin_with_bounds(PartBoundInfo *pbin,
					  const PartRelationInfo *prel,
					  const Expr *constraint_expr)
{
	AssertTemporaryContext();

	/* Copy partitioning type to 'pbin' */
	pbin->parttype = prel->parttype;

	/* Perform a partitioning_type-dependent task */
	switch (prel->parttype)
	{
		case PT_HASH:
			{
				if (!validate_hash_constraint(constraint_expr,
											  prel, &pbin->hash))
				{
					DisablePathman(); /* disable pg_pathman since config is broken */
					ereport(ERROR,
							(errmsg("wrong constraint format for HASH partition \"%s\"",
									get_rel_name_or_relid(pbin->child_rel)),
							 errhint(INIT_ERROR_HINT)));
				}
			}
			break;

		case PT_RANGE:
			{
				Datum	lower, upper;
				bool	lower_null, upper_null;

				if (validate_range_constraint(constraint_expr,
											  prel, &lower, &upper,
											  &lower_null, &upper_null))
				{
					MemoryContext old_mcxt;

					/* Switch to the persistent memory context */
					old_mcxt = MemoryContextSwitchTo(PathmanBoundCacheContext);

					pbin->range_min = lower_null ?
											MakeBoundInf(MINUS_INFINITY) :
											MakeBound(datumCopy(lower,
																prel->attbyval,
																prel->attlen));

					pbin->range_max = upper_null ?
											MakeBoundInf(PLUS_INFINITY) :
											MakeBound(datumCopy(upper,
																prel->attbyval,
																prel->attlen));

					/* Switch back */
					MemoryContextSwitchTo(old_mcxt);
				}
				else
				{
					DisablePathman(); /* disable pg_pathman since config is broken */
					ereport(ERROR,
							(errmsg("wrong constraint format for RANGE partition \"%s\"",
									get_rel_name_or_relid(pbin->child_rel)),
							 errhint(INIT_ERROR_HINT)));
				}
			}
			break;

		default:
			{
				DisablePathman(); /* disable pg_pathman since config is broken */
				ereport(ERROR,
						(errmsg("Unknown partitioning type for relation \"%s\"",
								get_rel_name_or_relid(PrelParentRelid(prel))),
						 errhint(INIT_ERROR_HINT)));
			}
			break;
	}
}

/* qsort comparison function for RangeEntries */
static int
cmp_range_entries(const void *p1, const void *p2, void *arg)
{
	const RangeEntry   *v1 = (const RangeEntry *) p1;
	const RangeEntry   *v2 = (const RangeEntry *) p2;
	FmgrInfo		   *flinfo = (FmgrInfo *) arg;

	return cmp_bounds(flinfo, &v1->min, &v2->min);
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

char *
PartTypeToCString(PartType parttype)
{
	static char *hash_str	= "1",
				*range_str	= "2";

	switch (parttype)
	{
		case PT_HASH:
			return hash_str;

		case PT_RANGE:
			return range_str;

		default:
			elog(ERROR, "Unknown partitioning type %u", parttype);
			return NULL; /* keep compiler happy */
	}
}


/*
 * Common PartRelationInfo checks. Emit ERROR if anything is wrong.
 */
void
shout_if_prel_is_invalid(const Oid parent_oid,
						 const PartRelationInfo *prel,
						 const PartType expected_part_type)
{
	if (!prel)
		elog(ERROR, "relation \"%s\" has no partitions",
			 get_rel_name_or_relid(parent_oid));

	if (!PrelIsValid(prel))
		elog(ERROR, "pg_pathman's cache contains invalid entry "
					"for relation \"%s\" [%u]",
			 get_rel_name_or_relid(parent_oid),
			 MyProcPid);

	/* Check partitioning type unless it's "ANY" */
	if (expected_part_type != PT_ANY &&
		expected_part_type != prel->parttype)
	{
		char *expected_str;

		switch (expected_part_type)
		{
			case PT_HASH:
				expected_str = "HASH";
				break;

			case PT_RANGE:
				expected_str = "RANGE";
				break;

			default:
				elog(ERROR,
					 "expected_str selection not implemented for type %d",
					 expected_part_type);
		}

		elog(ERROR, "relation \"%s\" is not partitioned by %s",
			 get_rel_name_or_relid(parent_oid),
			 expected_str);
	}
}
