/* ------------------------------------------------------------------------
 *
 * init.c
 *		Initialization functions
 *
 * Copyright (c) 2015-2020, Postgres Professional
 *
 * Portions Copyright (c) 1996-2015, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * ------------------------------------------------------------------------
 */

#include "compat/pg_compat.h"

#include "hooks.h"
#include "init.h"
#include "pathman.h"
#include "pathman_workers.h"
#include "relation_info.h"
#include "utils.h"

#include "access/htup_details.h"
#include "access/heapam.h"
#include "access/genam.h"
#include "access/sysattr.h"
#if PG_VERSION_NUM >= 120000
#include "access/table.h"
#endif
#include "catalog/indexing.h"
#include "catalog/pg_extension.h"
#include "catalog/pg_inherits.h"
#include "catalog/pg_type.h"
#include "miscadmin.h"
#if PG_VERSION_NUM >= 120000
#include "nodes/nodeFuncs.h"
#endif
#include "optimizer/clauses.h"
#include "utils/inval.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/memutils.h"
#include "utils/lsyscache.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"
#include "utils/typcache.h"

#if PG_VERSION_NUM < 110000
#include "catalog/pg_inherits_fn.h"
#endif

#include <stdlib.h>


/* Various memory contexts for caches */
MemoryContext		TopPathmanContext				= NULL;
MemoryContext		PathmanParentsCacheContext		= NULL;
MemoryContext		PathmanStatusCacheContext		= NULL;
MemoryContext		PathmanBoundsCacheContext		= NULL;


/* Storage for PartParentInfos */
HTAB			   *parents_cache	= NULL;

/* Storage for PartStatusInfos */
HTAB			   *status_cache	= NULL;

/* Storage for PartBoundInfos */
HTAB			   *bounds_cache	= NULL;

/* pg_pathman's init status */
PathmanInitState 	pathman_init_state;

/* pg_pathman's hooks state */
bool				pathman_hooks_enabled = true;


/* Functions for various local caches */
static bool init_pathman_relation_oids(void);
static void fini_pathman_relation_oids(void);
static void init_local_cache(void);
static void fini_local_cache(void);

static bool validate_range_opexpr(const Expr *expr,
								  const PartRelationInfo *prel,
								  const TypeCacheEntry *tce,
								  Datum *lower, Datum *upper,
								  bool *lower_null, bool *upper_null);

static bool read_opexpr_const(const OpExpr *opexpr,
							  const PartRelationInfo *prel,
							  Datum *value);


/* Validate SQL facade */
static uint32 build_semver_uint32(char *version_cstr);
static uint32 get_plpgsql_frontend_version(void);
static void validate_plpgsql_frontend_version(uint32 current_ver,
											  uint32 compatible_ver);


/*
 * Safe hash search (takes care of disabled pg_pathman).
 */
void *
pathman_cache_search_relid(HTAB *cache_table,
						   Oid relid,
						   HASHACTION action,
						   bool *found)
{
	/* Table is NULL, take some actions */
	if (cache_table == NULL)
		switch (action)
		{
			case HASH_FIND:
			case HASH_ENTER:
			case HASH_REMOVE:
				elog(ERROR, "pg_pathman is not initialized yet");
				break;

			/* Something strange has just happened */
			default:
				elog(ERROR, "unexpected action in function "
					 CppAsString(pathman_cache_search_relid));
				break;
		}

	/* Everything is fine */
	return hash_search(cache_table, (const void *) &relid, action, found);
}

/*
 * Save and restore main init state.
 */

void
save_pathman_init_state(volatile PathmanInitState *temp_init_state)
{
	*temp_init_state = pathman_init_state;
}

void
restore_pathman_init_state(const volatile PathmanInitState *temp_init_state)
{
 	/*
	 * initialization_needed is not restored: it is not just a setting but
	 * internal thing, caches must be inited when it is set. Better would be
	 * to separate it from this struct entirely.
	 */
	pathman_init_state.pg_pathman_enable = temp_init_state->pg_pathman_enable;
	pathman_init_state.auto_partition = temp_init_state->auto_partition;
	pathman_init_state.override_copy = temp_init_state->override_copy;
}

/*
 * Create main GUCs.
 */
void
init_main_pathman_toggles(void)
{
	/* Main toggle, load_config() will enable it */
	DefineCustomBoolVariable(PATHMAN_ENABLE,
							 "Enables pg_pathman's optimizations during planning stage",
							 NULL,
							 &pathman_init_state.pg_pathman_enable,
							 DEFAULT_PATHMAN_ENABLE,
							 PGC_SUSET,
							 0,
							 pathman_enable_check_hook,
							 pathman_enable_assign_hook,
							 NULL);

	/* Global toggle for automatic partition creation */
	DefineCustomBoolVariable(PATHMAN_ENABLE_AUTO_PARTITION,
							 "Enables automatic partition creation",
							 NULL,
							 &pathman_init_state.auto_partition,
							 DEFAULT_PATHMAN_AUTO,
							 PGC_SUSET,
							 0,
							 NULL,
							 NULL,
							 NULL);

	/* Global toggle for COPY stmt handling */
	DefineCustomBoolVariable(PATHMAN_OVERRIDE_COPY,
							 "Override COPY statement handling",
							 NULL,
							 &pathman_init_state.override_copy,
							 DEFAULT_PATHMAN_OVERRIDE_COPY,
							 PGC_SUSET,
							 0,
							 NULL,
							 NULL,
							 NULL);
}

/*
 * Create local PartRelationInfo cache & load pg_pathman's config.
 * Return true on success. May occasionally emit ERROR.
 */
bool
load_config(void)
{
	static bool relcache_callback_needed = true;

	/*
	 * Try to cache important relids.
	 *
	 * Once CREATE EXTENSION stmt is processed, get_pathman_schema()
	 * function starts returning perfectly valid schema Oid, which
	 * means we have to check that *ALL* pg_pathman's relations' Oids
	 * have been cached properly. Only then can we assume that
	 * initialization is not needed anymore.
	 */
	if (!init_pathman_relation_oids())
		return false; /* remain 'uninitialized', exit before creating main caches */

	/* Validate pg_pathman's Pl/PgSQL facade (might be outdated) */
	validate_plpgsql_frontend_version(get_plpgsql_frontend_version(),
									  build_semver_uint32(LOWEST_COMPATIBLE_FRONT));

	/* Create various hash tables (caches) */
	init_local_cache();

	/* Register pathman_relcache_hook(), currently we can't unregister it */
	if (relcache_callback_needed)
	{
		CacheRegisterRelcacheCallback(pathman_relcache_hook, PointerGetDatum(NULL));
		relcache_callback_needed = false;
	}

	/* Mark pg_pathman as initialized */
	pathman_init_state.initialization_needed = false;

	elog(DEBUG2, "pg_pathman's config has been loaded successfully [%u]", MyProcPid);

	return true;
}

/*
 * Destroy local caches & free memory.
 */
void
unload_config(void)
{
	/* Don't forget to reset pg_pathman's cached relids */
	fini_pathman_relation_oids();

	/* Destroy 'partitioned_rels' & 'parent_cache' hash tables */
	fini_local_cache();

	/* Mark pg_pathman as uninitialized */
	pathman_init_state.initialization_needed = true;

	elog(DEBUG2, "pg_pathman's config has been unloaded successfully [%u]", MyProcPid);
}

/*
 * Estimate total amount of shmem needed for pg_pathman to run.
 */
Size
estimate_pathman_shmem_size(void)
{
	return estimate_concurrent_part_task_slots_size();
}

/*
 * Cache *all* important pg_pathman's relids at once.
 * We should NOT rely on any previously cached values.
 */
static bool
init_pathman_relation_oids(void)
{
	Oid schema = get_pathman_schema();
	if (schema == InvalidOid)
		return false;		/* extension can be dropped by another backend */

	/* Cache PATHMAN_CONFIG relation's Oid */
	pathman_config_relid = get_relname_relid(PATHMAN_CONFIG, schema);
	if (pathman_config_relid == InvalidOid)
		return false;

	/* Cache PATHMAN_CONFIG_PARAMS relation's Oid */
	pathman_config_params_relid = get_relname_relid(PATHMAN_CONFIG_PARAMS,
													schema);
	if (pathman_config_params_relid == InvalidOid)
		return false;

	/* NOTE: add more relations to be cached right here ^^^ */

	/* Everything is fine, proceed */
	return true;
}

/*
 * Forget *all* pg_pathman's cached relids.
 */
static void
fini_pathman_relation_oids(void)
{
	pathman_config_relid = InvalidOid;
	pathman_config_params_relid = InvalidOid;

	/* NOTE: add more relations to be forgotten right here ^^^ */
}

/*
 * Initialize per-process resources.
 */
static void
init_local_cache(void)
{
	HASHCTL ctl;

	/* Destroy caches, just in case */
	hash_destroy(parents_cache);
	hash_destroy(status_cache);
	hash_destroy(bounds_cache);

	/* Reset pg_pathman's memory contexts */
	if (TopPathmanContext)
	{
		/* Check that child contexts exist */
		Assert(MemoryContextIsValid(PathmanParentsCacheContext));
		Assert(MemoryContextIsValid(PathmanStatusCacheContext));
		Assert(MemoryContextIsValid(PathmanBoundsCacheContext));

		/* Clear children */
		MemoryContextReset(PathmanParentsCacheContext);
		MemoryContextReset(PathmanStatusCacheContext);
		MemoryContextReset(PathmanBoundsCacheContext);
	}
	/* Initialize pg_pathman's memory contexts */
	else
	{
		Assert(PathmanParentsCacheContext == NULL);
		Assert(PathmanStatusCacheContext == NULL);
		Assert(PathmanBoundsCacheContext == NULL);

		TopPathmanContext =
				AllocSetContextCreate(TopMemoryContext,
									  PATHMAN_TOP_CONTEXT,
									  ALLOCSET_DEFAULT_SIZES);

		/* For PartParentInfo */
		PathmanParentsCacheContext =
				AllocSetContextCreate(TopPathmanContext,
									  PATHMAN_PARENTS_CACHE,
									  ALLOCSET_SMALL_SIZES);

		/* For PartStatusInfo */
		PathmanStatusCacheContext =
				AllocSetContextCreate(TopPathmanContext,
									  PATHMAN_STATUS_CACHE,
									  ALLOCSET_SMALL_SIZES);

		/* For PartBoundInfo */
		PathmanBoundsCacheContext =
				AllocSetContextCreate(TopPathmanContext,
									  PATHMAN_BOUNDS_CACHE,
									  ALLOCSET_SMALL_SIZES);
	}

	memset(&ctl, 0, sizeof(ctl));
	ctl.keysize = sizeof(Oid);
	ctl.entrysize = sizeof(PartParentInfo);
	ctl.hcxt = PathmanParentsCacheContext;

	parents_cache = hash_create(PATHMAN_PARENTS_CACHE,
								PART_RELS_SIZE, &ctl,
								HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);

	memset(&ctl, 0, sizeof(ctl));
	ctl.keysize = sizeof(Oid);
	ctl.entrysize = sizeof(PartStatusInfo);
	ctl.hcxt = PathmanStatusCacheContext;

	status_cache = hash_create(PATHMAN_STATUS_CACHE,
							   PART_RELS_SIZE * CHILD_FACTOR, &ctl,
							   HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);

	memset(&ctl, 0, sizeof(ctl));
	ctl.keysize = sizeof(Oid);
	ctl.entrysize = sizeof(PartBoundInfo);
	ctl.hcxt = PathmanBoundsCacheContext;

	bounds_cache = hash_create(PATHMAN_BOUNDS_CACHE,
							   PART_RELS_SIZE * CHILD_FACTOR, &ctl,
							   HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
}

/*
 * Safely free per-process resources.
 */
static void
fini_local_cache(void)
{
	/* First, destroy hash tables */
	hash_destroy(parents_cache);
	hash_destroy(status_cache);
	hash_destroy(bounds_cache);

	parents_cache	= NULL;
	status_cache	= NULL;
	bounds_cache	= NULL;

	if (prel_resowner != NULL)
	{
		hash_destroy(prel_resowner);
		prel_resowner = NULL;
	}

	/* Now we can clear allocations */
	if (TopPathmanContext)
	{
		MemoryContextReset(PathmanParentsCacheContext);
		MemoryContextReset(PathmanStatusCacheContext);
		MemoryContextReset(PathmanBoundsCacheContext);
	}
}


/*
 * find_inheritance_children
 *
 * Returns an array containing the OIDs of all relations which
 * inherit *directly* from the relation with OID 'parent_relid'.
 *
 * The specified lock type is acquired on each child relation (but not on the
 * given rel; caller should already have locked it).  If lockmode is NoLock
 * then no locks are acquired, but caller must beware of race conditions
 * against possible DROPs of child relations.
 *
 * borrowed from pg_inherits.c
 */
find_children_status
find_inheritance_children_array(Oid parent_relid,
								LOCKMODE lockmode,
								bool nowait,
								uint32 *children_size,	/* ret value #1 */
								Oid **children)			/* ret value #2 */
{
	Relation	relation;
	SysScanDesc scan;
	ScanKeyData key[1];
	HeapTuple	inheritsTuple;

	Oid		   *oidarr;
	uint32		maxoids,
				numoids;

	Oid		   *result = NULL;
	uint32		nresult = 0;

	uint32		i;

	/* Init safe return values */
	*children_size = 0;
	*children = NULL;

	/*
	 * Can skip the scan if pg_class shows the
	 * relation has never had a subclass.
	 */
	if (!has_subclass(parent_relid))
		return FCS_NO_CHILDREN;

	/*
	 * Scan pg_inherits and build a working array of subclass OIDs.
	 */
	ArrayAlloc(oidarr, maxoids, numoids, 32);

	relation = heap_open_compat(InheritsRelationId, AccessShareLock);

	ScanKeyInit(&key[0],
				Anum_pg_inherits_inhparent,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(parent_relid));

	scan = systable_beginscan(relation, InheritsParentIndexId, true,
							  NULL, 1, key);

	while ((inheritsTuple = systable_getnext(scan)) != NULL)
	{
		Oid inhrelid;

		inhrelid = ((Form_pg_inherits) GETSTRUCT(inheritsTuple))->inhrelid;
		ArrayPush(oidarr, maxoids, numoids, inhrelid);
	}

	systable_endscan(scan);

	heap_close_compat(relation, AccessShareLock);

	/*
	 * If we found more than one child, sort them by OID.  This ensures
	 * reasonably consistent behavior regardless of the vagaries of an
	 * indexscan.  This is important since we need to be sure all backends
	 * lock children in the same order to avoid needless deadlocks.
	 */
	if (numoids > 1)
		qsort(oidarr, numoids, sizeof(Oid), oid_cmp);

	/* Acquire locks and build the result list */
	for (i = 0; i < numoids; i++)
	{
		Oid inhrelid = oidarr[i];

		if (lockmode != NoLock)
		{
			/* Get the lock to synchronize against concurrent drop */
			if (nowait)
			{
				if (!ConditionalLockRelationOid(inhrelid, lockmode))
				{
					uint32 j;

					/* Unlock all previously locked children */
					for (j = 0; j < i; j++)
						UnlockRelationOid(oidarr[j], lockmode);

					pfree(oidarr);

					/* We couldn't lock this child, retreat! */
					return FCS_COULD_NOT_LOCK;
				}
			}
			else LockRelationOid(inhrelid, lockmode);

			/*
			 * Now that we have the lock, double-check to see if the relation
			 * really exists or not.  If not, assume it was dropped while we
			 * waited to acquire lock, and ignore it.
			 */
			if (!SearchSysCacheExists1(RELOID, ObjectIdGetDatum(inhrelid)))
			{
				/* Release useless lock */
				UnlockRelationOid(inhrelid, lockmode);

				/* And ignore this relation */
				continue;
			}
		}

		/* Alloc array if it's the first time */
		if (nresult == 0)
			result = palloc(numoids * sizeof(Oid));

		/* Save Oid of the existing relation */
		result[nresult++] = inhrelid;
	}

	/* Set return values */
	*children_size = nresult;
	*children = result;

	pfree(oidarr);

	/* Do we have children? */
	return nresult > 0 ? FCS_FOUND : FCS_NO_CHILDREN;
}



/*
 * Generate check constraint name for a partition.
 * NOTE: this function does not perform sanity checks at all.
 */
char *
build_check_constraint_name_relid_internal(Oid relid)
{
	Assert(OidIsValid(relid));
	return build_check_constraint_name_relname_internal(get_rel_name(relid));
}

/*
 * Generate check constraint name for a partition.
 * NOTE: this function does not perform sanity checks at all.
 */
char *
build_check_constraint_name_relname_internal(const char *relname)
{
	Assert(relname != NULL);
	return psprintf("pathman_%s_check", relname);
}

/*
 * Generate part sequence name for a parent.
 * NOTE: this function does not perform sanity checks at all.
 */
char *
build_sequence_name_relid_internal(Oid relid)
{
	Assert(OidIsValid(relid));
	return build_sequence_name_relname_internal(get_rel_name(relid));
}

/*
 * Generate part sequence name for a parent.
 * NOTE: this function does not perform sanity checks at all.
 */
char *
build_sequence_name_relname_internal(const char *relname)
{
	Assert(relname != NULL);
	return psprintf("%s_seq", relname);
}

/*
 * Generate name for update trigger.
 * NOTE: this function does not perform sanity checks at all.
 */
char *
build_update_trigger_name_internal(Oid relid)
{
	Assert(OidIsValid(relid));
	return psprintf("%s_upd_trig", get_rel_name(relid));
}

/*
 * Generate name for update trigger's function.
 * NOTE: this function does not perform sanity checks at all.
 */
char *
build_update_trigger_func_name_internal(Oid relid)
{
	Assert(OidIsValid(relid));
	return psprintf("%s_upd_trig_func", get_rel_name(relid));
}



/*
 * Check that relation 'relid' is partitioned by pg_pathman.
 * Extract tuple into 'values', 'isnull', 'xmin', 'iptr' if they're provided.
 */
bool
pathman_config_contains_relation(Oid relid, Datum *values, bool *isnull,
								 TransactionId *xmin, ItemPointerData* iptr)
{
	Relation		rel;
#if PG_VERSION_NUM >= 120000
	TableScanDesc	scan;
#else
	HeapScanDesc	scan;
#endif
	ScanKeyData		key[1];
	Snapshot		snapshot;
	HeapTuple		htup;
	bool			contains_rel = false;
	TupleDesc		tupleDescr;

	ScanKeyInit(&key[0],
				Anum_pathman_config_partrel,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(relid));

	/* Open PATHMAN_CONFIG with latest snapshot available */
	rel = heap_open_compat(get_pathman_config_relid(false), AccessShareLock);
	tupleDescr = RelationGetDescr(rel);

	/* Check that 'partrel' column is of regclass type */
	Assert(TupleDescAttr(tupleDescr,
				Anum_pathman_config_partrel - 1)->atttypid == REGCLASSOID);

	/* Check that number of columns == Natts_pathman_config */
	Assert(tupleDescr->natts == Natts_pathman_config
			|| tupleDescr->natts == Natts_pathman_config_historic);

	snapshot = RegisterSnapshot(GetLatestSnapshot());
#if PG_VERSION_NUM >= 120000
	scan = table_beginscan(rel, snapshot, 1, key);
#else
	scan = heap_beginscan(rel, snapshot, 1, key);
#endif

	while ((htup = heap_getnext(scan, ForwardScanDirection)) != NULL)
	{
		contains_rel = true; /* found partitioned table */

		/* Extract data if necessary */
		if (values && isnull)
		{
			htup = heap_copytuple(htup);
			heap_deform_tuple(htup, tupleDescr, values, isnull);

			/* Perform checks for non-NULL columns */
			Assert(!isnull[Anum_pathman_config_partrel - 1]);
			Assert(!isnull[Anum_pathman_config_expr - 1]);
			Assert(!isnull[Anum_pathman_config_parttype - 1]);
		}

		/* Set xmin if necessary */
		if (xmin)
			*xmin = HeapTupleGetXminCompat(htup);

		/* Set ItemPointer if necessary */
		if (iptr)
			*iptr = htup->t_self; /* FIXME: callers should lock table beforehand */
	}

	/* Clean resources */
#if PG_VERSION_NUM >= 120000
	table_endscan(scan);
#else
	heap_endscan(scan);
#endif
	UnregisterSnapshot(snapshot);
	heap_close_compat(rel, AccessShareLock);

	elog(DEBUG2, "PATHMAN_CONFIG %s relation %u",
		 (contains_rel ? "contains" : "doesn't contain"), relid);

	return contains_rel;
}

/*
 * Loads additional pathman parameters like 'enable_parent'
 * or 'auto' from PATHMAN_CONFIG_PARAMS.
 */
bool
read_pathman_params(Oid relid, Datum *values, bool *isnull)
{
	Relation		rel;
#if PG_VERSION_NUM >= 120000
	TableScanDesc	scan;
#else
	HeapScanDesc	scan;
#endif
	ScanKeyData		key[1];
	Snapshot		snapshot;
	HeapTuple		htup;
	bool			row_found = false;

	ScanKeyInit(&key[0],
				Anum_pathman_config_params_partrel,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(relid));

	rel = heap_open_compat(get_pathman_config_params_relid(false), AccessShareLock);
	snapshot = RegisterSnapshot(GetLatestSnapshot());
#if PG_VERSION_NUM >= 120000
	scan = table_beginscan(rel, snapshot, 1, key);
#else
	scan = heap_beginscan(rel, snapshot, 1, key);
#endif

	/* There should be just 1 row */
	if ((htup = heap_getnext(scan, ForwardScanDirection)) != NULL)
	{
		/* Extract data if necessary */
		htup = heap_copytuple(htup);
		heap_deform_tuple(htup, RelationGetDescr(rel), values, isnull);
		row_found = true;

		/* Perform checks for non-NULL columns */
		Assert(!isnull[Anum_pathman_config_params_partrel - 1]);
		Assert(!isnull[Anum_pathman_config_params_enable_parent - 1]);
		Assert(!isnull[Anum_pathman_config_params_auto - 1]);
		Assert(!isnull[Anum_pathman_config_params_spawn_using_bgw - 1]);
	}

	/* Clean resources */
#if PG_VERSION_NUM >= 120000
	table_endscan(scan);
#else
	heap_endscan(scan);
#endif
	UnregisterSnapshot(snapshot);
	heap_close_compat(rel, AccessShareLock);

	return row_found;
}


/*
 * Validates range constraint. It MUST have one of the following formats:
 *		1) EXPRESSION >= CONST AND EXPRESSION < CONST
 *		2) EXPRESSION >= CONST
 *		3) EXPRESSION < CONST
 *
 * Writes 'lower' & 'upper' and 'lower_null' & 'upper_null' values on success.
 */
bool
validate_range_constraint(const Expr *expr,
						  const PartRelationInfo *prel,
						  Datum *lower, Datum *upper,
						  bool *lower_null, bool *upper_null)
{
	const TypeCacheEntry *tce;

	if (!expr)
		return false;

	/* Set default values */
	*lower_null = *upper_null = true;

	/* Find type cache entry for partitioned expression type */
	tce = lookup_type_cache(prel->ev_type, TYPECACHE_BTREE_OPFAMILY);

	/* Is it an AND clause? */
	if (is_andclause_compat((Node *) expr))
	{
		const BoolExpr *boolexpr = (const BoolExpr *) expr;
		ListCell	   *lc;

		/* Walk through boolexpr's args */
		foreach (lc, boolexpr->args)
		{
			const OpExpr *opexpr = (const OpExpr *) lfirst(lc);

			/* Exit immediately if something is wrong */
			if (!validate_range_opexpr((const Expr *) opexpr, prel, tce,
									   lower, upper, lower_null, upper_null))
				return false;
		}

		/* Everything seems to be fine */
		return true;
	}

	/* It might be just an OpExpr clause */
	else return validate_range_opexpr(expr, prel, tce,
									  lower, upper, lower_null, upper_null);
}

/*
 * Validates a single expression of kind:
 *		1) EXPRESSION >= CONST
 *		2) EXPRESSION < CONST
 */
static bool
validate_range_opexpr(const Expr *expr,
					  const PartRelationInfo *prel,
					  const TypeCacheEntry *tce,
					  Datum *lower, Datum *upper,
					  bool *lower_null, bool *upper_null)
{
	const OpExpr   *opexpr;
	Datum			val;

	if (!expr)
		return false;

	/* Fail fast if it's not an OpExpr node */
	if (!IsA(expr, OpExpr))
		return false;

	/* Perform cast */
	opexpr = (const OpExpr *) expr;

	/* Try reading Const value */
	if (!read_opexpr_const(opexpr, prel, &val))
		return false;

	/* Examine the strategy (expect '>=' OR '<') */
	switch (get_op_opfamily_strategy(opexpr->opno, tce->btree_opf))
	{
		case BTGreaterEqualStrategyNumber:
			{
				/* Bound already exists */
				if (*lower_null == false)
					return false;

				*lower_null = false;
				*lower = val;

				return true;
			}

		case BTLessStrategyNumber:
			{
				/* Bound already exists */
				if (*upper_null == false)
					return false;

				*upper_null = false;
				*upper = val;

				return true;
			}

		default:
			return false;
	}
}

/*
 * Reads const value from expressions of kind:
 *		1) EXPRESSION >= CONST
 *		2) EXPRESSION < CONST
 */
static bool
read_opexpr_const(const OpExpr *opexpr,
				  const PartRelationInfo *prel,
				  Datum *value)
{
	const Node	   *right;
	const Const	   *boundary;
	bool			cast_success;

	/* There should be exactly 2 args */
	if (list_length(opexpr->args) != 2)
		return false;

	/* Fetch args of expression */
	right = lsecond(opexpr->args);

	/* Examine RIGHT argument */
	switch (nodeTag(right))
	{
		case T_FuncExpr:
			{
				FuncExpr   *func_expr = (FuncExpr *) right;
				Const	   *constant;

				/* This node should represent a type cast */
				if (func_expr->funcformat != COERCE_EXPLICIT_CAST &&
					func_expr->funcformat != COERCE_IMPLICIT_CAST)
					return false;

				/* This node should have exactly 1 argument */
				if (list_length(func_expr->args) != 1)
					return false;

				/* Extract single argument */
				constant = linitial(func_expr->args);

				/* Argument should be a Const */
				if (!IsA(constant, Const))
					return false;

				/* Update RIGHT */
				right = (Node *) constant;
			}
			/* FALLTHROUGH */

		case T_Const:
			{
				boundary = (Const *) right;

				/* CONST is NOT NULL */
				if (boundary->constisnull)
					return false;
			}
			break;

		default:
			return false;
	}

	/* Cast Const to a proper type if needed */
	*value = perform_type_cast(boundary->constvalue,
							   getBaseType(boundary->consttype),
							   getBaseType(prel->ev_type),
							   &cast_success);

	if (!cast_success)
	{
		elog(WARNING, "constant type in some check constraint "
					  "does not match the partitioned column's type");

		return false;
	}

	return true;
}

/*
 * Validate hash constraint. It MUST have this exact format:
 *
 *		get_hash_part_idx(TYPE_HASH_PROC(VALUE), PARTITIONS_COUNT) = CUR_PARTITION_IDX
 *
 * Writes 'part_idx' hash value for this partition on success.
 */
bool
validate_hash_constraint(const Expr *expr,
						 const PartRelationInfo *prel,
						 uint32 *part_idx)
{
	const TypeCacheEntry   *tce;
	const OpExpr		   *eq_expr;
	const FuncExpr		   *get_hash_expr,
						   *type_hash_proc_expr;

	if (!expr)
		return false;

	if (!IsA(expr, OpExpr))
		return false;

	eq_expr = (const OpExpr *) expr;

	/* Check that left expression is a function call */
	if (!IsA(linitial(eq_expr->args), FuncExpr))
		return false;

	get_hash_expr = (FuncExpr *) linitial(eq_expr->args); /* get_hash_part_idx(...) */

	/* Is 'eqexpr' an equality operator? */
	tce = lookup_type_cache(get_hash_expr->funcresulttype, TYPECACHE_BTREE_OPFAMILY);
	if (BTEqualStrategyNumber != get_op_opfamily_strategy(eq_expr->opno,
														  tce->btree_opf))
		return false;

	if (list_length(get_hash_expr->args) == 2)
	{
		Node   *first = linitial(get_hash_expr->args);	/* arg #1: TYPE_HASH_PROC(EXPRESSION) */
		Node   *second = lsecond(get_hash_expr->args);	/* arg #2: PARTITIONS_COUNT */
		Const  *cur_partition_idx;						/* hash value for this partition */

		if (!IsA(first, FuncExpr) || !IsA(second, Const))
			return false;

		type_hash_proc_expr = (FuncExpr *) first;

		/* Check that function is indeed TYPE_HASH_PROC() */
		if (type_hash_proc_expr->funcid != prel->hash_proc)
			return false;

		/* There should be exactly 1 argument */
		if (list_length(type_hash_proc_expr->args) != 1)
			return false;

		/* Check that PARTITIONS_COUNT is equal to total amount of partitions */
		if (DatumGetUInt32(((Const *) second)->constvalue) != PrelChildrenCount(prel))
			return false;

		/* Check that CUR_PARTITION_HASH is Const */
		if (!IsA(lsecond(eq_expr->args), Const))
			return false;

		/* Fetch CUR_PARTITION_IDX */
		cur_partition_idx = lsecond(eq_expr->args);

		/* Check that CUR_PARTITION_HASH is NOT NULL */
		if (cur_partition_idx->constisnull)
			return false;

		*part_idx = DatumGetUInt32(cur_partition_idx->constvalue);
		if (*part_idx >= PrelChildrenCount(prel))
			return false;

		return true; /* everything seems to be ok */
	}

	return false;
}


/* Parse cstring and build uint32 representing the version */
static uint32
build_semver_uint32(char *version_cstr)
{
	uint32	version = 0;
	bool	expect_num_token = false;
	long	max_dots = 2;
	char   *pos = version_cstr;

	while (*pos)
	{
		/* Invert expected token type */
		expect_num_token = !expect_num_token;

		if (expect_num_token)
		{
			char   *end_pos;
			long	num;
			long	i;

			/* Parse number */
			num = strtol(pos, &end_pos, 10);

			if (pos == end_pos || num > 99 || num < 0)
				goto version_error;

			for (i = 0; i < max_dots; i++)
				num *= 100;

			version += num;

			/* Move position */
			pos = end_pos;
		}
		else
		{
			/* Expect to see less dots */
			max_dots--;

			if (*pos != '.' || max_dots < 0)
				goto version_error;

			/* Move position */
			pos++;
		}
	}

	if (!expect_num_token)
		goto version_error;

	return version;

version_error:
	DisablePathman(); /* disable pg_pathman since config is broken */
	ereport(ERROR, (errmsg("wrong version: \"%s\"", version_cstr),
					errhint(INIT_ERROR_HINT)));
	return 0; /* keep compiler happy */
}

/* Get version of pg_pathman's facade written in Pl/PgSQL */
static uint32
get_plpgsql_frontend_version(void)
{
	Relation		pg_extension_rel;
	ScanKeyData		skey;
	SysScanDesc		scan;
	HeapTuple		htup;

	Datum			datum;
	bool			isnull;
	char		   *version_cstr;

	/* Look up the extension */
	pg_extension_rel = heap_open_compat(ExtensionRelationId, AccessShareLock);

	ScanKeyInit(&skey,
				Anum_pg_extension_extname,
				BTEqualStrategyNumber, F_NAMEEQ,
				CStringGetDatum("pg_pathman"));

	scan = systable_beginscan(pg_extension_rel,
							  ExtensionNameIndexId,
							  true, NULL, 1, &skey);

	htup = systable_getnext(scan);

	/* Exit if pg_pathman's missing */
	if (!HeapTupleIsValid(htup))
		return 0;

	datum = heap_getattr(htup, Anum_pg_extension_extversion,
						 RelationGetDescr(pg_extension_rel), &isnull);
	Assert(isnull == false); /* extversion should not be NULL */

	/* Extract pg_pathman's version as cstring */
	version_cstr = text_to_cstring(DatumGetTextPP(datum));

	systable_endscan(scan);
	heap_close_compat(pg_extension_rel, AccessShareLock);

	return build_semver_uint32(version_cstr);
}

/* Check that current Pl/PgSQL facade is compatible with internals */
static void
validate_plpgsql_frontend_version(uint32 current_ver, uint32 compatible_ver)
{
	Assert(current_ver > 0);
	Assert(compatible_ver > 0);

	/* Compare ver to 'lowest compatible frontend' version */
	if (current_ver < compatible_ver)
	{
		elog(DEBUG1, "current version: %x, lowest compatible: %x",
					 current_ver, compatible_ver);

		DisablePathman(); /* disable pg_pathman since config is broken */
		ereport(ERROR,
				(errmsg("pg_pathman's Pl/PgSQL frontend is incompatible with "
						"its shared library"),
				 errdetail("consider performing an update procedure"),
				 errhint(INIT_ERROR_HINT)));
	}
}
