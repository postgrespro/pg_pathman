/* ------------------------------------------------------------------------
 *
 * init.c
 *		Initialization functions
 *
 * Copyright (c) 2015-2016, Postgres Professional
 *
 * Portions Copyright (c) 1996-2015, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * ------------------------------------------------------------------------
 */

#include "hooks.h"
#include "init.h"
#include "pathman.h"
#include "pathman_workers.h"
#include "relation_info.h"
#include "utils.h"

#include "access/htup_details.h"
#include "access/sysattr.h"
#include "catalog/indexing.h"
#include "catalog/pg_constraint.h"
#include "catalog/pg_inherits.h"
#include "catalog/pg_inherits_fn.h"
#include "catalog/pg_type.h"
#include "miscadmin.h"
#include "optimizer/clauses.h"
#include "parser/parse_coerce.h"
#include "utils/datum.h"
#include "utils/inval.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/memutils.h"
#include "utils/lsyscache.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"
#include "utils/typcache.h"

#if PG_VERSION_NUM >= 90600
#include "catalog/pg_constraint_fn.h"
#endif


/* Help user in case of emergency */
#define INIT_ERROR_HINT "pg_pathman will be disabled to allow you to resolve this issue"

/* Initial size of 'partitioned_rels' table */
#define PART_RELS_SIZE	10
#define CHILD_FACTOR	500


/* Storage for PartRelationInfos */
HTAB			   *partitioned_rels = NULL;

/* Storage for PartParentInfos */
HTAB			   *parent_cache = NULL;

/* pg_pathman's init status */
PathmanInitState 	pg_pathman_init_state;

/* Shall we install new relcache callback? */
static bool			relcache_callback_needed = true;

/* Functions for various local caches */
static bool init_pathman_relation_oids(void);
static void fini_pathman_relation_oids(void);
static void init_local_cache(void);
static void fini_local_cache(void);
static void read_pathman_config(void);

static Expr *get_partition_constraint_expr(Oid partition, AttrNumber part_attno);

static int cmp_range_entries(const void *p1, const void *p2, void *arg);

static bool validate_range_constraint(const Expr *expr,
									  const PartRelationInfo *prel,
									  Datum *min,
									  Datum *max);

static bool validate_hash_constraint(const Expr *expr,
									 const PartRelationInfo *prel,
									 uint32 *part_hash);

static bool read_opexpr_const(const OpExpr *opexpr,
							  const PartRelationInfo *prel,
							  Datum *val);

static int oid_cmp(const void *p1, const void *p2);


/*
 * Save and restore main init state.
 */

void
save_pathman_init_state(PathmanInitState *temp_init_state)
{
	*temp_init_state = pg_pathman_init_state;
}

void
restore_pathman_init_state(const PathmanInitState *temp_init_state)
{
	pg_pathman_init_state = *temp_init_state;
}

/*
 * Create main GUCs.
 */
void
init_main_pathman_toggles(void)
{
	/* Main toggle, load_config() will enable it */
	DefineCustomBoolVariable("pg_pathman.enable",
							 "Enables pg_pathman's optimizations during the planner stage",
							 NULL,
							 &pg_pathman_init_state.pg_pathman_enable,
							 true,
							 PGC_USERSET,
							 0,
							 NULL,
							 pg_pathman_enable_assign_hook,
							 NULL);

	/* Global toggle for automatic partition creation */
	DefineCustomBoolVariable("pg_pathman.enable_auto_partition",
							 "Enables automatic partition creation",
							 NULL,
							 &pg_pathman_init_state.auto_partition,
							 true,
							 PGC_USERSET,
							 0,
							 NULL,
							 NULL,
							 NULL);

	/* Global toggle for COPY stmt handling */
	DefineCustomBoolVariable("pg_pathman.override_copy",
							 "Override COPY statement handling",
							 NULL,
							 &pg_pathman_init_state.override_copy,
							 true,
							 PGC_USERSET,
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

	init_local_cache();		/* create 'partitioned_rels' hash table */
	read_pathman_config();	/* read PATHMAN_CONFIG table & fill cache */

	/* Register pathman_relcache_hook(), currently we can't unregister it */
	if (relcache_callback_needed)
	{
		CacheRegisterRelcacheCallback(pathman_relcache_hook, PointerGetDatum(NULL));
		relcache_callback_needed = false;
	}

	/* Mark pg_pathman as initialized */
	pg_pathman_init_state.initialization_needed = false;

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
	pg_pathman_init_state.initialization_needed = true;

	elog(DEBUG2, "pg_pathman's config has been unloaded successfully [%u]", MyProcPid);
}

/*
 * Estimate total amount of shmem needed for pg_pathman to run.
 */
Size
estimate_pathman_shmem_size(void)
{
	return estimate_concurrent_part_task_slots_size() +
		   MAXALIGN(sizeof(PathmanState));
}

/*
 * Cache *all* important pg_pathman's relids at once.
 * We should NOT rely on any previously cached values.
 */
static bool
init_pathman_relation_oids(void)
{
	Oid schema = get_pathman_schema();
	Assert(schema != InvalidOid);

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

	memset(&ctl, 0, sizeof(ctl));
	ctl.keysize = sizeof(Oid);
	ctl.entrysize = sizeof(PartRelationInfo);
	ctl.hcxt = TopMemoryContext; /* place data to persistent mcxt */

	partitioned_rels = hash_create("pg_pathman's partitioned relations cache",
								   PART_RELS_SIZE, &ctl, HASH_ELEM | HASH_BLOBS);

	memset(&ctl, 0, sizeof(ctl));
	ctl.keysize = sizeof(Oid);
	ctl.entrysize = sizeof(PartParentInfo);
	ctl.hcxt = TopMemoryContext; /* place data to persistent mcxt */

	parent_cache = hash_create("pg_pathman's partition parents cache",
							   PART_RELS_SIZE * CHILD_FACTOR,
							   &ctl, HASH_ELEM | HASH_BLOBS);
}

/*
 * Safely free per-process resources.
 */
static void
fini_local_cache(void)
{
	HASH_SEQ_STATUS		status;
	PartRelationInfo   *prel;

	hash_seq_init(&status, partitioned_rels);
	while((prel = (PartRelationInfo *) hash_seq_search(&status)) != NULL)
	{
		if (PrelIsValid(prel))
		{
			FreeChildrenArray(prel);
			FreeRangesArray(prel);
		}
	}

	/* Now we can safely destroy hash tables */
	hash_destroy(partitioned_rels);
	hash_destroy(parent_cache);
	partitioned_rels = NULL;
	parent_cache = NULL;
}

/*
 * Initializes pg_pathman's global state (PathmanState) & locks.
 */
void
init_shmem_config(void)
{
	bool found;

	/* Check if module was initialized in postmaster */
	pmstate = ShmemInitStruct("pg_pathman's global state",
							  sizeof(PathmanState), &found);
	if (!found)
	{
		/*
		 * Initialize locks in postmaster
		 */
		if (!IsUnderPostmaster)
		{
			/* NOTE: dsm_array is redundant, hence the commented code */
			/* pmstate->dsm_init_lock = LWLockAssign(); */
		}
	}

	/* Allocate some space for concurrent part slots */
	init_concurrent_part_task_slots();
}

/*
 * Fill PartRelationInfo with partition-related info.
 */
void
fill_prel_with_partitions(const Oid *partitions,
						  const uint32 parts_count,
						  PartRelationInfo *prel)
{
	uint32			i;
	Expr		   *con_expr;
	MemoryContext	mcxt = TopMemoryContext;

	/* Allocate memory for 'prel->children' & 'prel->ranges' (if needed) */
	prel->children = MemoryContextAllocZero(mcxt, parts_count * sizeof(Oid));
	if (prel->parttype == PT_RANGE)
		prel->ranges = MemoryContextAllocZero(mcxt, parts_count * sizeof(RangeEntry));
	prel->children_count = parts_count;

	for (i = 0; i < PrelChildrenCount(prel); i++)
	{
		con_expr = get_partition_constraint_expr(partitions[i], prel->attnum);

		/* Perform a partitioning_type-dependent task */
		switch (prel->parttype)
		{
			case PT_HASH:
				{
					uint32	hash; /* hash value < parts_count */

					if (validate_hash_constraint(con_expr, prel, &hash))
						prel->children[hash] = partitions[i];
					else
					{
						DisablePathman(); /* disable pg_pathman since config is broken */
						ereport(ERROR,
								(errmsg("Wrong constraint format for HASH partition \"%s\"",
										get_rel_name_or_relid(partitions[i])),
								 errhint(INIT_ERROR_HINT)));
					}
				}
				break;

			case PT_RANGE:
				{
					Datum	range_min, range_max;

					if (validate_range_constraint(con_expr, prel,
												  &range_min, &range_max))
					{
						prel->ranges[i].child_oid	= partitions[i];
						prel->ranges[i].min			= range_min;
						prel->ranges[i].max			= range_max;
					}
					else
					{
						DisablePathman(); /* disable pg_pathman since config is broken */
						ereport(ERROR,
								(errmsg("Wrong constraint format for RANGE partition \"%s\"",
										get_rel_name_or_relid(partitions[i])),
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
		}
	}

	/* Finalize 'prel' for a RANGE-partitioned table */
	if (prel->parttype == PT_RANGE)
	{
		MemoryContext	old_mcxt;

		/* Sort partitions by RangeEntry->min asc */
		qsort_arg((void *) prel->ranges, PrelChildrenCount(prel),
				  sizeof(RangeEntry), cmp_range_entries,
				  (void *) &prel->cmp_proc);

		/* Initialize 'prel->children' array */
		for (i = 0; i < PrelChildrenCount(prel); i++)
			prel->children[i] = prel->ranges[i].child_oid;

		/* Copy all min & max Datums to the persistent mcxt */
		old_mcxt = MemoryContextSwitchTo(TopMemoryContext);
		for (i = 0; i < PrelChildrenCount(prel); i++)
		{
			prel->ranges[i].max = datumCopy(prel->ranges[i].max,
											prel->attbyval,
											prel->attlen);

			prel->ranges[i].min = datumCopy(prel->ranges[i].min,
											prel->attbyval,
											prel->attlen);
		}
		MemoryContextSwitchTo(old_mcxt);

	}

#ifdef USE_ASSERT_CHECKING
	/* Check that each partition Oid has been assigned properly */
	if (prel->parttype == PT_HASH)
		for (i = 0; i < PrelChildrenCount(prel); i++)
		{
			if (prel->children[i] == InvalidOid)
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
 * find_inheritance_children
 *
 * Returns an array containing the OIDs of all relations which
 * inherit *directly* from the relation with OID 'parentrelId'.
 *
 * The specified lock type is acquired on each child relation (but not on the
 * given rel; caller should already have locked it).  If lockmode is NoLock
 * then no locks are acquired, but caller must beware of race conditions
 * against possible DROPs of child relations.
 *
 * borrowed from pg_inherits.c
 */
Oid *
find_inheritance_children_array(Oid parentrelId, LOCKMODE lockmode, uint32 *size)
{
	Relation	relation;
	SysScanDesc scan;
	ScanKeyData key[1];
	HeapTuple	inheritsTuple;
	Oid			inhrelid;
	Oid		   *oidarr;
	uint32		maxoids,
				numoids,
				i;

	/*
	 * Can skip the scan if pg_class shows the relation has never had a
	 * subclass.
	 */
	if (!has_subclass(parentrelId))
	{
		*size = 0;
		return NULL;
	}

	/*
	 * Scan pg_inherits and build a working array of subclass OIDs.
	 */
	maxoids = 32;
	oidarr = (Oid *) palloc(maxoids * sizeof(Oid));
	numoids = 0;

	relation = heap_open(InheritsRelationId, AccessShareLock);

	ScanKeyInit(&key[0],
				Anum_pg_inherits_inhparent,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(parentrelId));

	scan = systable_beginscan(relation, InheritsParentIndexId, true,
							  NULL, 1, key);

	while ((inheritsTuple = systable_getnext(scan)) != NULL)
	{
		inhrelid = ((Form_pg_inherits) GETSTRUCT(inheritsTuple))->inhrelid;
		if (numoids >= maxoids)
		{
			maxoids *= 2;
			oidarr = (Oid *) repalloc(oidarr, maxoids * sizeof(Oid));
		}
		oidarr[numoids++] = inhrelid;
	}

	systable_endscan(scan);

	heap_close(relation, AccessShareLock);

	/*
	 * If we found more than one child, sort them by OID.  This ensures
	 * reasonably consistent behavior regardless of the vagaries of an
	 * indexscan.  This is important since we need to be sure all backends
	 * lock children in the same order to avoid needless deadlocks.
	 */
	if (numoids > 1)
		qsort(oidarr, numoids, sizeof(Oid), oid_cmp);

	/*
	 * Acquire locks and build the result list.
	 */
	for (i = 0; i < numoids; i++)
	{
		inhrelid = oidarr[i];

		if (lockmode != NoLock)
		{
			/* Get the lock to synchronize against concurrent drop */
			LockRelationOid(inhrelid, lockmode);

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
	}

	*size = numoids;
	return oidarr;
}

/*
 * Generate check constraint name for a partition.
 *
 * This function does not perform sanity checks at all.
 */
char *
build_check_constraint_name_internal(Oid relid, AttrNumber attno)
{
	return psprintf("pathman_%s_%u_check", get_rel_name(relid), attno);
}

/*
 * Check that relation 'relid' is partitioned by pg_pathman.
 *
 * Extract tuple into 'values' and 'isnull' if they're provided.
 */
bool
pathman_config_contains_relation(Oid relid, Datum *values, bool *isnull,
								 TransactionId *xmin)
{
	Relation		rel;
	HeapScanDesc	scan;
	ScanKeyData		key[1];
	Snapshot		snapshot;
	HeapTuple		htup;
	bool			contains_rel = false;

	ScanKeyInit(&key[0],
				Anum_pathman_config_partrel,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(relid));

	/* Open PATHMAN_CONFIG with latest snapshot available */
	rel = heap_open(get_pathman_config_relid(), AccessShareLock);

	/* Check that 'partrel' column is if regclass type */
	Assert(RelationGetDescr(rel)->
		   attrs[Anum_pathman_config_partrel - 1]->
		   atttypid == REGCLASSOID);

	/* Check that number of columns == Natts_pathman_config */
	Assert(RelationGetDescr(rel)->natts == Natts_pathman_config);

	snapshot = RegisterSnapshot(GetLatestSnapshot());
	scan = heap_beginscan(rel, snapshot, 1, key);

	while ((htup = heap_getnext(scan, ForwardScanDirection)) != NULL)
	{
		contains_rel = true; /* found partitioned table */

		/* Extract data if necessary */
		if (values && isnull)
		{
			heap_deform_tuple(htup, RelationGetDescr(rel), values, isnull);

			/* Perform checks for non-NULL columns */
			Assert(!isnull[Anum_pathman_config_partrel - 1]);
			Assert(!isnull[Anum_pathman_config_attname - 1]);
			Assert(!isnull[Anum_pathman_config_parttype - 1]);
		}

		/* Set xmin if necessary */
		if (xmin)
		{
			Datum	value;
			bool	isnull;

			value = heap_getsysattr(htup,
									MinTransactionIdAttributeNumber,
									RelationGetDescr(rel),
									&isnull);

			Assert(!isnull);
			*xmin = DatumGetTransactionId(value);
		}
	}

	/* Clean resources */
	heap_endscan(scan);
	UnregisterSnapshot(snapshot);
	heap_close(rel, AccessShareLock);

	elog(DEBUG2, "PATHMAN_CONFIG table %s relation %u",
		 (contains_rel ? "contains" : "doesn't contain"), relid);

	return contains_rel;
}

/*
 * Loads additional pathman parameters like 'enable_parent' or 'auto'
 * from PATHMAN_CONFIG_PARAMS
 */
bool
read_pathman_params(Oid relid, Datum *values, bool *isnull)
{
	Relation		rel;
	HeapScanDesc	scan;
	ScanKeyData		key[1];
	Snapshot		snapshot;
	HeapTuple		htup;
	bool			row_found = false;

	ScanKeyInit(&key[0],
				Anum_pathman_config_params_partrel,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(relid));

	rel = heap_open(get_pathman_config_params_relid(), AccessShareLock);
	snapshot = RegisterSnapshot(GetLatestSnapshot());
	scan = heap_beginscan(rel, snapshot, 1, key);

	/* There should be just 1 row */
	if ((htup = heap_getnext(scan, ForwardScanDirection)) != NULL)
	{
		/* Extract data if necessary */
		heap_deform_tuple(htup, RelationGetDescr(rel), values, isnull);
		row_found = true;

		/* Perform checks for non-NULL columns */
		Assert(!isnull[Anum_pathman_config_params_partrel - 1]);
		Assert(!isnull[Anum_pathman_config_params_enable_parent - 1]);
		Assert(!isnull[Anum_pathman_config_params_auto - 1]);
		Assert(!isnull[Anum_pathman_config_params_init_callback - 1]);
	}

	/* Clean resources */
	heap_endscan(scan);
	UnregisterSnapshot(snapshot);
	heap_close(rel, AccessShareLock);

	return row_found;
}

/*
 * Go through the PATHMAN_CONFIG table and create PartRelationInfo entries.
 */
static void
read_pathman_config(void)
{
	Relation		rel;
	HeapScanDesc	scan;
	Snapshot		snapshot;
	HeapTuple		htup;

	/* Open PATHMAN_CONFIG with latest snapshot available */
	rel = heap_open(get_pathman_config_relid(), AccessShareLock);

	/* Check that 'partrel' column is if regclass type */
	Assert(RelationGetDescr(rel)->
		   attrs[Anum_pathman_config_partrel - 1]->
		   atttypid == REGCLASSOID);

	/* Check that number of columns == Natts_pathman_config */
	Assert(RelationGetDescr(rel)->natts == Natts_pathman_config);

	snapshot = RegisterSnapshot(GetLatestSnapshot());
	scan = heap_beginscan(rel, snapshot, 0, NULL);

	/* Examine each row and create a PartRelationInfo in local cache */
	while((htup = heap_getnext(scan, ForwardScanDirection)) != NULL)
	{
		Datum		values[Natts_pathman_config];
		bool		isnull[Natts_pathman_config];
		Oid			relid;		/* partitioned table */
		PartType	parttype;	/* partitioning type */
		text	   *attname;	/* partitioned column name */

		/* Extract Datums from tuple 'htup' */
		heap_deform_tuple(htup, RelationGetDescr(rel), values, isnull);

		/* These attributes are marked as NOT NULL, check anyway */
		Assert(!isnull[Anum_pathman_config_partrel - 1]);
		Assert(!isnull[Anum_pathman_config_parttype - 1]);
		Assert(!isnull[Anum_pathman_config_attname - 1]);

		/* Extract values from Datums */
		relid = DatumGetObjectId(values[Anum_pathman_config_partrel - 1]);
		parttype = DatumGetPartType(values[Anum_pathman_config_parttype - 1]);
		attname = DatumGetTextP(values[Anum_pathman_config_attname - 1]);

		/* Check that relation 'relid' exists */
		if (get_rel_type_id(relid) == InvalidOid)
		{
			DisablePathman(); /* disable pg_pathman since config is broken */
			ereport(ERROR,
					(errmsg("Table \"%s\" contains nonexistent relation %u",
							PATHMAN_CONFIG, relid),
					 errhint(INIT_ERROR_HINT)));
		}

		/* Create or update PartRelationInfo for this partitioned table */
		refresh_pathman_relation_info(relid, parttype, text_to_cstring(attname));
	}

	/* Clean resources */
	heap_endscan(scan);
	UnregisterSnapshot(snapshot);
	heap_close(rel, AccessShareLock);
}

/*
 * Get constraint expression tree for a partition.
 *
 * build_check_constraint_name_internal() is used to build conname.
 */
static Expr *
get_partition_constraint_expr(Oid partition, AttrNumber part_attno)
{
	Oid			conid;			/* constraint Oid */
	char	   *conname;		/* constraint name */
	HeapTuple	con_tuple;
	Datum		conbin_datum;
	bool		conbin_isnull;
	Expr	   *expr;			/* expression tree for constraint */

	conname = build_check_constraint_name_internal(partition, part_attno);
	conid = get_relation_constraint_oid(partition, conname, true);
	if (conid == InvalidOid)
	{
		DisablePathman(); /* disable pg_pathman since config is broken */
		ereport(ERROR,
				(errmsg("constraint \"%s\" for partition \"%s\" does not exist",
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
				(errmsg("constraint \"%s\" for partition \"%s\" has NULL conbin",
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

/* qsort comparison function for RangeEntries */
static int
cmp_range_entries(const void *p1, const void *p2, void *arg)
{
	const RangeEntry   *v1 = (const RangeEntry *) p1;
	const RangeEntry   *v2 = (const RangeEntry *) p2;

	Oid					cmp_proc_oid = *(Oid *) arg;

	return OidFunctionCall2(cmp_proc_oid, v1->min, v2->min);
}

/*
 * Validates range constraint. It MUST have this exact format:
 *
 *		VARIABLE >= CONST AND VARIABLE < CONST
 *
 * Writes 'min' & 'max' values on success.
 */
static bool
validate_range_constraint(const Expr *expr,
						  const PartRelationInfo *prel,
						  Datum *min,
						  Datum *max)
{
	const TypeCacheEntry   *tce;
	const BoolExpr		   *boolexpr = (const BoolExpr *) expr;
	const OpExpr		   *opexpr;

	if (!expr)
		return false;

	/* it should be an AND operator on top */
	if (!and_clause((Node *) expr))
		return false;

	tce = lookup_type_cache(prel->atttype, TYPECACHE_BTREE_OPFAMILY);

	/* check that left operand is >= operator */
	opexpr = (OpExpr *) linitial(boolexpr->args);
	if (BTGreaterEqualStrategyNumber == get_op_opfamily_strategy(opexpr->opno,
																 tce->btree_opf))
	{
		if (!read_opexpr_const(opexpr, prel, min))
			return false;
	}
	else
		return false;

	/* check that right operand is < operator */
	opexpr = (OpExpr *) lsecond(boolexpr->args);
	if (BTLessStrategyNumber == get_op_opfamily_strategy(opexpr->opno,
														 tce->btree_opf))
	{
		if (!read_opexpr_const(opexpr, prel, max))
			return false;
	}
	else
		return false;

	return true;
}

/*
 * Reads const value from expressions of kind:
 *		1) VAR >= CONST OR VAR < CONST
 *		2) RELABELTYPE(VAR) >= CONST OR RELABELTYPE(VAR) < CONST
 */
static bool
read_opexpr_const(const OpExpr *opexpr,
				  const PartRelationInfo *prel,
				  Datum *val)
{
	const Node	   *left;
	const Node	   *right;
	const Var	   *part_attr;	/* partitioned column */
	const Const	   *constant;

	if (list_length(opexpr->args) != 2)
		return false;

	left = linitial(opexpr->args);
	right = lsecond(opexpr->args);

	/* VAR is a part of RelabelType node */
	if (IsA(left, RelabelType) && IsA(right, Const))
	{
		Var *var = (Var *) ((RelabelType *) left)->arg;

		if (IsA(var, Var))
			part_attr = var;
		else
			return false;
	}
	/* left arg is of type VAR */
	else if (IsA(left, Var) && IsA(right, Const))
	{
		part_attr = (Var *) left;
	}
	/* Something is wrong, retreat! */
	else return false;

	/* VAR.attno == partitioned attribute number */
	if (part_attr->varoattno != prel->attnum)
		return false;

	/* CONST is NOT NULL */
	if (((Const *) right)->constisnull)
		return false;

	constant = (Const *) right;

	/* Check that types are binary coercible */
	if (IsBinaryCoercible(constant->consttype, prel->atttype))
	{
		*val = constant->constvalue;
	}
	/* If not, try to perfrom a type cast */
	else
	{
		CoercionPathType	ret;
		Oid					castfunc = InvalidOid;

		ret = find_coercion_pathway(prel->atttype, constant->consttype,
									COERCION_EXPLICIT, &castfunc);

		switch (ret)
		{
			/* There's a function */
			case COERCION_PATH_FUNC:
				{
					/* Perform conversion */
					Assert(castfunc != InvalidOid);
					*val = OidFunctionCall1(castfunc, constant->constvalue);
				}
				break;

			/* Types are binary compatible (no implicit cast) */
			case COERCION_PATH_RELABELTYPE:
				{
					/* We don't perform any checks here */
					*val = constant->constvalue;
				}
				break;

			/* TODO: implement these if needed */
			case COERCION_PATH_ARRAYCOERCE:
			case COERCION_PATH_COERCEVIAIO:

			/* There's no cast available */
			case COERCION_PATH_NONE:
			default:
				{
					elog(WARNING, "Constant type in some check constraint "
								  "does not match the partitioned column's type");
					return false;
				}
		}
	}

	return true;
}

/*
 * Validate hash constraint. It MUST have this exact format:
 *
 *		get_hash_part_idx(TYPE_HASH_PROC(VALUE), PARTITIONS_COUNT) = CUR_PARTITION_HASH
 *
 * Writes 'part_hash' hash value for this partition on success.
 */
static bool
validate_hash_constraint(const Expr *expr,
						 const PartRelationInfo *prel,
						 uint32 *part_hash)
{
	const TypeCacheEntry   *tce;
	const OpExpr		   *eq_expr;
	const FuncExpr		   *get_hash_expr,
						   *type_hash_proc_expr;
	const Var			   *var; /* partitioned column */

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
		Node   *first = linitial(get_hash_expr->args);	/* arg #1: TYPE_HASH_PROC(VALUE) */
		Node   *second = lsecond(get_hash_expr->args);	/* arg #2: PARTITIONS_COUNT */
		Const  *cur_partition_hash;						/* hash value for this partition */

		if (!IsA(first, FuncExpr) || !IsA(second, Const))
			return false;

		type_hash_proc_expr = (FuncExpr *) first;

		/* Check that function is indeed TYPE_HASH_PROC */
		if (type_hash_proc_expr->funcid != prel->hash_proc ||
				!(IsA(linitial(type_hash_proc_expr->args), Var) ||
				  IsA(linitial(type_hash_proc_expr->args), RelabelType)))
		{
			return false;
		}

		/* Extract argument into 'var' */
		if (IsA(linitial(type_hash_proc_expr->args), RelabelType))
			var = (Var *) ((RelabelType *) linitial(type_hash_proc_expr->args))->arg;
		else
			var = (Var *) linitial(type_hash_proc_expr->args);

		/* Check that 'var' is the partitioning key attribute */
		if (var->varoattno != prel->attnum)
			return false;

		/* Check that PARTITIONS_COUNT is equal to total amount of partitions */
		if (DatumGetUInt32(((Const *) second)->constvalue) != PrelChildrenCount(prel))
			return false;

		/* Check that CUR_PARTITION_HASH is Const */
		if (!IsA(lsecond(eq_expr->args), Const))
			return false;

		cur_partition_hash = lsecond(eq_expr->args);

		/* Check that CUR_PARTITION_HASH is NOT NULL */
		if (cur_partition_hash->constisnull)
			return false;

		*part_hash = DatumGetUInt32(cur_partition_hash->constvalue);
		if (*part_hash >= PrelChildrenCount(prel))
			return false;

		return true; /* everything seems to be ok */
	}

	return false;
}

/* needed for find_inheritance_children_array() function */
static int
oid_cmp(const void *p1, const void *p2)
{
	Oid			v1 = *((const Oid *) p1);
	Oid			v2 = *((const Oid *) p2);

	if (v1 < v2)
		return -1;
	if (v1 > v2)
		return 1;
	return 0;
}
