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
#include "init.h"
#include "utils.h"
#include "xact_handling.h"

#include "access/htup_details.h"
#if PG_VERSION_NUM >= 120000
#include "access/genam.h"
#include "access/table.h"
#endif
#include "access/xact.h"
#include "catalog/catalog.h"
#include "catalog/indexing.h"
#include "catalog/pg_constraint.h"
#include "catalog/pg_inherits.h"
#include "catalog/pg_type.h"
#include "miscadmin.h"
#include "nodes/nodeFuncs.h"
#if PG_VERSION_NUM >= 120000
#include "optimizer/optimizer.h"
#else
#include "optimizer/clauses.h"
#include "optimizer/var.h"
#endif
#include "parser/analyze.h"
#include "parser/parser.h"
#include "storage/lmgr.h"
#include "tcop/tcopprot.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/hsearch.h"
#include "utils/inval.h"
#include "utils/memutils.h"
#include "utils/resowner.h"
#include "utils/ruleutils.h"
#include "utils/syscache.h"
#include "utils/lsyscache.h"
#include "utils/typcache.h"

#if PG_VERSION_NUM < 90600
#include "optimizer/planmain.h"
#endif
#if PG_VERSION_NUM < 110000 && PG_VERSION_NUM >= 90600
#include "catalog/pg_constraint_fn.h"
#endif


/* Error messages for partitioning expression */
#define PARSE_PART_EXPR_ERROR	"failed to parse partitioning expression \"%s\""
#define COOK_PART_EXPR_ERROR	"failed to analyze partitioning expression \"%s\""


#ifdef USE_RELINFO_LEAK_TRACKER
#undef get_pathman_relation_info
#undef close_pathman_relation_info

const char	   *prel_resowner_function = NULL;
int				prel_resowner_line = 0;

#define LeakTrackerAdd(prel) \
	do { \
		MemoryContext old_mcxt = MemoryContextSwitchTo((prel)->mcxt); \
		(prel)->owners = \
				list_append_unique( \
						(prel)->owners, \
						list_make2(makeString((char *) prel_resowner_function), \
								   makeInteger(prel_resowner_line))); \
		MemoryContextSwitchTo(old_mcxt); \
		\
		(prel)->access_total++; \
	} while (0)

#define LeakTrackerPrint(prel) \
	do { \
		ListCell *lc; \
		foreach (lc, (prel)->owners) \
		{ \
			char   *fun = strVal(linitial(lfirst(lc))); \
			int		line = intVal(lsecond(lfirst(lc))); \
			elog(WARNING, "PartRelationInfo referenced in %s:%d", fun, line); \
		} \
	} while (0)

#define LeakTrackerFree(prel) \
	do { \
		ListCell *lc; \
		foreach (lc, (prel)->owners) \
		{ \
			list_free_deep(lfirst(lc)); \
		} \
		list_free((prel)->owners); \
		(prel)->owners = NIL; \
	} while (0)
#else
#define LeakTrackerAdd(prel)
#define LeakTrackerPrint(prel)
#define LeakTrackerFree(prel)
#endif


/* Comparison function info */
typedef struct cmp_func_info
{
	FmgrInfo	flinfo;
	Oid			collid;
} cmp_func_info;

typedef struct prel_resowner_info
{
	ResourceOwner	owner;
	List		   *prels;
} prel_resowner_info;


/*
 * For pg_pathman.enable_bounds_cache GUC.
 */
bool			pg_pathman_enable_bounds_cache = true;


/*
 * We delay all invalidation jobs received in relcache hook.
 */
static bool		delayed_shutdown = false; /* pathman was dropped */

/*
 * PartRelationInfo is controlled by ResourceOwner;
 * resowner -> List of controlled PartRelationInfos by this ResourceOwner
 */
HTAB	   *prel_resowner = NULL;


/* Handy wrappers for Oids */
#define bsearch_oid(key, array, array_size) \
	bsearch((const void *) &(key), (array), (array_size), sizeof(Oid), oid_cmp)


static PartRelationInfo *build_pathman_relation_info(Oid relid, Datum *values);
static void free_pathman_relation_info(PartRelationInfo *prel);
static void invalidate_psin_entries_using_relid(Oid relid);
static void invalidate_psin_entry(PartStatusInfo *psin);

static PartRelationInfo *resowner_prel_add(PartRelationInfo *prel);
static PartRelationInfo *resowner_prel_del(PartRelationInfo *prel);
static void resonwner_prel_callback(ResourceReleasePhase phase,
									bool isCommit,
									bool isTopLevel,
									void *arg);

static void fill_prel_with_partitions(PartRelationInfo *prel,
									  const Oid *partitions,
									  const uint32 parts_count);

static void fill_pbin_with_bounds(PartBoundInfo *pbin,
								  const PartRelationInfo *prel,
								  const Expr *constraint_expr);

static int cmp_range_entries(const void *p1, const void *p2, void *arg);

static void forget_bounds_of_partition(Oid partition);

static bool query_contains_subqueries(Node *node, void *context);


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
 * Status cache routines.
 */

/* Invalidate PartStatusInfo for 'relid' */
void
forget_status_of_relation(Oid relid)
{
	PartStatusInfo *psin;
	PartParentInfo *ppar;

	/* Find status cache entry for this relation */
	psin = pathman_cache_search_relid(status_cache,
									  relid, HASH_FIND,
									  NULL);
	if (psin)
		invalidate_psin_entry(psin);

	/*
	 * Find parent of this relation.
	 *
	 * We don't want to use get_parent_of_partition()
	 * since it relies upon the syscache.
	 */
	ppar = pathman_cache_search_relid(parents_cache,
									  relid, HASH_FIND,
									  NULL);

	/* Invalidate parent directly */
	if (ppar)
	{
		/* Find status cache entry for parent */
		psin = pathman_cache_search_relid(status_cache,
										  ppar->parent_relid, HASH_FIND,
										  NULL);
		if (psin)
			invalidate_psin_entry(psin);
	}
	/* Otherwise, look through all entries */
	else invalidate_psin_entries_using_relid(relid);
}

/* Invalidate all PartStatusInfo entries */
void
invalidate_status_cache(void)
{
	invalidate_psin_entries_using_relid(InvalidOid);
}

/* Invalidate PartStatusInfo entry referencing 'relid' */
static void
invalidate_psin_entries_using_relid(Oid relid)
{
	HASH_SEQ_STATUS		status;
	PartStatusInfo	   *psin;

	hash_seq_init(&status, status_cache);

	while ((psin = (PartStatusInfo *) hash_seq_search(&status)) != NULL)
	{
		if (!OidIsValid(relid) ||
			psin->relid == relid ||
			(psin->prel && PrelHasPartition(psin->prel, relid)))
		{
			/* Perform invalidation */
			invalidate_psin_entry(psin);

			/* Exit if exact match */
			if (OidIsValid(relid))
			{
				hash_seq_term(&status);
				break;
			}
		}
	}
}

/* Invalidate single PartStatusInfo entry */
static void
invalidate_psin_entry(PartStatusInfo *psin)
{
#ifdef USE_RELINFO_LOGGING
	elog(DEBUG2, "invalidation message for relation %u [%u]",
		 psin->relid, MyProcPid);
#endif

	if (psin->prel)
	{
		if (PrelReferenceCount(psin->prel) > 0)
		{
			/* Mark entry as outdated and detach it */
			PrelIsFresh(psin->prel) = false;
		}
		else
		{
			free_pathman_relation_info(psin->prel);
		}
	}

	(void) pathman_cache_search_relid(status_cache,
									  psin->relid,
									  HASH_REMOVE,
									  NULL);
}


/*
 * Dispatch cache routines.
 */

/* Close PartRelationInfo entry */
void
close_pathman_relation_info(PartRelationInfo *prel)
{
	AssertArg(prel);

	(void) resowner_prel_del(prel);
}

/* Check if relation is partitioned by pg_pathman */
bool
has_pathman_relation_info(Oid relid)
{
	PartRelationInfo *prel;

	if ((prel = get_pathman_relation_info(relid)) != NULL)
	{
		close_pathman_relation_info(prel);

		return true;
	}

	return false;
}

/* Get PartRelationInfo from local cache */
PartRelationInfo *
get_pathman_relation_info(Oid relid)
{
	PartStatusInfo *psin;

	if (!IsPathmanReady())
		elog(ERROR, "pg_pathman is disabled");

	/* Should always be called in transaction */
	Assert(IsTransactionState());

	/* We don't create entries for catalog */
	if (relid < FirstNormalObjectId)
		return NULL;

	/* Do we know anything about this relation? */
	psin = pathman_cache_search_relid(status_cache,
									  relid, HASH_FIND,
									  NULL);

	if (!psin)
	{
		PartRelationInfo   *prel = NULL;
		ItemPointerData		iptr;
		Datum				values[Natts_pathman_config];
		bool				isnull[Natts_pathman_config];
		bool				found;

		/*
		 * Check if PATHMAN_CONFIG table contains this relation and
		 * build a partitioned table cache entry (might emit ERROR).
		 */
		if (pathman_config_contains_relation(relid, values, isnull, NULL, &iptr))
			prel = build_pathman_relation_info(relid, values);

		/* Create a new entry for this relation */
		psin = pathman_cache_search_relid(status_cache,
										  relid, HASH_ENTER,
										  &found);
		Assert(!found); /* it shouldn't just appear out of thin air */

		/* Cache fresh entry */
		psin->prel = prel;
	}

	/* Check invariants */
	Assert(!psin->prel || PrelIsFresh(psin->prel));

#ifdef USE_RELINFO_LOGGING
	elog(DEBUG2,
		 "fetching %s record for parent %u [%u]",
		 (psin->prel ? "live" : "NULL"), relid, MyProcPid);
#endif

	return resowner_prel_add(psin->prel);
}

/* Build a new PartRelationInfo for partitioned relation */
static PartRelationInfo *
build_pathman_relation_info(Oid relid, Datum *values)
{
	const LOCKMODE		lockmode = AccessShareLock;
	MemoryContext		prel_mcxt;
	PartRelationInfo   *prel;

	AssertTemporaryContext();

	/* Lock parent table */
	LockRelationOid(relid, lockmode);

	/* Check if parent exists */
	if (!SearchSysCacheExists1(RELOID, ObjectIdGetDatum(relid)))
	{
		/* Nope, it doesn't, remove this entry and exit */
		UnlockRelationOid(relid, lockmode);
		return NULL; /* exit */
	}

	/* Create a new memory context to store expression tree etc */
	prel_mcxt = AllocSetContextCreate(PathmanParentsCacheContext,
									  "build_pathman_relation_info",
									  ALLOCSET_SMALL_SIZES);

	/* Create a new PartRelationInfo */
	prel = MemoryContextAllocZero(prel_mcxt, sizeof(PartRelationInfo));
	prel->relid		= relid;
	prel->refcount	= 0;
	prel->fresh		= true;
	prel->mcxt		= prel_mcxt;

	/* Memory leak and cache protection */
	PG_TRY();
	{
		MemoryContext			old_mcxt;
		const TypeCacheEntry   *typcache;
		Datum					param_values[Natts_pathman_config_params];
		bool					param_isnull[Natts_pathman_config_params];
		Oid					   *prel_children;
		uint32					prel_children_count = 0,
								i;

		/* Make both arrays point to NULL */
		prel->children	= NULL;
		prel->ranges	= NULL;

		/* Set partitioning type */
		prel->parttype	= DatumGetPartType(values[Anum_pathman_config_parttype - 1]);

		/* Switch to persistent memory context */
		old_mcxt = MemoryContextSwitchTo(prel->mcxt);

		/* Build partitioning expression tree */
		prel->expr_cstr = TextDatumGetCString(values[Anum_pathman_config_expr - 1]);
		prel->expr = cook_partitioning_expression(relid, prel->expr_cstr, NULL);
		fix_opfuncids(prel->expr);

		/* Extract Vars and varattnos of partitioning expression */
		prel->expr_vars = NIL;
		prel->expr_atts = NULL;
		prel->expr_vars = pull_var_clause_compat(prel->expr, 0, 0);
		pull_varattnos((Node *) prel->expr_vars, PART_EXPR_VARNO, &prel->expr_atts);

		MemoryContextSwitchTo(old_mcxt);

		/* First, fetch type of partitioning expression */
		prel->ev_type	= exprType(prel->expr);
		prel->ev_typmod	= exprTypmod(prel->expr);
		prel->ev_collid = exprCollation(prel->expr);

		/* Fetch HASH & CMP fuctions and other stuff from type cache */
		typcache = lookup_type_cache(prel->ev_type,
									 TYPECACHE_CMP_PROC | TYPECACHE_HASH_PROC);

		prel->ev_byval	= typcache->typbyval;
		prel->ev_len	= typcache->typlen;
		prel->ev_align	= typcache->typalign;

		prel->cmp_proc	= typcache->cmp_proc;
		prel->hash_proc	= typcache->hash_proc;

		/* Try searching for children */
		(void) find_inheritance_children_array(relid, lockmode, false,
											   &prel_children_count,
											   &prel_children);

		/* Fill 'prel' with partition info, raise ERROR if anything is wrong */
		fill_prel_with_partitions(prel, prel_children, prel_children_count);

		/* Unlock the parent */
		UnlockRelationOid(relid, lockmode);

		/* Now it's time to take care of children */
		for (i = 0; i < prel_children_count; i++)
		{
			/* Cache this child */
			cache_parent_of_partition(prel_children[i], relid);

			/* Unlock this child */
			UnlockRelationOid(prel_children[i], lockmode);
		}

		if (prel_children)
			pfree(prel_children);

		/* Read additional parameters ('enable_parent' at the moment) */
		if (read_pathman_params(relid, param_values, param_isnull))
		{
			prel->enable_parent =
					param_values[Anum_pathman_config_params_enable_parent - 1];
		}
		/* Else set default values if they cannot be found */
		else
		{
			prel->enable_parent = DEFAULT_PATHMAN_ENABLE_PARENT;
		}
	}
	PG_CATCH();
	{
		/*
		 * If we managed to create some children but failed later, bounds
		 * cache now might have obsolete data for something that probably is
		 * not a partitioned table at all. Remove it.
		 */
		if (!IsPathmanInitialized())
			/*
			 * ... unless failure was so hard that caches were already destoyed,
			 * i.e. extension disabled
			 */
			PG_RE_THROW();

		if (prel->children != NULL)
		{
			uint32 i;

			for (i = 0; i < PrelChildrenCount(prel); i++)
			{
				Oid child;

				/*
				 * We rely on children and ranges array allocated with 0s, not
				 * random data
				 */
				if (prel->parttype == PT_HASH)
					child = prel->children[i];
				else
				{
					Assert(prel->parttype == PT_RANGE);
					child = prel->ranges[i].child_oid;
				}

				forget_bounds_of_partition(child);
			}
		}

		/* Free this entry */
		free_pathman_relation_info(prel);

		/* Rethrow ERROR further */
		PG_RE_THROW();
	}
	PG_END_TRY();

	/* Free trivial entries */
	if (PrelChildrenCount(prel) == 0)
	{
		free_pathman_relation_info(prel);
		prel = NULL;
	}

	return prel;
}

/* Free PartRelationInfo struct safely */
static void
free_pathman_relation_info(PartRelationInfo *prel)
{
	MemoryContextDelete(prel->mcxt);
}

static PartRelationInfo *
resowner_prel_add(PartRelationInfo *prel)
{
	if (!prel_resowner)
	{
		HASHCTL ctl;

		memset(&ctl, 0, sizeof(ctl));
		ctl.keysize = sizeof(ResourceOwner);
		ctl.entrysize = sizeof(prel_resowner_info);
		ctl.hcxt = TopPathmanContext;

		prel_resowner = hash_create("prel resowner",
									PART_RELS_SIZE, &ctl,
									HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);

		RegisterResourceReleaseCallback(resonwner_prel_callback, NULL);
	}

	if (prel)
	{
		ResourceOwner		resowner = CurrentResourceOwner;
		prel_resowner_info *info;
		bool				found;
		MemoryContext		old_mcxt;

		info = hash_search(prel_resowner,
						   (void *) &resowner,
						   HASH_ENTER,
						   &found);

		if (!found)
			info->prels = NIL;

		/* Register this 'prel' */
		old_mcxt = MemoryContextSwitchTo(TopPathmanContext);
		info->prels = lappend(info->prels, prel);
		MemoryContextSwitchTo(old_mcxt);

		/* Save current caller (function:line) */
		LeakTrackerAdd(prel);

		/* Finally, increment refcount */
		PrelReferenceCount(prel) += 1;
	}

	return prel;
}

static PartRelationInfo *
resowner_prel_del(PartRelationInfo *prel)
{
	/* Must be active! */
	Assert(prel_resowner);

	if (prel)
	{
		ResourceOwner		resowner = CurrentResourceOwner;
		prel_resowner_info *info;

		info = hash_search(prel_resowner,
						   (void *) &resowner,
						   HASH_FIND,
						   NULL);

		if (info)
		{
			/* Check that 'prel' is registered! */
			Assert(list_member_ptr(info->prels, prel));

			/* Remove it from list */
			info->prels = list_delete_ptr(info->prels, prel);
		}

		/* Check that refcount is valid */
		Assert(PrelReferenceCount(prel) > 0);

		/* Decrease refcount */
		PrelReferenceCount(prel) -= 1;

		/* Free list of owners */
		if (PrelReferenceCount(prel) == 0)
		{
			LeakTrackerFree(prel);
		}

		/* Free this entry if it's time */
		if (PrelReferenceCount(prel) == 0 && !PrelIsFresh(prel))
		{
			free_pathman_relation_info(prel);
		}
	}

	return prel;
}

static void
resonwner_prel_callback(ResourceReleasePhase phase,
						bool isCommit,
						bool isTopLevel,
						void *arg)
{
	ResourceOwner		resowner = CurrentResourceOwner;
	prel_resowner_info *info;

	if (prel_resowner)
	{
		ListCell *lc;

		info = hash_search(prel_resowner,
						   (void *) &resowner,
						   HASH_FIND,
						   NULL);

		if (info)
		{
			foreach (lc, info->prels)
			{
				PartRelationInfo *prel = lfirst(lc);

				if (isCommit)
				{
					/* Print verbose list of *possible* owners */
					LeakTrackerPrint(prel);

					elog(WARNING,
						 "cache reference leak: PartRelationInfo(%d) has count %d",
						 PrelParentRelid(prel), PrelReferenceCount(prel));
				}

				/* Check that refcount is valid */
				Assert(PrelReferenceCount(prel) > 0);

				/* Decrease refcount */
				PrelReferenceCount(prel) -= 1;

				/* Free list of owners */
				LeakTrackerFree(prel);

				/* Free this entry if it's time */
				if (PrelReferenceCount(prel) == 0 && !PrelIsFresh(prel))
				{
					free_pathman_relation_info(prel);
				}
			}

			list_free(info->prels);

			hash_search(prel_resowner,
						(void *) &resowner,
						HASH_REMOVE,
						NULL);
		}
	}
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
	MemoryContext	temp_mcxt,	/* reference temporary mcxt */
					old_mcxt;	/* reference current mcxt */

	AssertTemporaryContext();

	/* Allocate memory for 'prel->children' & 'prel->ranges' (if needed) */
	prel->children	= AllocZeroArray(PT_ANY,   prel->mcxt, parts_count, Oid);
	prel->ranges	= AllocZeroArray(PT_RANGE, prel->mcxt, parts_count, RangeEntry);

	/* Set number of children */
	PrelChildrenCount(prel) = parts_count;

	/* Create temporary memory context for loop */
	temp_mcxt = AllocSetContextCreate(CurrentMemoryContext,
									  CppAsString(fill_prel_with_partitions),
									  ALLOCSET_SMALL_SIZES);

	/* Initialize bounds of partitions */
	for (i = 0; i < PrelChildrenCount(prel); i++)
	{
		PartBoundInfo *pbin;

		/* Clear all previous allocations */
		MemoryContextReset(temp_mcxt);

		/* Switch to the temporary memory context */
		old_mcxt = MemoryContextSwitchTo(temp_mcxt);
		{
			/* Fetch constraint's expression tree */
			pbin = get_bounds_of_partition(partitions[i], prel);
		}
		MemoryContextSwitchTo(old_mcxt);

		/* Copy bounds from bound cache */
		switch (prel->parttype)
		{
			case PT_HASH:
				/*
				 * This might be the case if hash part was dropped, and thus
				 * children array alloc'ed smaller than needed, but parts
				 * bound cache still keeps entries with high indexes.
				 */
				if (pbin->part_idx >= PrelChildrenCount(prel))
				{
					/* purged caches will destoy prel, save oid for reporting */
					Oid parent_relid = PrelParentRelid(prel);

					DisablePathman(); /* disable pg_pathman since config is broken */
					ereport(ERROR, (errmsg("pg_pathman's cache for relation %d "
										   "has not been properly initialized. "
										   "Looks like one of hash partitions was dropped.",
										   parent_relid),
									errhint(INIT_ERROR_HINT)));
				}

				prel->children[pbin->part_idx] = pbin->child_relid;
				break;

			case PT_RANGE:
				{
					/* Copy child's Oid */
					prel->ranges[i].child_oid = pbin->child_relid;

					/* Copy all min & max Datums to the persistent mcxt */
					old_mcxt = MemoryContextSwitchTo(prel->mcxt);
					{
						prel->ranges[i].min = CopyBound(&pbin->range_min,
														prel->ev_byval,
														prel->ev_len);

						prel->ranges[i].max = CopyBound(&pbin->range_max,
														prel->ev_byval,
														prel->ev_len);
					}
					MemoryContextSwitchTo(old_mcxt);
				}
				break;

			default:
				{
					DisablePathman(); /* disable pg_pathman since config is broken */
					WrongPartType(prel->parttype);
				}
				break;
		}
	}

	/* Drop temporary memory context */
	MemoryContextDelete(temp_mcxt);

	/* Finalize 'prel' for a RANGE-partitioned table */
	if (prel->parttype == PT_RANGE)
	{
		qsort_range_entries(PrelGetRangesArray(prel),
							PrelChildrenCount(prel),
							prel);

		/* Initialize 'prel->children' array */
		for (i = 0; i < PrelChildrenCount(prel); i++)
			prel->children[i] = prel->ranges[i].child_oid;
	}

	/* Check that each partition Oid has been assigned properly */
	if (prel->parttype == PT_HASH)
		for (i = 0; i < PrelChildrenCount(prel); i++)
		{
			if (!OidIsValid(prel->children[i]))
			{
				DisablePathman(); /* disable pg_pathman since config is broken */
				ereport(ERROR, (errmsg("pg_pathman's cache for relation \"%s\" "
									   "has not been properly initialized",
									   get_rel_name_or_relid(PrelParentRelid(prel))),
								errhint(INIT_ERROR_HINT)));
			}
		}
}

/* qsort() comparison function for RangeEntries */
static int
cmp_range_entries(const void *p1, const void *p2, void *arg)
{
	const RangeEntry   *v1 = (const RangeEntry *) p1;
	const RangeEntry   *v2 = (const RangeEntry *) p2;
	cmp_func_info	   *info = (cmp_func_info *) arg;

	return cmp_bounds(&info->flinfo, info->collid, &v1->min, &v2->min);
}

void
qsort_range_entries(RangeEntry *entries, int nentries,
					const PartRelationInfo *prel)
{
	cmp_func_info cmp_info;

	/* Prepare function info */
	fmgr_info(prel->cmp_proc, &cmp_info.flinfo);
	cmp_info.collid = prel->ev_collid;

	/* Sort partitions by RangeEntry->min asc */
	qsort_arg(entries, nentries,
			  sizeof(RangeEntry),
			  cmp_range_entries,
			  (void *) &cmp_info);
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
				WrongPartType(expected_part_type);
				expected_str = NULL; /* keep compiler happy */
		}

		elog(ERROR, "relation \"%s\" is not partitioned by %s",
			 get_rel_name_or_relid(parent_oid),
			 expected_str);
	}
}

/*
 * Remap partitioning expression columns for tuple source relation.
 * This is a simplified version of functions that return TupleConversionMap.
 * It should be faster if expression uses a few fields of relation.
 */
AttrNumber *
PrelExpressionAttributesMap(const PartRelationInfo *prel,
							TupleDesc source_tupdesc,
							int *map_length)
{
	Oid			parent_relid = PrelParentRelid(prel);
	int			source_natts = source_tupdesc->natts,
				expr_natts = 0;
	AttrNumber *result,
				i;
	bool		is_trivial = true;

	/* Get largest attribute number used in expression */
	i = -1;
	while ((i = bms_next_member(prel->expr_atts, i)) >= 0)
		expr_natts = i;

	/* Allocate array for map */
	result = (AttrNumber *) palloc0(expr_natts * sizeof(AttrNumber));

	/* Find a match for each attribute */
	i = -1;
	while ((i = bms_next_member(prel->expr_atts, i)) >= 0)
	{
		AttrNumber	attnum = i + FirstLowInvalidHeapAttributeNumber;
		char	   *attname = get_attname_compat(parent_relid, attnum);
		int			j;

		Assert(attnum <= expr_natts);

		for (j = 0; j < source_natts; j++)
		{
			Form_pg_attribute att = TupleDescAttr(source_tupdesc, j);

			if (att->attisdropped)
				continue; /* attrMap[attnum - 1] is already 0 */

			if (strcmp(NameStr(att->attname), attname) == 0)
			{
				result[attnum - 1] = (AttrNumber) (j + 1);
				break;
			}
		}

		if (result[attnum - 1] == 0)
			elog(ERROR, "cannot find column \"%s\" in child relation", attname);

		if (result[attnum - 1] != attnum)
			is_trivial = false;
	}

	/* Check if map is trivial */
	if (is_trivial)
	{
		pfree(result);
		return NULL;
	}

	*map_length = expr_natts;
	return result;
}


/*
 * Bounds cache routines.
 */

/* Remove partition's constraint from cache */
static void
forget_bounds_of_partition(Oid partition)
{
	PartBoundInfo *pbin;

	/* Should we search in bounds cache? */
	pbin = pg_pathman_enable_bounds_cache ?
				pathman_cache_search_relid(bounds_cache,
										   partition,
										   HASH_FIND,
										   NULL) :
				NULL; /* don't even bother */

	if (pbin)
	{
		/* Free this entry */
		FreePartBoundInfo(pbin);

		/* Finally remove this entry from cache */
		pathman_cache_search_relid(bounds_cache,
								   partition,
								   HASH_REMOVE,
								   NULL);
	}

}

/*
 * Remove rel's constraint from cache, if relid is partition;
 * Remove all children constraints, if it is parent.
 */
void
forget_bounds_of_rel(Oid relid)
{
	PartStatusInfo *psin;

	forget_bounds_of_partition(relid);

	/*
	 * If it was the parent who got invalidated, purge children's bounds.
	 * We assume here that if bounds_cache has something, parent must be also
	 * in status_cache. Fragile, but seems better then blowing out full bounds
	 * cache or digging pathman_config on each relcache invalidation.
	 */

	/* Find status cache entry for this relation */
	psin = pathman_cache_search_relid(status_cache,
									  relid, HASH_FIND,
									  NULL);
	if (psin != NULL && psin->prel != NULL)
	{
		uint32 i;
		PartRelationInfo *prel = psin->prel;
		Oid	   *children = PrelGetChildrenArray(prel);

		for	(i = 0; i < PrelChildrenCount(prel); i++)
		{
			forget_bounds_of_partition(children[i]);
		}
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

	/* PartRelationInfo must be provided */
	Assert(prel != NULL);

	/* Should always be called in transaction */
	Assert(IsTransactionState());

	/* Should we search in bounds cache? */
	pbin = pg_pathman_enable_bounds_cache ?
				pathman_cache_search_relid(bounds_cache,
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
		pbin_local.child_relid = partition;
		pbin_local.byval = prel->ev_byval;

		/* Try to build constraint's expression tree (may emit ERROR) */
		con_expr = get_partition_constraint_expr(partition, true);

		/* Grab bounds/hash and fill in 'pbin_local' (may emit ERROR) */
		fill_pbin_with_bounds(&pbin_local, prel, con_expr);

		/* We strive to delay the creation of cache's entry */
		pbin = pg_pathman_enable_bounds_cache ?
					pathman_cache_search_relid(bounds_cache,
											   partition,
											   HASH_ENTER,
											   NULL) :
					palloc(sizeof(PartBoundInfo));

		/* Copy data from 'pbin_local' */
		memcpy(pbin, &pbin_local, sizeof(PartBoundInfo));
	}

	return pbin;
}

void
invalidate_bounds_cache(void)
{
	HASH_SEQ_STATUS		status;
	PartBoundInfo	   *pbin;

	Assert(offsetof(PartBoundInfo, child_relid) == 0);

	hash_seq_init(&status, bounds_cache);

	while ((pbin = hash_seq_search(&status)) != NULL)
	{
		FreePartBoundInfo(pbin);

		pathman_cache_search_relid(bounds_cache,
								   pbin->child_relid,
								   HASH_REMOVE, NULL);
	}
}

/*
 * Get constraint expression tree of a partition.
 *
 * build_check_constraint_name_internal() is used to build conname.
 */
Expr *
get_partition_constraint_expr(Oid partition, bool raise_error)
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
		if (!raise_error)
			return NULL;

		ereport(ERROR,
				(errmsg("constraint \"%s\" of partition \"%s\" does not exist",
						conname, get_rel_name_or_relid(partition))));
	}

	con_tuple = SearchSysCache1(CONSTROID, ObjectIdGetDatum(conid));
	conbin_datum = SysCacheGetAttr(CONSTROID, con_tuple,
								   Anum_pg_constraint_conbin,
								   &conbin_isnull);
	if (conbin_isnull)
	{
		if (!raise_error)
			return NULL;

		ereport(ERROR,
				(errmsg("constraint \"%s\" of partition \"%s\" has NULL conbin",
						conname, get_rel_name_or_relid(partition))));
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
											  prel, &pbin->part_idx))
				{
					DisablePathman(); /* disable pg_pathman since config is broken */
					ereport(ERROR,
							(errmsg("wrong constraint format for HASH partition \"%s\"",
									get_rel_name_or_relid(pbin->child_relid)),
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
					old_mcxt = MemoryContextSwitchTo(PathmanBoundsCacheContext);

					pbin->range_min = lower_null ?
											MakeBoundInf(MINUS_INFINITY) :
											MakeBound(datumCopy(lower,
																prel->ev_byval,
																prel->ev_len));

					pbin->range_max = upper_null ?
											MakeBoundInf(PLUS_INFINITY) :
											MakeBound(datumCopy(upper,
																prel->ev_byval,
																prel->ev_len));

					/* Switch back */
					MemoryContextSwitchTo(old_mcxt);
				}
				else
				{
					DisablePathman(); /* disable pg_pathman since config is broken */
					ereport(ERROR,
							(errmsg("wrong constraint format for RANGE partition \"%s\"",
									get_rel_name_or_relid(pbin->child_relid)),
							 errhint(INIT_ERROR_HINT)));
				}
			}
			break;

		default:
			{
				DisablePathman(); /* disable pg_pathman since config is broken */
				WrongPartType(prel->parttype);
			}
			break;
	}
}


/*
 * Parents cache routines.
 */

/* Add parent of partition to cache */
void
cache_parent_of_partition(Oid partition, Oid parent)
{
	PartParentInfo *ppar;

	/* Why would we want to call it not in transaction? */
	Assert(IsTransactionState());

	/* Create a new cache entry */
	ppar = pathman_cache_search_relid(parents_cache,
									  partition,
									  HASH_ENTER,
									  NULL);

	/* Fill entry with parent */
	ppar->parent_relid = parent;
}

/* Remove parent of partition from cache */
void
forget_parent_of_partition(Oid partition)
{
	pathman_cache_search_relid(parents_cache,
							   partition,
							   HASH_REMOVE,
							   NULL);
}

/* Return parent of partition */
Oid
get_parent_of_partition(Oid partition)
{
	PartParentInfo *ppar;

	/* Should always be called in transaction */
	Assert(IsTransactionState());

	/* We don't cache catalog objects */
	if (partition < FirstNormalObjectId)
		return InvalidOid;

	ppar = pathman_cache_search_relid(parents_cache,
									  partition,
									  HASH_FIND,
									  NULL);

	/* Nice, we have a cached entry */
	if (ppar)
	{
		return ppar->parent_relid;
	}
	/* Bad luck, let's search in catalog */
	else
	{
		Relation		relation;
		ScanKeyData		key[1];
		SysScanDesc		scan;
		HeapTuple		htup;
		Oid				parent = InvalidOid;

		relation = heap_open(InheritsRelationId, AccessShareLock);

		ScanKeyInit(&key[0],
					Anum_pg_inherits_inhrelid,
					BTEqualStrategyNumber, F_OIDEQ,
					ObjectIdGetDatum(partition));

		scan = systable_beginscan(relation, InheritsRelidSeqnoIndexId,
								  true, NULL, 1, key);

		while ((htup = systable_getnext(scan)) != NULL)
		{
			/* Extract parent from catalog tuple */
			Oid inhparent = ((Form_pg_inherits) GETSTRUCT(htup))->inhparent;

			/* Check that PATHMAN_CONFIG contains this table */
			if (pathman_config_contains_relation(inhparent, NULL, NULL, NULL, NULL))
			{
				/* We should return this parent */
				parent = inhparent;

				/* Now, let's cache this parent */
				cache_parent_of_partition(partition, parent);
			}

			break; /* there should be no more rows */
		}

		systable_endscan(scan);
		heap_close(relation, AccessShareLock);

		return parent;
	}
}

void
invalidate_parents_cache(void)
{
	HASH_SEQ_STATUS		status;
	PartParentInfo	   *ppar;

	Assert(offsetof(PartParentInfo, child_relid) == 0);

	hash_seq_init(&status, parents_cache);

	while ((ppar = hash_seq_search(&status)) != NULL)
	{
		/* This is a plain structure, no need to pfree() */

		pathman_cache_search_relid(parents_cache,
								   ppar->child_relid,
								   HASH_REMOVE, NULL);
	}
}


/*
 * Partitioning expression routines.
 */

/* Wraps expression in SELECT query and returns parse tree */
Node *
parse_partitioning_expression(const Oid relid,
							  const char *expr_cstr,
							  char **query_string_out,	/* ret value #1 */
							  Node **parsetree_out)		/* ret value #2 */
{
	SelectStmt		   *select_stmt;
	List			   *parsetree_list;
	MemoryContext		old_mcxt;

	const char *sql = "SELECT (%s) FROM ONLY %s.%s";
	char	   *relname = get_rel_name(relid),
			   *nspname = get_namespace_name(get_rel_namespace(relid));
	char	   *query_string = psprintf(sql, expr_cstr,
										quote_identifier(nspname),
										quote_identifier(relname));

	old_mcxt = CurrentMemoryContext;

	PG_TRY();
	{
		parsetree_list = raw_parser(query_string);
	}
	PG_CATCH();
	{
		ErrorData  *error;

		/* Switch to the original context & copy edata */
		MemoryContextSwitchTo(old_mcxt);
		error = CopyErrorData();
		FlushErrorState();

		/* Adjust error message */
		error->detail		= error->message;
		error->message		= psprintf(PARSE_PART_EXPR_ERROR, expr_cstr);
		error->sqlerrcode	= ERRCODE_INVALID_PARAMETER_VALUE;
		error->cursorpos	= 0;
		error->internalpos	= 0;

		ReThrowError(error);
	}
	PG_END_TRY();

	if (list_length(parsetree_list) != 1)
		elog(ERROR, "expression \"%s\" produced more than one query", expr_cstr);

#if PG_VERSION_NUM >= 100000
	select_stmt = (SelectStmt *) ((RawStmt *) linitial(parsetree_list))->stmt;
#else
	select_stmt = (SelectStmt *) linitial(parsetree_list);
#endif

	if (query_string_out)
		*query_string_out = query_string;

	if (parsetree_out)
		*parsetree_out = (Node *) linitial(parsetree_list);

	return ((ResTarget *) linitial(select_stmt->targetList))->val;
}

/* Parse partitioning expression and return its type and nodeToString() as TEXT */
Node *
cook_partitioning_expression(const Oid relid,
							 const char *expr_cstr,
							 Oid *expr_type_out) /* ret value #1 */
{
	Node		   *expr;
	Node		   *parse_tree;
	List		   *query_tree_list;

	char		   *query_string;

	MemoryContext	parse_mcxt,
					old_mcxt;

	AssertTemporaryContext();

	/*
	 * We use separate memory context here, just to make sure we won't
	 * leave anything behind after parsing, rewriting and planning.
	 */
	parse_mcxt = AllocSetContextCreate(CurrentMemoryContext,
									   CppAsString(cook_partitioning_expression),
									   ALLOCSET_SMALL_SIZES);

	/* Switch to mcxt for cooking :) */
	old_mcxt = MemoryContextSwitchTo(parse_mcxt);

	/* First we have to build a raw AST */
	(void) parse_partitioning_expression(relid, expr_cstr,
										 &query_string, &parse_tree);

	/* We don't need pg_pathman's magic here */
	pathman_hooks_enabled = false;

	PG_TRY();
	{
		Query	   *query;
		int			expr_attr;
		Relids		expr_varnos;
		Bitmapset  *expr_varattnos = NULL;

		/* This will fail with ERROR in case of wrong expression */
		query_tree_list = pg_analyze_and_rewrite_compat(parse_tree, query_string,
														NULL, 0, NULL);

		/* Sanity check #1 */
		if (list_length(query_tree_list) != 1)
			elog(ERROR, "partitioning expression produced more than 1 query");

		query = (Query *) linitial(query_tree_list);

		/* Sanity check #2 */
		if (list_length(query->targetList) != 1)
			elog(ERROR, "there should be exactly 1 partitioning expression");

		/* Sanity check #3 */
		if (query_tree_walker(query, query_contains_subqueries, NULL, 0))
			elog(ERROR, "subqueries are not allowed in partitioning expression");

		expr = (Node *) ((TargetEntry *) linitial(query->targetList))->expr;
		expr = eval_const_expressions(NULL, expr);

		/* Sanity check #4 */
		if (contain_mutable_functions(expr))
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
					 errmsg("functions in partitioning expression"
							" must be marked IMMUTABLE")));

		/* Sanity check #5 */
		expr_varnos = pull_varnos(expr);
		if (bms_num_members(expr_varnos) != 1 ||
			relid != ((RangeTblEntry *) linitial(query->rtable))->relid)
		{
			elog(ERROR, "partitioning expression should reference table \"%s\"",
				 get_rel_name(relid));
		}

		/* Sanity check #6 */
		pull_varattnos(expr, bms_singleton_member(expr_varnos), &expr_varattnos);
		expr_attr = -1;
		while ((expr_attr = bms_next_member(expr_varattnos, expr_attr)) >= 0)
		{
			AttrNumber	attnum = expr_attr + FirstLowInvalidHeapAttributeNumber;
			HeapTuple	htup;

			/* Check that there's no system attributes in expression */
			if (attnum < InvalidAttrNumber)
				ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
								errmsg("system attributes are not supported")));

			htup = SearchSysCache2(ATTNUM,
								   ObjectIdGetDatum(relid),
								   Int16GetDatum(attnum));
			if (HeapTupleIsValid(htup))
			{
				bool nullable;

				/* Fetch 'nullable' and free syscache tuple */
				nullable = !((Form_pg_attribute) GETSTRUCT(htup))->attnotnull;
				ReleaseSysCache(htup);

				if (nullable)
					ereport(ERROR, (errcode(ERRCODE_NOT_NULL_VIOLATION),
									errmsg("column \"%s\" should be marked NOT NULL",
										   get_attname_compat(relid, attnum))));
			}
		}

		/* Free sets */
		bms_free(expr_varnos);
		bms_free(expr_varattnos);

		Assert(expr);

		/* Set 'expr_type_out' if needed */
		if (expr_type_out)
			*expr_type_out = exprType(expr);
	}
	PG_CATCH();
	{
		ErrorData  *error;

		/* Don't forget to enable pg_pathman's hooks */
		pathman_hooks_enabled = true;

		/* Switch to the original context & copy edata */
		MemoryContextSwitchTo(old_mcxt);
		error = CopyErrorData();
		FlushErrorState();

		/* Adjust error message */
		error->detail		= error->message;
		error->message		= psprintf(COOK_PART_EXPR_ERROR, expr_cstr);
		error->sqlerrcode	= ERRCODE_INVALID_PARAMETER_VALUE;
		error->cursorpos	= 0;
		error->internalpos	= 0;

		ReThrowError(error);
	}
	PG_END_TRY();

	/* Don't forget to enable pg_pathman's hooks */
	pathman_hooks_enabled = true;

	/* Switch to previous mcxt */
	MemoryContextSwitchTo(old_mcxt);

	/* Get Datum of serialized expression (right mcxt) */
	expr = copyObject(expr);

	/* Free memory */
	MemoryContextDelete(parse_mcxt);

	return expr;
}

/* Canonicalize user's expression (trim whitespaces etc) */
char *
canonicalize_partitioning_expression(const Oid relid,
									 const char *expr_cstr)
{
	Node   *parse_tree;
	Expr   *expr;
	char   *query_string;
	Query  *query;

	AssertTemporaryContext();

	/* First we have to build a raw AST */
	(void) parse_partitioning_expression(relid, expr_cstr,
										 &query_string, &parse_tree);

	query = parse_analyze_compat(parse_tree, query_string, NULL, 0, NULL);
	expr = ((TargetEntry *) linitial(query->targetList))->expr;

	/* We don't care about memory efficiency here */
	return deparse_expression((Node *) expr,
							  deparse_context_for(get_rel_name(relid), relid),
							  false, false);
}

/* Check if query has subqueries */
static bool
query_contains_subqueries(Node *node, void *context)
{
	if (node == NULL)
		return false;

	/* We've met a subquery */
	if (IsA(node, Query))
		return true;

	return expression_tree_walker(node, query_contains_subqueries, NULL);
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

/* Finish all pending invalidation jobs if possible */
void
finish_delayed_invalidation(void)
{
	/* Check that current state is transactional */
	if (IsTransactionState())
	{
		AcceptInvalidationMessages();

		/* Handle the probable 'DROP EXTENSION' case */
		if (delayed_shutdown)
		{
			Oid cur_pathman_config_relid;

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

				/* No need to continue, exit */
				return;
			}
		}
	}
}
