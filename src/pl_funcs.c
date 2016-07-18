/* ------------------------------------------------------------------------
 *
 * pl_funcs.c
 *		Utility C functions for stored procedures
 *
 * Copyright (c) 2015-2016, Postgres Professional
 *
 * ------------------------------------------------------------------------
 */

#include "pathman.h"
#include "utils/lsyscache.h"
#include "utils/typcache.h"
#include "utils/array.h"
#include "utils/snapmgr.h"
#include "utils/memutils.h"
#include "access/nbtree.h"
#include "access/xact.h"
#include "catalog/pg_type.h"
#include "executor/spi.h"
#include "storage/lmgr.h"
#include "utils.h"


/* declarations */
PG_FUNCTION_INFO_V1( on_partitions_created );
PG_FUNCTION_INFO_V1( on_partitions_updated );
PG_FUNCTION_INFO_V1( on_partitions_removed );
PG_FUNCTION_INFO_V1( find_or_create_range_partition);
PG_FUNCTION_INFO_V1( get_range_by_idx );
PG_FUNCTION_INFO_V1( get_partition_range );
PG_FUNCTION_INFO_V1( acquire_partitions_lock );
PG_FUNCTION_INFO_V1( release_partitions_lock );
PG_FUNCTION_INFO_V1( check_overlap );
PG_FUNCTION_INFO_V1( get_min_range_value );
PG_FUNCTION_INFO_V1( get_max_range_value );
PG_FUNCTION_INFO_V1( get_type_hash_func );
PG_FUNCTION_INFO_V1( get_hash );


/*
 * Partition-related operation type.
 */
typedef enum
{
	EV_ON_PART_CREATED = 1,
	EV_ON_PART_UPDATED,
	EV_ON_PART_REMOVED
} part_event_type;

/*
 * We have to reset shared memory cache each time a transaction
 * containing a partitioning-related operation has been rollbacked,
 * hence we need to pass a partitioned table's Oid & some other stuff.
 *
 * Note: 'relname' cannot be fetched within
 * Xact callbacks, so we have to store it here.
 */
typedef struct part_abort_arg part_abort_arg;

struct part_abort_arg
{
	Oid					partitioned_table_relid;
	char			   *relname;

	bool				is_subxact;		/* needed for correct callback removal */
	SubTransactionId	subxact_id;		/* necessary for detecting specific subxact */
	part_abort_arg	   *xact_cb_arg;	/* points to the parent Xact's arg */

	part_event_type		event;			/* created | updated | removed partitions */

	bool				expired;		/* set by (Sub)Xact when a job is done */
};


static part_abort_arg * make_part_abort_arg(Oid partitioned_table,
											part_event_type event,
											bool is_subxact,
											part_abort_arg *xact_cb_arg);

static void handle_part_event_cancellation(const part_abort_arg *arg);
static void on_xact_abort_callback(XactEvent event, void *arg);
static void on_subxact_abort_callback(SubXactEvent event, SubTransactionId mySubid,
									  SubTransactionId parentSubid, void *arg);

static void remove_on_xact_abort_callbacks(void *arg);
static void add_on_xact_abort_callbacks(Oid partitioned_table, part_event_type event);

static void on_partitions_created_internal(Oid partitioned_table, bool add_callbacks);
static void on_partitions_updated_internal(Oid partitioned_table, bool add_callbacks);
static void on_partitions_removed_internal(Oid partitioned_table, bool add_callbacks);


/* Construct part_abort_arg for callbacks in TopTransactionContext. */
static part_abort_arg *
make_part_abort_arg(Oid partitioned_table, part_event_type event,
					bool is_subxact, part_abort_arg *xact_cb_arg)
{
	part_abort_arg *arg = MemoryContextAlloc(TopTransactionContext,
											 sizeof(part_abort_arg));

	const char	   *relname = get_rel_name(partitioned_table);

	/* Fill in Oid & relation name */
	arg->partitioned_table_relid = partitioned_table;
	arg->relname = MemoryContextStrdup(TopTransactionContext, relname);
	arg->is_subxact = is_subxact;
	arg->subxact_id = GetCurrentSubTransactionId(); /* for SubXact callback */
	arg->xact_cb_arg = xact_cb_arg;
	arg->event = event;
	arg->expired = false;

	return arg;
}

/* Revert shared memory cache changes iff xact has been aborted. */
static void
handle_part_event_cancellation(const part_abort_arg *arg)
{
#define DO_NOT_USE_CALLBACKS false /* just to clarify intentions */

	switch (arg->event)
	{
		case EV_ON_PART_CREATED:
			{
				elog(WARNING, "Partitioning of table '%s' has been aborted, "
							  "removing partitions from pg_pathman's cache",
					 arg->relname);

				on_partitions_removed_internal(arg->partitioned_table_relid,
											   DO_NOT_USE_CALLBACKS);
			}
			break;

		case EV_ON_PART_UPDATED:
			{
				elog(WARNING, "All changes in partitioned table "
							  "'%s' will be discarded",
					 arg->relname);

				on_partitions_updated_internal(arg->partitioned_table_relid,
											   DO_NOT_USE_CALLBACKS);
			}
			break;

		case EV_ON_PART_REMOVED:
			{
				elog(WARNING, "All changes in partitioned table "
							  "'%s' will be discarded",
					 arg->relname);

				on_partitions_created_internal(arg->partitioned_table_relid,
											   DO_NOT_USE_CALLBACKS);
			}
			break;

		default:
			elog(ERROR, "Unknown event spotted in xact callback");
	}
}

/*
 * Add & remove xact callbacks
 */

static void
remove_on_xact_abort_callbacks(void *arg)
{
	part_abort_arg *parg = (part_abort_arg *) arg;

	elog(DEBUG2, "remove_on_xact_abort_callbacks() "
				 "[is_subxact = %s, relname = '%s', event = %u] "
				 "triggered for relation %u",
		 (parg->is_subxact ? "true" : "false"), parg->relname,
		 parg->event, parg->partitioned_table_relid);

	/* Is this a SubXact callback or not? */
	if (!parg->is_subxact)
		UnregisterXactCallback(on_xact_abort_callback, arg);
	else
		UnregisterSubXactCallback(on_subxact_abort_callback, arg);

	pfree(arg);
}

static void
add_on_xact_abort_callbacks(Oid partitioned_table, part_event_type event)
{
	part_abort_arg *xact_cb_arg = make_part_abort_arg(partitioned_table,
													  event, false, NULL);

	RegisterXactCallback(on_xact_abort_callback, (void *) xact_cb_arg);
	execute_on_xact_mcxt_reset(TopTransactionContext,
							   remove_on_xact_abort_callbacks,
							   xact_cb_arg);

	/* Register SubXact callback if necessary */
	if (IsSubTransaction())
	{
		/*
		 * SubXact callback's arg contains a pointer to the parent
		 * Xact callback's arg. This will allow it to 'expire' both
		 * args and to prevent Xact's callback from doing anything
		 */
		void *subxact_cb_arg = make_part_abort_arg(partitioned_table, event,
												   true, xact_cb_arg);

		RegisterSubXactCallback(on_subxact_abort_callback, subxact_cb_arg);
		execute_on_xact_mcxt_reset(CurTransactionContext,
								   remove_on_xact_abort_callbacks,
								   subxact_cb_arg);
	}
}

/*
 * Xact & SubXact callbacks
 */

static void
on_xact_abort_callback(XactEvent event, void *arg)
{
	part_abort_arg *parg = (part_abort_arg *) arg;

	/* Check that this is an aborted Xact & action has not expired yet */
	if ((event == XACT_EVENT_ABORT || event == XACT_EVENT_PARALLEL_ABORT) &&
		!parg->expired)
	{
		handle_part_event_cancellation(parg);

		/* Set expiration flag */
		parg->expired = true;
	}
}

static void
on_subxact_abort_callback(SubXactEvent event, SubTransactionId mySubid,
						  SubTransactionId parentSubid, void *arg)
{
	part_abort_arg *parg = (part_abort_arg *) arg;

	Assert(parg->subxact_id != InvalidSubTransactionId);

	/* Check if this is an aborted SubXact we've been waiting for */
	if (event == SUBXACT_EVENT_ABORT_SUB &&
		mySubid <= parg->subxact_id && !parg->expired)
	{
		handle_part_event_cancellation(parg);

		/* Now set expiration flags to disable Xact callback */
		parg->xact_cb_arg->expired = true;
		parg->expired = true;
	}
}

/*
 * Callbacks
 */

static void
on_partitions_created_internal(Oid partitioned_table, bool add_callbacks)
{
	elog(DEBUG2, "on_partitions_created() [add_callbacks = %s] "
				 "triggered for relation %u",
		 (add_callbacks ? "true" : "false"), partitioned_table);

	LWLockAcquire(pmstate->load_config_lock, LW_EXCLUSIVE);
		load_relations(false);
	LWLockRelease(pmstate->load_config_lock);

	/* Register hooks that will clear shmem cache if needed */
	if (add_callbacks)
		add_on_xact_abort_callbacks(partitioned_table, EV_ON_PART_CREATED);
}

static void
on_partitions_updated_internal(Oid partitioned_table, bool add_callbacks)
{
	elog(DEBUG2, "on_partitions_updated() [add_callbacks = %s] "
				 "triggered for relation %u",
		 (add_callbacks ? "true" : "false"), partitioned_table);

	if (get_pathman_relation_info(partitioned_table, NULL))
	{
		LWLockAcquire(pmstate->load_config_lock, LW_EXCLUSIVE);
			remove_relation_info(partitioned_table);
			load_relations(false);
		LWLockRelease(pmstate->load_config_lock);
	}

	/* Register hooks that will clear shmem cache if needed */
	if (add_callbacks)
		add_on_xact_abort_callbacks(partitioned_table, EV_ON_PART_UPDATED);
}

static void
on_partitions_removed_internal(Oid partitioned_table, bool add_callbacks)
{
	elog(DEBUG2, "on_partitions_removed() [add_callbacks = %s] "
				 "triggered for relation %u",
		 (add_callbacks ? "true" : "false"), partitioned_table);

	LWLockAcquire(pmstate->load_config_lock, LW_EXCLUSIVE);
		remove_relation_info(partitioned_table);
	LWLockRelease(pmstate->load_config_lock);

	/* Register hooks that will clear shmem cache if needed */
	if (add_callbacks)
		add_on_xact_abort_callbacks(partitioned_table, EV_ON_PART_REMOVED);
}

/*
 * Thin layer between pure c and pl/PgSQL
 */

Datum
on_partitions_created(PG_FUNCTION_ARGS)
{
	on_partitions_created_internal(PG_GETARG_OID(0), true);
	PG_RETURN_NULL();
}

Datum
on_partitions_updated(PG_FUNCTION_ARGS)
{
	on_partitions_updated_internal(PG_GETARG_OID(0), true);
	PG_RETURN_NULL();
}

Datum
on_partitions_removed(PG_FUNCTION_ARGS)
{
	on_partitions_removed_internal(PG_GETARG_OID(0), true);
	PG_RETURN_NULL();
}

/*
 * Returns partition oid for specified parent relid and value.
 * In case when partition isn't exist try to create one.
 */
Datum
find_or_create_range_partition(PG_FUNCTION_ARGS)
{
	Oid		relid = PG_GETARG_OID(0);
	Datum	value = PG_GETARG_DATUM(1);
	Oid		value_type = get_fn_expr_argtype(fcinfo->flinfo, 1);
	int		pos;
	bool	found;
	RangeRelation	*rangerel;
	RangeEntry		*ranges;
	TypeCacheEntry	*tce;
	PartRelationInfo *prel;
	Oid				 cmp_proc_oid;
	FmgrInfo		 cmp_func;

	tce = lookup_type_cache(value_type,
		TYPECACHE_EQ_OPR | TYPECACHE_LT_OPR | TYPECACHE_GT_OPR |
		TYPECACHE_CMP_PROC | TYPECACHE_CMP_PROC_FINFO);

	prel = get_pathman_relation_info(relid, NULL);
	rangerel = get_pathman_range_relation(relid, NULL);

	if (!prel || !rangerel)
		PG_RETURN_NULL();

	cmp_proc_oid = get_opfamily_proc(tce->btree_opf,
									 value_type,
									 prel->atttype,
									 BTORDER_PROC);
	fmgr_info(cmp_proc_oid, &cmp_func);

	ranges = dsm_array_get_pointer(&rangerel->ranges);
	pos = range_binary_search(rangerel, &cmp_func, value, &found);

	/*
	 * If found then just return oid. Else create new partitions
	 */
	if (found)
		PG_RETURN_OID(ranges[pos].child_oid);
	/*
	 * If not found and value is between first and last partitions
	*/
	if (!found && pos >= 0)
		PG_RETURN_NULL();
	else
	{
		Oid		child_oid;
		bool	crashed = false;

		/* Lock config before appending new partitions */
		LWLockAcquire(pmstate->load_config_lock, LW_EXCLUSIVE);

		/* Restrict concurrent partition creation */
		LWLockAcquire(pmstate->edit_partitions_lock, LW_EXCLUSIVE);

		/*
		 * Check if someone else has already created partition.
		 */
		ranges = dsm_array_get_pointer(&rangerel->ranges);
		pos = range_binary_search(rangerel, &cmp_func, value, &found);
		if (found)
		{
			LWLockRelease(pmstate->edit_partitions_lock);
			LWLockRelease(pmstate->load_config_lock);
			PG_RETURN_OID(ranges[pos].child_oid);
		}

		/* Start background worker to create new partitions */
		child_oid = create_partitions_bg_worker(relid, value, value_type, &crashed);

		/* Release locks */
		if (!crashed)
		{
			LWLockRelease(pmstate->edit_partitions_lock);
			LWLockRelease(pmstate->load_config_lock);
		}

		/* Repeat binary search */
		(void) range_binary_search(rangerel, &cmp_func, value, &found);
		if (found)
			PG_RETURN_OID(child_oid);
	}

	PG_RETURN_NULL();
}

/*
 * Returns range (min, max) as output parameters
 *
 * first argument is the parent relid
 * second is the partition relid
 * third and forth are MIN and MAX output parameters
 */
Datum
get_partition_range(PG_FUNCTION_ARGS)
{
	Oid		parent_oid = PG_GETARG_OID(0);
	Oid		child_oid = PG_GETARG_OID(1);
	int		nelems = 2;
	int 	i;
	bool	found = false;
	Datum			   *elems;
	PartRelationInfo   *prel;
	RangeRelation	   *rangerel;
	RangeEntry		   *ranges;
	TypeCacheEntry	   *tce;
	ArrayType		   *arr;

	prel = get_pathman_relation_info(parent_oid, NULL);

	rangerel = get_pathman_range_relation(parent_oid, NULL);

	if (!prel || !rangerel)
		PG_RETURN_NULL();

	ranges = dsm_array_get_pointer(&rangerel->ranges);
	tce = lookup_type_cache(prel->atttype, 0);

	/* Looking for specified partition */
	for(i=0; i<rangerel->ranges.length; i++)
		if (ranges[i].child_oid == child_oid)
		{
			found = true;
			break;
		}

	if (found)
	{
		bool byVal = rangerel->by_val;

		elems = palloc(nelems * sizeof(Datum));
		elems[0] = PATHMAN_GET_DATUM(ranges[i].min, byVal);
		elems[1] = PATHMAN_GET_DATUM(ranges[i].max, byVal);

		arr = construct_array(elems, nelems, prel->atttype,
							  tce->typlen, tce->typbyval, tce->typalign);
		PG_RETURN_ARRAYTYPE_P(arr);
	}

	PG_RETURN_NULL();
}


/*
 * Returns N-th range (in form of array)
 *
 * First argument is the parent relid.
 * Second argument is the index of the range (if it is negative then the last
 * range will be returned).
 */
Datum
get_range_by_idx(PG_FUNCTION_ARGS)
{
	Oid parent_oid = PG_GETARG_OID(0);
	int idx = PG_GETARG_INT32(1);
	PartRelationInfo *prel;
	RangeRelation	*rangerel;
	RangeEntry		*ranges;
	RangeEntry		*re;
	Datum			*elems;
	TypeCacheEntry	*tce;

	prel = get_pathman_relation_info(parent_oid, NULL);

	rangerel = get_pathman_range_relation(parent_oid, NULL);

	if (!prel || !rangerel || idx >= (int)rangerel->ranges.length)
		PG_RETURN_NULL();

	tce = lookup_type_cache(prel->atttype, 0);
	ranges = dsm_array_get_pointer(&rangerel->ranges);
	if (idx >= 0)
		re = &ranges[idx];
	else
		re = &ranges[rangerel->ranges.length - 1];

	elems = palloc(2 * sizeof(Datum));
	elems[0] = PATHMAN_GET_DATUM(re->min, rangerel->by_val);
	elems[1] = PATHMAN_GET_DATUM(re->max, rangerel->by_val);

	PG_RETURN_ARRAYTYPE_P(
		construct_array(elems, 2, prel->atttype,
						tce->typlen, tce->typbyval, tce->typalign));
}

/*
 * Returns min value of the first range for relation
 */
Datum
get_min_range_value(PG_FUNCTION_ARGS)
{
	Oid parent_oid = PG_GETARG_OID(0);
	PartRelationInfo *prel;
	RangeRelation	*rangerel;
	RangeEntry		*ranges;

	prel = get_pathman_relation_info(parent_oid, NULL);
	rangerel = get_pathman_range_relation(parent_oid, NULL);

	if (!prel || !rangerel || prel->parttype != PT_RANGE || rangerel->ranges.length == 0)
		PG_RETURN_NULL();

	ranges = dsm_array_get_pointer(&rangerel->ranges);
	PG_RETURN_DATUM(PATHMAN_GET_DATUM(ranges[0].min, rangerel->by_val));
}

/*
 * Returns max value of the last range for relation
 */
Datum
get_max_range_value(PG_FUNCTION_ARGS)
{
	Oid parent_oid = PG_GETARG_OID(0);
	PartRelationInfo *prel;
	RangeRelation	 *rangerel;
	RangeEntry		 *ranges;

	prel = get_pathman_relation_info(parent_oid, NULL);
	rangerel = get_pathman_range_relation(parent_oid, NULL);

	if (!prel || !rangerel || prel->parttype != PT_RANGE || rangerel->ranges.length == 0)
		PG_RETURN_NULL();

	ranges = dsm_array_get_pointer(&rangerel->ranges);
	PG_RETURN_DATUM(PATHMAN_GET_DATUM(ranges[rangerel->ranges.length-1].max, rangerel->by_val));
}

/*
 * Checks if range overlaps with existing partitions.
 * Returns TRUE if overlaps and FALSE otherwise.
 */
Datum
check_overlap(PG_FUNCTION_ARGS)
{
	Oid					partitioned_table = PG_GETARG_OID(0);

	Datum				p1 = PG_GETARG_DATUM(1),
						p2 = PG_GETARG_DATUM(2);

	Oid					p1_type = get_fn_expr_argtype(fcinfo->flinfo, 1),
						p2_type = get_fn_expr_argtype(fcinfo->flinfo, 2);

	FmgrInfo		   *cmp_func_1,
					   *cmp_func_2;

	PartRelationInfo   *prel;
	RangeRelation	   *rangerel;
	RangeEntry		   *ranges;
	int					i;
	bool				byVal;

	prel = get_pathman_relation_info(partitioned_table, NULL);
	rangerel = get_pathman_range_relation(partitioned_table, NULL);

	if (!prel || !rangerel || prel->parttype != PT_RANGE)
		PG_RETURN_NULL();

	/* Get comparison functions */
	cmp_func_1 = get_cmp_func(p1_type, prel->atttype);
	cmp_func_2 = get_cmp_func(p2_type, prel->atttype);

	byVal = rangerel->by_val;
	ranges = (RangeEntry *) dsm_array_get_pointer(&rangerel->ranges);
	for (i = 0; i < rangerel->ranges.length; i++)
	{
		int c1 = FunctionCall2(cmp_func_1, p1,
								PATHMAN_GET_DATUM(ranges[i].max, byVal));
		int c2 = FunctionCall2(cmp_func_2, p2,
								PATHMAN_GET_DATUM(ranges[i].min, byVal));

		if (c1 < 0 && c2 > 0)
			PG_RETURN_BOOL(true);
	}

	PG_RETURN_BOOL(false);
}

/*
 * Acquire partitions lock
 */
Datum
acquire_partitions_lock(PG_FUNCTION_ARGS)
{
	LWLockAcquire(pmstate->edit_partitions_lock, LW_EXCLUSIVE);
	PG_RETURN_NULL();
}

Datum
release_partitions_lock(PG_FUNCTION_ARGS)
{
	LWLockRelease(pmstate->edit_partitions_lock);
	PG_RETURN_NULL();
}

/*
 * Returns hash function OID for specified type
 */
Datum
get_type_hash_func(PG_FUNCTION_ARGS)
{
	TypeCacheEntry *tce;
	Oid 			type_oid = PG_GETARG_OID(0);

	tce = lookup_type_cache(type_oid, TYPECACHE_HASH_PROC);
	PG_RETURN_OID(tce->hash_proc);
}

Datum
get_hash(PG_FUNCTION_ARGS)
{
	uint32	value = PG_GETARG_UINT32(0),
			part_count = PG_GETARG_UINT32(1);

	PG_RETURN_UINT32(make_hash(value, part_count));
}
