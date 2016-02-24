#include "pathman.h"
#include "miscadmin.h"
#include "postmaster/bgworker.h"
#include "catalog/pg_type.h"
#include "executor/spi.h"
#include "storage/dsm.h"
#include "access/xact.h"
#include "utils/snapmgr.h"
#include "utils/typcache.h"

/*-------------------------------------------------------------------------
 *
 * worker.c
 *
 * The purpose of this module is to create partitions in a separate
 * transaction. To do so we create a separate background worker,
 * pass arguments to it (see PartitionArgs) and gather the result
 * (which is the new partition oid).
 *
 *-------------------------------------------------------------------------
 */

static dsm_segment *segment;

static void bg_worker_main(Datum main_arg);

typedef struct PartitionArgs
{
	Oid	dbid;
	Oid	relid;
	#ifdef HAVE_INT64_TIMESTAMP
	int64	value;
	#else
	double	value;
	#endif
	Oid	value_type;
	bool	by_val;
	Oid	result;
} PartitionArgs;

/*
 * Starts background worker that will create new partitions,
 * waits till it finishes the job and returns the result (new partition oid)
 */
Oid
create_partitions_bg_worker(Oid relid, Datum value, Oid value_type)
{
	BackgroundWorker		worker;
	BackgroundWorkerHandle *worker_handle;
	BgwHandleStatus			status;
	dsm_segment	   *segment;
	dsm_handle		segment_handle;
	pid_t 			pid;
	PartitionArgs	   *args;
	Oid 			child_oid;
	TypeCacheEntry	   *tce;

	/* Create a dsm segment for the worker to pass arguments */
	segment = dsm_create(sizeof(PartitionArgs), 0);
	segment_handle = dsm_segment_handle(segment);

	tce = lookup_type_cache(value_type, 0);

	/* Fill arguments structure */
	args = (PartitionArgs *) dsm_segment_address(segment);
	args->dbid = MyDatabaseId;
	args->relid = relid;
	if (tce->typbyval)
		args->value = value;
	else
		memcpy(&args->value, DatumGetPointer(value), sizeof(args->value));
	args->by_val = tce->typbyval;
	args->value_type = value_type;
	args->result = 0;

	/* Initialize worker struct */
	worker.bgw_flags = BGWORKER_SHMEM_ACCESS |
		BGWORKER_BACKEND_DATABASE_CONNECTION;
	worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
	worker.bgw_restart_time = BGW_NEVER_RESTART;
	worker.bgw_main = bg_worker_main;
	worker.bgw_main_arg = Int32GetDatum(segment_handle);
	worker.bgw_notify_pid = MyProcPid;

	/* Start dynamic worker */
	if (!RegisterDynamicBackgroundWorker(&worker, &worker_handle))
	{
		elog(WARNING, "Unable to create background worker for pg_pathman");
	}

	status = WaitForBackgroundWorkerStartup(worker_handle, &pid);
	if (status == BGWH_POSTMASTER_DIED)
	{
		ereport(WARNING,
                (errmsg("Postmaster died during the pg_pathman background worker process"),
                 errhint("More details may be available in the server log.")));
	}

	/* Wait till the worker finishes its job */
	status = WaitForBackgroundWorkerShutdown(worker_handle);
	child_oid = args->result;

	/* Free dsm segment */
	dsm_detach(segment);

	return child_oid;
}

/*
 * Main worker routine. Accepts dsm_handle as an argument
 */
static void
bg_worker_main(Datum main_arg)
{
	PartitionArgs *args;
	dsm_handle		handle = DatumGetInt32(main_arg);

	/* Create resource owner */
	CurrentResourceOwner = ResourceOwnerCreate(NULL, "CreatePartitionsWorker");

	/* Attach to dynamic shared memory */
	if (!handle)
	{
		ereport(WARNING,
                (errmsg("pg_pathman worker: ivalid dsm_handle")));
	}
	segment = dsm_attach(handle);
	args = dsm_segment_address(segment);

	/* Establish connection and start transaction */
	BackgroundWorkerInitializeConnectionByOid(args->dbid, InvalidOid);
	StartTransactionCommand();
	SPI_connect();
	PushActiveSnapshot(GetTransactionSnapshot());

	/* Create partitions */
	args->result = create_partitions(args->relid, PATHMAN_GET_DATUM(args->value, args->by_val), args->value_type);

	/* Cleanup */
	SPI_finish();
	PopActiveSnapshot();
	CommitTransactionCommand();

	dsm_detach(segment);
}

/*
 * Create partitions and return an OID of the partition that contain value
 */
Oid
create_partitions(Oid relid, Datum value, Oid value_type)
{
	int 		ret;
	RangeEntry *ranges;
	Datum		vals[2];
	Oid			oids[] = {OIDOID, value_type};
	bool		nulls[] = {false, false};
	char	   *sql;
	bool		found;
	int pos;
	PartRelationInfo *prel;
	RangeRelation	*rangerel;
	FmgrInfo   cmp_func;
	char *schema;

	// elog(WARNING, "WORKER PID >>>%d<<<", MyProcPid);
	// sleep(10);

	schema = get_extension_schema();

	prel = get_pathman_relation_info(relid, NULL);
	rangerel = get_pathman_range_relation(relid, NULL);
	ranges = dsm_array_get_pointer(&rangerel->ranges);

	/* Comparison function */
	cmp_func = *get_cmp_func(value_type, prel->atttype);

	vals[0] = ObjectIdGetDatum(relid);
	vals[1] = value;

	/* Perform PL procedure */
	sql = psprintf("SELECT %s.append_partitions_on_demand_internal($1, $2)",
				   schema);
	ret = SPI_execute_with_args(sql, 2, oids, vals, nulls, false, 0);
	if (ret > 0)
	{
		/* Update relation info */
		free_dsm_array(&rangerel->ranges);
		free_dsm_array(&prel->children);
		load_check_constraints(relid, GetCatalogSnapshot(relid));
	}
	else
		elog(WARNING, "Attempt to create new partitions failed");

	/* Repeat binary search */
	ranges = dsm_array_get_pointer(&rangerel->ranges);
	pos = range_binary_search(rangerel, &cmp_func, value, &found);
	if (found)
		return ranges[pos].child_oid;

	return 0;
}
