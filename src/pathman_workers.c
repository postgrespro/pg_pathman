/*-------------------------------------------------------------------------
 *
 * pathman_workers.c
 *
 *		There are two purposes of this subsystem:
 *
 *			* Create new partitions for INSERT in separate transaction
 *			* Process concurrent partitioning operations
 *
 *		Background worker API is used for both cases.
 *
 * Copyright (c) 2015-2016, Postgres Professional
 *
 *-------------------------------------------------------------------------
 */

#include "init.h"
#include "partition_creation.h"
#include "pathman_workers.h"
#include "relation_info.h"
#include "utils.h"
#include "xact_handling.h"

#include "access/htup_details.h"
#include "access/xact.h"
#include "catalog/pg_type.h"
#include "executor/spi.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "postmaster/bgworker.h"
#include "storage/dsm.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/proc.h"
#include "utils/builtins.h"
#include "utils/datum.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"
#include "utils/typcache.h"
#include "utils/resowner.h"
#include "utils/snapmgr.h"



/* Declarations for ConcurrentPartWorker */
PG_FUNCTION_INFO_V1( partition_table_concurrently );
PG_FUNCTION_INFO_V1( show_concurrent_part_tasks_internal );
PG_FUNCTION_INFO_V1( stop_concurrent_part_task );


/*
 * Dynamically resolve functions (for BGW API).
 */
extern PGDLLEXPORT void bgw_main_spawn_partitions(Datum main_arg);
extern PGDLLEXPORT void bgw_main_concurrent_part(Datum main_arg);


static void handle_sigterm(SIGNAL_ARGS);
static void bg_worker_load_config(const char *bgw_name);
static bool start_bgworker(const char bgworker_name[BGW_MAXLEN],
							const char bgworker_proc[BGW_MAXLEN],
							Datum bgw_arg, bool wait_for_shutdown);


/*
 * Function context for concurrent_part_tasks_internal() SRF.
 */
typedef struct
{
	int cur_idx; /* current slot to be processed */
} active_workers_cxt;


/*
 * Slots for concurrent partitioning tasks.
 */
static ConcurrentPartSlot  *concurrent_part_slots;


/*
 * Available workers' names.
 */
static const char		   *spawn_partitions_bgw	= "SpawnPartitionsWorker";
static const char		   *concurrent_part_bgw		= "ConcurrentPartWorker";


/*
 * Estimate amount of shmem needed for concurrent partitioning.
 */
Size
estimate_concurrent_part_task_slots_size(void)
{
	/* NOTE: we suggest that max_worker_processes is in PGC_POSTMASTER */
	return sizeof(ConcurrentPartSlot) * PART_WORKER_SLOTS;
}

/*
 * Initialize shared memory needed for concurrent partitioning.
 */
void
init_concurrent_part_task_slots(void)
{
	bool	found;
	Size	size = estimate_concurrent_part_task_slots_size();
	int		i;

	concurrent_part_slots = (ConcurrentPartSlot *)
			ShmemInitStruct("array of ConcurrentPartSlots", size, &found);

	/* Initialize 'concurrent_part_slots' if needed */
	if (!found)
	{
		memset(concurrent_part_slots, 0, size);

		for (i = 0; i < PART_WORKER_SLOTS; i++)
			SpinLockInit(&concurrent_part_slots[i].mutex);
	}
}


/*
 * -------------------------------------------------
 *  Common utility functions for background workers
 * -------------------------------------------------
 */

/*
 * Handle SIGTERM in BGW's process.
 * Use it in favor of bgworker_die().
 */
static void
handle_sigterm(SIGNAL_ARGS)
{
	int save_errno = errno;

	SetLatch(MyLatch);

	if (!proc_exit_inprogress)
	{
		InterruptPending = true;
		ProcDiePending = true;
	}

	errno = save_errno;
}

/*
 * Initialize pg_pathman's local config in BGW's process.
 */
static void
bg_worker_load_config(const char *bgw_name)
{
	/* Try to load config */
	if (!load_config())
		elog(ERROR, "%s: could not load pg_pathman's config [%u]",
			 bgw_name, MyProcPid);
	else
		elog(LOG, "%s: loaded pg_pathman's config [%u]",
			 bgw_name, MyProcPid);
}

/*
 * Common function to start background worker.
 */
static bool
start_bgworker(const char bgworker_name[BGW_MAXLEN],
				const char bgworker_proc[BGW_MAXLEN],
				Datum bgw_arg, bool wait_for_shutdown)
{
#define HandleError(condition, new_state) \
	if (condition) { exec_state = (new_state); goto handle_exec_state; }

	/* Execution state to be checked */
	enum
	{
		BGW_OK = 0,				/* everything is fine (default) */
		BGW_COULD_NOT_START,	/* could not start worker */
		BGW_PM_DIED				/* postmaster died */
	}						exec_state = BGW_OK;

	BackgroundWorker		worker;
	BackgroundWorkerHandle *bgw_handle;
	BgwHandleStatus			bgw_status;
	bool					bgw_started;
	pid_t					pid;

	/* Initialize worker struct */
	memset(&worker, 0, sizeof(worker));

	snprintf(worker.bgw_name, BGW_MAXLEN, "%s", bgworker_name);
	snprintf(worker.bgw_function_name, BGW_MAXLEN, "%s", bgworker_proc);
	snprintf(worker.bgw_library_name, BGW_MAXLEN, "pg_pathman");

	worker.bgw_flags			= BGWORKER_SHMEM_ACCESS |
									BGWORKER_BACKEND_DATABASE_CONNECTION;
	worker.bgw_start_time		= BgWorkerStart_RecoveryFinished;
	worker.bgw_restart_time		= BGW_NEVER_RESTART;
	worker.bgw_main_arg			= bgw_arg;
	worker.bgw_notify_pid		= MyProcPid;

	/* Start dynamic worker */
	bgw_started = RegisterDynamicBackgroundWorker(&worker, &bgw_handle);
	HandleError(bgw_started == false, BGW_COULD_NOT_START);

	/* Wait till the worker starts */
	bgw_status = WaitForBackgroundWorkerStartup(bgw_handle, &pid);
	HandleError(bgw_status == BGWH_POSTMASTER_DIED, BGW_PM_DIED);

	/* Wait till the edn if we're asked to */
	if (wait_for_shutdown)
	{
		/* Wait till the worker finishes job */
		bgw_status = WaitForBackgroundWorkerShutdown(bgw_handle);
		HandleError(bgw_status == BGWH_POSTMASTER_DIED, BGW_PM_DIED);
	}

/* end execution */
handle_exec_state:

	switch (exec_state)
	{
		/* Caller might want to handle this case */
		case BGW_COULD_NOT_START:
			return false;

		case BGW_PM_DIED:
			ereport(ERROR,
					(errmsg("Postmaster died during the pg_pathman background worker process"),
					 errhint("More details may be available in the server log.")));
			break;

		default:
			break;
	}

	return true;
}

/*
 * Show generic error message if we failed to start bgworker.
 */
static inline void
start_bgworker_errmsg(const char *bgworker_name)
{
	ereport(ERROR, (errmsg("could not start %s", bgworker_name),
					errhint("consider increasing max_worker_processes")));
}


/*
 * --------------------------------------
 *  SpawnPartitionsWorker implementation
 * --------------------------------------
 */

/*
 * Create args segment for partitions bgw.
 */
static dsm_segment *
create_partitions_bg_worker_segment(Oid relid, Datum value, Oid value_type)
{
	TypeCacheEntry		   *typcache;
	Size					datum_size;
	Size					segment_size;
	dsm_segment			   *segment;
	SpawnPartitionArgs	   *args;

	typcache = lookup_type_cache(value_type, 0);

	/* Calculate segment size */
	datum_size = datumGetSize(value, typcache->typbyval, typcache->typlen);
	segment_size = offsetof(SpawnPartitionArgs, value) + datum_size;

	segment = dsm_create(segment_size, 0);

	/* Initialize BGW args */
	args = (SpawnPartitionArgs *) dsm_segment_address(segment);

	args->userid = GetUserId();
	args->result = InvalidOid;
	args->dbid = MyDatabaseId;
	args->partitioned_table = relid;

#if PG_VERSION_NUM >= 90600
	/* Initialize args for BecomeLockGroupMember() */
	args->parallel_master_pgproc = MyProc;
	args->parallel_master_pid = MyProcPid;
#endif

	/* Write value-related stuff */
	args->value_type = value_type;
	args->value_size = datum_size;
	args->value_byval = typcache->typbyval;

	PackDatumToByteArray((void *) args->value, value,
						 datum_size, args->value_byval);

	return segment;
}

/*
 * Starts background worker that will create new partitions,
 * waits till it finishes the job and returns the result (new partition oid)
 *
 * NB: This function should not be called directly, use create_partitions() instead.
 */
Oid
create_partitions_for_value_bg_worker(Oid relid, Datum value, Oid value_type)
{
	dsm_segment			   *segment;
	dsm_handle				segment_handle;
	SpawnPartitionArgs	   *bgw_args;
	Oid						child_oid = InvalidOid;

	/* Create a dsm segment for the worker to pass arguments */
	segment = create_partitions_bg_worker_segment(relid, value, value_type);
	segment_handle = dsm_segment_handle(segment);
	bgw_args = (SpawnPartitionArgs *) dsm_segment_address(segment);

#if PG_VERSION_NUM >= 90600
	/* Become locking group leader */
	BecomeLockGroupLeader();
#endif

	/* Start worker and wait for it to finish */
	if (!start_bgworker(spawn_partitions_bgw,
						CppAsString(bgw_main_spawn_partitions),
						UInt32GetDatum(segment_handle),
						true))
	{
		start_bgworker_errmsg(spawn_partitions_bgw);
	}

	/* Save the result (partition Oid) */
	child_oid = bgw_args->result;

	/* Free dsm segment */
	dsm_detach(segment);

	if (child_oid == InvalidOid)
		ereport(ERROR,
				(errmsg("attempt to spawn new partitions of relation \"%s\" failed",
						get_rel_name_or_relid(relid)),
				 errhint("See server log for more details.")));

	return child_oid;
}

/*
 * Entry point for SpawnPartitionsWorker's process.
 */
void
bgw_main_spawn_partitions(Datum main_arg)
{
	dsm_handle				handle = DatumGetUInt32(main_arg);
	dsm_segment			   *segment;
	SpawnPartitionArgs	   *args;
	Datum					value;

	/* Establish signal handlers before unblocking signals. */
	pqsignal(SIGTERM, handle_sigterm);

	/* We're now ready to receive signals */
	BackgroundWorkerUnblockSignals();

	/* Create resource owner */
	CurrentResourceOwner = ResourceOwnerCreate(NULL, spawn_partitions_bgw);

	if (!handle)
		elog(ERROR, "%s: invalid dsm_handle [%u]",
			 spawn_partitions_bgw, MyProcPid);

	/* Attach to dynamic shared memory */
	if ((segment = dsm_attach(handle)) == NULL)
		elog(ERROR, "%s: cannot attach to segment [%u]",
			 spawn_partitions_bgw, MyProcPid);
	args = dsm_segment_address(segment);

#if PG_VERSION_NUM >= 90600
	/* Join locking group. If we can't join the group, quit */
	if (!BecomeLockGroupMember(args->parallel_master_pgproc,
							   args->parallel_master_pid))
		return;
#endif
	/* Establish connection and start transaction */

	BackgroundWorkerInitializeConnectionByOidCompat(args->dbid, args->userid);

	/* Start new transaction (syscache access etc.) */
	StartTransactionCommand();

	/* Initialize pg_pathman's local config */
	bg_worker_load_config(spawn_partitions_bgw);

	/* Upack Datum from segment to 'value' */
	UnpackDatumFromByteArray(&value,
							 args->value_size,
							 args->value_byval,
							 (const void *) args->value);

/* Print 'arg->value' for debug purposes */
#ifdef USE_ASSERT_CHECKING
	elog(LOG, "%s: arg->value is '%s' [%u]",
		 spawn_partitions_bgw,
		 DebugPrintDatum(value, args->value_type), MyProcPid);
#endif

	/* Create partitions and save the Oid of the last one */
	args->result = create_partitions_for_value_internal(args->partitioned_table,
														value, /* unpacked Datum */
														args->value_type,
														true); /* background woker */

	/* Finish transaction in an appropriate way */
	if (args->result == InvalidOid)
		AbortCurrentTransaction();
	else
		CommitTransactionCommand();

	dsm_detach(segment);
}


/*
 * -------------------------------------
 *  ConcurrentPartWorker implementation
 * -------------------------------------
 */

/* Free bgworker's CPS slot */
static void
free_cps_slot(int code, Datum arg)
{
	ConcurrentPartSlot *part_slot = (ConcurrentPartSlot *) DatumGetPointer(arg);

	cps_set_status(part_slot, CPS_FREE);
}

/*
 * Entry point for ConcurrentPartWorker's process.
 */
void
bgw_main_concurrent_part(Datum main_arg)
{
	ConcurrentPartSlot *part_slot;
	char			   *sql = NULL;
	int64				rows;
	bool				failed;
	int					failures_count = 0;
	LOCKMODE			lockmode = RowExclusiveLock;

	/* Update concurrent part slot */
	part_slot = &concurrent_part_slots[DatumGetInt32(main_arg)];
	part_slot->pid = MyProcPid;

	/* Establish atexit callback that will fre CPS slot */
	on_proc_exit(free_cps_slot, PointerGetDatum(part_slot));

	/* Establish signal handlers before unblocking signals */
	pqsignal(SIGTERM, handle_sigterm);

	/* We're now ready to receive signals */
	BackgroundWorkerUnblockSignals();

	/* Create resource owner */
	CurrentResourceOwner = ResourceOwnerCreate(NULL, concurrent_part_bgw);

	/* Disable auto partition propagation */
	SetAutoPartitionEnabled(false);

	/* Establish connection and start transaction */
	BackgroundWorkerInitializeConnectionByOidCompat(part_slot->dbid, part_slot->userid);

	/* Initialize pg_pathman's local config */
	StartTransactionCommand();
	bg_worker_load_config(concurrent_part_bgw);
	CommitTransactionCommand();

	/* Do the job */
	do
	{
		MemoryContext old_mcxt;

		Oid		types[2]	= { OIDOID,				INT4OID };
		Datum	vals[2]		= { part_slot->relid,	part_slot->batch_size };

		bool	rel_locked = false;

		/* Reset loop variables */
		failed = false;
		rows = 0;

		CHECK_FOR_INTERRUPTS();

		/* Start new transaction (syscache access etc.) */
		StartTransactionCommand();

		/* We'll need this to recover from errors */
		old_mcxt = CurrentMemoryContext;

		if (SPI_connect() != SPI_OK_CONNECT)
			elog(ERROR, "could not connect using SPI");

		PushActiveSnapshot(GetTransactionSnapshot());

		/* Prepare the query if needed */
		if (sql == NULL)
		{
			MemoryContext	current_mcxt;

			/*
			 * Allocate SQL query in TopPathmanContext because current
			 * context will be destroyed after transaction finishes
			 */
			current_mcxt = MemoryContextSwitchTo(TopPathmanContext);
			sql = psprintf("SELECT %s._partition_data_concurrent($1::oid, p_limit:=$2)",
						   get_namespace_name(get_pathman_schema()));
			MemoryContextSwitchTo(current_mcxt);
		}

		/* Exec ret = _partition_data_concurrent() */
		PG_TRY();
		{
			int		ret;
			bool	isnull;

			/* Lock relation for DELETE and INSERT */
			if (!ConditionalLockRelationOid(part_slot->relid, lockmode))
			{
				elog(ERROR, "could not take lock on relation %u", part_slot->relid);
			}

			/* Great, now relation is locked */
			rel_locked = true;
			(void) rel_locked; /* mute clang analyzer */

			/* Make sure that relation exists */
			if (!SearchSysCacheExists1(RELOID, ObjectIdGetDatum(part_slot->relid)))
			{
				/* Exit after we raise ERROR */
				failures_count = PART_WORKER_MAX_ATTEMPTS;
				(void) failures_count; /* mute clang analyzer */

				elog(ERROR, "relation %u does not exist", part_slot->relid);
			}

			/* Make sure that relation has partitions */
			if (!has_pathman_relation_info(part_slot->relid))
			{
				/* Exit after we raise ERROR */
				failures_count = PART_WORKER_MAX_ATTEMPTS;
				(void) failures_count; /* mute clang analyzer */

				elog(ERROR, "relation \"%s\" is not partitioned",
					 get_rel_name(part_slot->relid));
			}

			/* Call concurrent partitioning function */
			ret = SPI_execute_with_args(sql, 2, types, vals, NULL, false, 0);
			if (ret == SPI_OK_SELECT)
			{
				TupleDesc	tupdesc	= SPI_tuptable->tupdesc;
				HeapTuple	tuple	= SPI_tuptable->vals[0];

				/* There should be 1 result at most */
				Assert(SPI_processed == 1);

				/* Extract number of processed rows */
				rows = DatumGetInt64(SPI_getbinval(tuple, tupdesc, 1, &isnull));
				Assert(TupleDescAttr(tupdesc, 0)->atttypid == INT8OID); /* check type */
				Assert(!isnull); /* ... and ofc it must not be NULL */
			}
			/* Else raise generic error */
			else elog(ERROR, "partitioning function returned %u", ret);

			/* Finally, unlock our partitioned table */
			UnlockRelationOid(part_slot->relid, lockmode);
		}
		PG_CATCH();
		{
			/*
			 * The most common exception we can catch here is a deadlock with
			 * concurrent user queries. Check that attempts count doesn't exceed
			 * some reasonable value.
			 */
			ErrorData *error;

			/* Unlock relation if we caught ERROR too early */
			if (rel_locked)
				UnlockRelationOid(part_slot->relid, lockmode);

			/* Increase number of failures and set 'failed' status */
			failures_count++;
			failed = true;

			/* Switch to the original context & copy edata */
			MemoryContextSwitchTo(old_mcxt);
			error = CopyErrorData();
			FlushErrorState();

			/* Print message for this BGWorker to server log */
			ereport(LOG,
					(errmsg("%s: %s", concurrent_part_bgw, error->message),
					 errdetail("attempt: %d/%d, sleep time: %.2f",
							   failures_count,
							   PART_WORKER_MAX_ATTEMPTS,
							   (float) part_slot->sleep_time)));

			/* Finally, free error data */
			FreeErrorData(error);
		}
		PG_END_TRY();

		SPI_finish();
		PopActiveSnapshot();

		/* We've run out of attempts, exit */
		if (failures_count >= PART_WORKER_MAX_ATTEMPTS)
		{
			AbortCurrentTransaction();

			/* Mark slot as FREE */
			cps_set_status(part_slot, CPS_FREE);

			elog(LOG,
				 "concurrent partitioning worker has canceled the task because "
				 "maximum amount of attempts (%d) had been exceeded, "
				 "see the error message below",
				 PART_WORKER_MAX_ATTEMPTS);

			return; /* time to exit */
		}

		/* Failed this time, wait */
		else if (failed)
		{
			/* Abort transaction */
			AbortCurrentTransaction();

			/* Sleep for a specified amount of time (default 1s) */
			DirectFunctionCall1(pg_sleep, Float8GetDatum(part_slot->sleep_time));
		}

		/* Everything is fine */
		else
		{
			/* Commit transaction and reset 'failures_count' */
			CommitTransactionCommand();
			failures_count = 0;

			/* Add rows to total_rows */
			SpinLockAcquire(&part_slot->mutex);
			part_slot->total_rows += rows;
			SpinLockRelease(&part_slot->mutex);

#ifdef USE_ASSERT_CHECKING
			/* Report debug message */
			elog(DEBUG1, "%s: "
						 "relocated" INT64_FORMAT "rows, "
						 "total: " INT64_FORMAT,
				 concurrent_part_bgw, rows, part_slot->total_rows);
#endif
		}

		/* If other backend requested to stop us, quit */
		if (cps_check_status(part_slot) == CPS_STOPPING)
			break;
	}
	while(rows > 0 || failed); /* do while there's still rows to be relocated */
}


/*
 * -----------------------------------------------
 *  Public interface for the ConcurrentPartWorker
 * -----------------------------------------------
 */

/*
 * Start concurrent partitioning worker to redistribute rows.
 * NOTE: this function returns immediately.
 */
Datum
partition_table_concurrently(PG_FUNCTION_ARGS)
{
	Oid				relid = PG_GETARG_OID(0);
	int32			batch_size = PG_GETARG_INT32(1);
	float8			sleep_time = PG_GETARG_FLOAT8(2);
	int				empty_slot_idx = -1,		/* do we have a slot for BGWorker? */
					i;
	TransactionId	rel_xmin;
	LOCKMODE		lockmode = ShareUpdateExclusiveLock;

	/* Check batch_size */
	if (batch_size < 1 || batch_size > 10000)
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("'batch_size' should not be less than 1"
							   " or greater than 10000")));

	/* Check sleep_time */
	if (sleep_time < 0.5)
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("'sleep_time' should not be less than 0.5")));

	/* Prevent concurrent function calls */
	LockRelationOid(relid, lockmode);

	/* Check if relation is a partitioned table */
	if (!has_pathman_relation_info(relid))
		shout_if_prel_is_invalid(relid, NULL, PT_ANY);

	/* Check that partitioning operation result is visible */
	if (pathman_config_contains_relation(relid, NULL, NULL, &rel_xmin, NULL))
	{
		if (!xact_object_is_visible(rel_xmin))
			ereport(ERROR, (errmsg("cannot start %s", concurrent_part_bgw),
							errdetail("table is being partitioned now")));
	}
	else ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("relation \"%s\" is not partitioned",
								get_rel_name_or_relid(relid))));

	/*
	 * Look for an empty slot and also check that a concurrent
	 * partitioning operation for this table hasn't started yet.
	 */
	for (i = 0; i < PART_WORKER_SLOTS; i++)
	{
		ConcurrentPartSlot *cur_slot = &concurrent_part_slots[i];
		bool				keep_this_lock = false;

		/* Lock current slot */
		SpinLockAcquire(&cur_slot->mutex);

		/* Should we take this slot into account? (it should be FREE) */
		if (empty_slot_idx < 0 && cur_slot->worker_status == CPS_FREE)
		{
			empty_slot_idx = i;		/* yes, remember this slot */
			keep_this_lock = true;	/* also don't unlock it */
		}

		/* Oops, looks like we already have BGWorker for this table */
		if (cur_slot->relid == relid &&
			cur_slot->dbid == MyDatabaseId &&
			cur_slot->worker_status != CPS_FREE)
		{
			/* Unlock current slot */
			SpinLockRelease(&cur_slot->mutex);

			/* Release borrowed slot for new BGWorker too */
			if (empty_slot_idx >= 0 && empty_slot_idx != i)
				SpinLockRelease(&concurrent_part_slots[empty_slot_idx].mutex);

			ereport(ERROR, (errmsg("table \"%s\" is already being partitioned",
								   get_rel_name(relid))));
		}

		/* Normally we don't want to keep it */
		if (!keep_this_lock)
			SpinLockRelease(&cur_slot->mutex);
	}

	/* Looks like we could not find an empty slot */
	if (empty_slot_idx < 0)
		ereport(ERROR, (errcode(ERRCODE_CONFIGURATION_LIMIT_EXCEEDED),
						errmsg("no empty worker slots found"),
						errhint("consider increasing max_worker_processes")));
	else
	{
		/* Initialize concurrent part slot */
		InitConcurrentPartSlot(&concurrent_part_slots[empty_slot_idx],
							   GetUserId(), CPS_WORKING, MyDatabaseId,
							   relid, batch_size, sleep_time);

		/* Now we can safely unlock slot for new BGWorker */
		SpinLockRelease(&concurrent_part_slots[empty_slot_idx].mutex);
	}

	/* Start worker (we should not wait) */
	if (!start_bgworker(concurrent_part_bgw,
						CppAsString(bgw_main_concurrent_part),
						Int32GetDatum(empty_slot_idx),
						false))
	{
		/* Couldn't start, free CPS slot */
		cps_set_status(&concurrent_part_slots[empty_slot_idx], CPS_FREE);

		start_bgworker_errmsg(concurrent_part_bgw);
	}

	/* Tell user everything's fine */
	elog(NOTICE,
		 "worker started, you can stop it "
		 "with the following command: select %s.%s('%s');",
		 get_namespace_name(get_pathman_schema()),
		 CppAsString(stop_concurrent_part_task),
		 get_rel_name(relid));

	/* We don't need this lock anymore */
	UnlockRelationOid(relid, lockmode);

	PG_RETURN_VOID();
}

/*
 * Return list of active concurrent partitioning workers.
 * NOTE: this is a set-returning-function (SRF).
 */
Datum
show_concurrent_part_tasks_internal(PG_FUNCTION_ARGS)
{
	FuncCallContext		   *funcctx;
	active_workers_cxt	   *userctx;
	int						i;

	/*
	 * Initialize tuple descriptor & function call context.
	 */
	if (SRF_IS_FIRSTCALL())
	{
		TupleDesc			tupdesc;
		MemoryContext		old_mcxt;

		funcctx = SRF_FIRSTCALL_INIT();

		old_mcxt = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		userctx = (active_workers_cxt *) palloc(sizeof(active_workers_cxt));
		userctx->cur_idx = 0;

		/* Create tuple descriptor */
		tupdesc = CreateTemplateTupleDesc(Natts_pathman_cp_tasks, false);

		TupleDescInitEntry(tupdesc, Anum_pathman_cp_tasks_userid,
						   "userid", REGROLEOID, -1, 0);
		TupleDescInitEntry(tupdesc, Anum_pathman_cp_tasks_pid,
						   "pid", INT4OID, -1, 0);
		TupleDescInitEntry(tupdesc, Anum_pathman_cp_tasks_dbid,
						   "dbid", OIDOID, -1, 0);
		TupleDescInitEntry(tupdesc, Anum_pathman_cp_tasks_relid,
						   "relid", REGCLASSOID, -1, 0);
		TupleDescInitEntry(tupdesc, Anum_pathman_cp_tasks_processed,
						   "processed", INT8OID, -1, 0);
		TupleDescInitEntry(tupdesc, Anum_pathman_cp_tasks_status,
						   "status", TEXTOID, -1, 0);

		funcctx->tuple_desc = BlessTupleDesc(tupdesc);
		funcctx->user_fctx = (void *) userctx;

		MemoryContextSwitchTo(old_mcxt);
	}

	funcctx = SRF_PERCALL_SETUP();
	userctx = (active_workers_cxt *) funcctx->user_fctx;

	/* Iterate through worker slots */
	for (i = userctx->cur_idx; i < PART_WORKER_SLOTS; i++)
	{
		ConcurrentPartSlot *cur_slot = &concurrent_part_slots[i],
							slot_copy;
		HeapTuple			htup = NULL;

		/* Copy slot to process local memory */
		SpinLockAcquire(&cur_slot->mutex);
		memcpy(&slot_copy, cur_slot, sizeof(ConcurrentPartSlot));
		SpinLockRelease(&cur_slot->mutex);

		if (slot_copy.worker_status != CPS_FREE)
		{
			Datum		values[Natts_pathman_cp_tasks];
			bool		isnull[Natts_pathman_cp_tasks] = { 0 };

			values[Anum_pathman_cp_tasks_userid - 1]	= slot_copy.userid;
			values[Anum_pathman_cp_tasks_pid - 1]		= slot_copy.pid;
			values[Anum_pathman_cp_tasks_dbid - 1]		= slot_copy.dbid;
			values[Anum_pathman_cp_tasks_relid - 1]		= slot_copy.relid;

			/* Record processed rows */
			values[Anum_pathman_cp_tasks_processed - 1]	=
					Int64GetDatum(slot_copy.total_rows);

			/* Now build a status string */
			values[Anum_pathman_cp_tasks_status - 1] =
					CStringGetTextDatum(cps_print_status(slot_copy.worker_status));

			/* Form output tuple */
			htup = heap_form_tuple(funcctx->tuple_desc, values, isnull);

			/* Switch to next worker */
			userctx->cur_idx = i + 1;
		}

		/* Return tuple if needed */
		if (htup)
			SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(htup));
	}

	SRF_RETURN_DONE(funcctx);
}

/*
 * Stop the specified concurrent partitioning worker.
 * NOTE: worker will stop after it finishes a batch.
 */
Datum
stop_concurrent_part_task(PG_FUNCTION_ARGS)
{
	Oid		relid = PG_GETARG_OID(0);
	bool	worker_found = false;
	int		i;

	for (i = 0; i < PART_WORKER_SLOTS && !worker_found; i++)
	{
		ConcurrentPartSlot *cur_slot = &concurrent_part_slots[i];

		SpinLockAcquire(&cur_slot->mutex);

		if (cur_slot->worker_status != CPS_FREE &&
			cur_slot->relid == relid &&
			cur_slot->dbid == MyDatabaseId)
		{
			/* Change worker's state & set 'worker_found' */
			cur_slot->worker_status = CPS_STOPPING;
			worker_found = true;
		}

		SpinLockRelease(&cur_slot->mutex);
	}

	if (worker_found)
	{
		elog(NOTICE, "worker will stop after it finishes current batch");
		PG_RETURN_BOOL(true);
	}
	else
	{
		elog(ERROR, "cannot find worker for relation \"%s\"",
			 get_rel_name_or_relid(relid));

		PG_RETURN_BOOL(false); /* keep compiler happy */
	}
}
