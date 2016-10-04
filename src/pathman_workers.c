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
#include "pathman_workers.h"
#include "relation_info.h"
#include "utils.h"

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
#include "utils/builtins.h"
#include "utils/datum.h"
#include "utils/memutils.h"
#include "utils/lsyscache.h"
#include "utils/typcache.h"
#include "utils/resowner.h"
#include "utils/snapmgr.h"



/* Declarations for ConcurrentPartWorker */
PG_FUNCTION_INFO_V1( partition_table_concurrently );
PG_FUNCTION_INFO_V1( show_concurrent_part_tasks_internal );
PG_FUNCTION_INFO_V1( stop_concurrent_part_task );


static void handle_sigterm(SIGNAL_ARGS);
static void bg_worker_load_config(const char *bgw_name);
static void start_bg_worker(const char bgworker_name[BGW_MAXLEN],
							bgworker_main_type bgw_main_func,
							Datum bgw_arg, bool wait_for_shutdown);

static void bgw_main_spawn_partitions(Datum main_arg);
static void bgw_main_concurrent_part(Datum main_arg);


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
static void
start_bg_worker(const char bgworker_name[BGW_MAXLEN],
				bgworker_main_type bgw_main_func,
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
	memcpy(worker.bgw_name, bgworker_name, BGW_MAXLEN);
	worker.bgw_flags		= BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
	worker.bgw_start_time	= BgWorkerStart_RecoveryFinished;
	worker.bgw_restart_time	= BGW_NEVER_RESTART;
	worker.bgw_main			= bgw_main_func;
	worker.bgw_main_arg		= bgw_arg;
	worker.bgw_notify_pid	= MyProcPid;

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
		case BGW_COULD_NOT_START:
			elog(ERROR, "Unable to create background %s for pg_pathman",
				 bgworker_name);
			break;

		case BGW_PM_DIED:
			ereport(ERROR,
					(errmsg("Postmaster died during the pg_pathman background worker process"),
					errhint("More details may be available in the server log.")));
			break;

		default:
			break;
	}
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

	args->userid = GetAuthenticatedUserId();

	args->result = InvalidOid;
	args->dbid = MyDatabaseId;
	args->partitioned_table = relid;

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
create_partitions_bg_worker(Oid relid, Datum value, Oid value_type)
{
	dsm_segment			   *segment;
	dsm_handle				segment_handle;
	SpawnPartitionArgs	   *bgw_args;
	Oid						child_oid = InvalidOid;

	/* Create a dsm segment for the worker to pass arguments */
	segment = create_partitions_bg_worker_segment(relid, value, value_type);
	segment_handle = dsm_segment_handle(segment);
	bgw_args = (SpawnPartitionArgs *) dsm_segment_address(segment);

	/* Start worker and wait for it to finish */
	start_bg_worker(spawn_partitions_bgw,
					bgw_main_spawn_partitions,
					UInt32GetDatum(segment_handle),
					true);

	/* Save the result (partition Oid) */
	child_oid = bgw_args->result;

	/* Free dsm segment */
	dsm_detach(segment);

	if (child_oid == InvalidOid)
		elog(ERROR,
			 "Attempt to append new partitions to relation \"%s\" failed",
			 get_rel_name_or_relid(relid));

	return child_oid;
}

/*
 * Entry point for SpawnPartitionsWorker's process.
 */
static void
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

	/* Establish connection and start transaction */
	BackgroundWorkerInitializeConnectionByOid(args->dbid, args->userid);

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
	args->result = create_partitions_internal(args->partitioned_table,
											  value, /* unpacked Datum */
											  args->value_type);

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

/*
 * Entry point for ConcurrentPartWorker's process.
 */
static void
bgw_main_concurrent_part(Datum main_arg)
{
	int					rows;
	bool				failed;
	int					failures_count = 0;
	char			   *sql = NULL;
	ConcurrentPartSlot *part_slot;

	/* Establish signal handlers before unblocking signals. */
	pqsignal(SIGTERM, handle_sigterm);

	/* We're now ready to receive signals */
	BackgroundWorkerUnblockSignals();

	/* Create resource owner */
	CurrentResourceOwner = ResourceOwnerCreate(NULL, concurrent_part_bgw);

	/* Update concurrent part slot */
	part_slot = &concurrent_part_slots[DatumGetInt32(main_arg)];
	part_slot->pid = MyProcPid;

	/* Disable auto partition propagation */
	SetAutoPartitionEnabled(false);

	/* Establish connection and start transaction */
	BackgroundWorkerInitializeConnectionByOid(part_slot->dbid, part_slot->userid);

	/* Initialize pg_pathman's local config */
	StartTransactionCommand();
	bg_worker_load_config(concurrent_part_bgw);
	CommitTransactionCommand();

	/* Do the job */
	do
	{
		MemoryContext	old_mcxt;

		Oid		types[2]	= { OIDOID,				INT4OID };
		Datum	vals[2]		= { part_slot->relid,	part_slot->batch_size };
		bool	nulls[2]	= { false,				false };

		/* Reset loop variables */
		failed = false;
		rows = 0;

		/* Start new transaction (syscache access etc.) */
		StartTransactionCommand();

		/* We'll need this to recover from errors */
		old_mcxt = CurrentMemoryContext;

		SPI_connect();
		PushActiveSnapshot(GetTransactionSnapshot());

		/* Prepare the query if needed */
		if (sql == NULL)
		{
			MemoryContext	current_mcxt;

			/*
			 * Allocate as SQL query in top memory context because current
			 * context will be destroyed after transaction finishes
			 */
			current_mcxt = MemoryContextSwitchTo(TopMemoryContext);
			sql = psprintf("SELECT %s._partition_data_concurrent($1::oid, p_limit:=$2)",
						   get_namespace_name(get_pathman_schema()));
			MemoryContextSwitchTo(current_mcxt);
		}

		/* Exec ret = _partition_data_concurrent() */
		PG_TRY();
		{
			int		ret;
			bool	isnull;

			ret = SPI_execute_with_args(sql, 2, types, vals, nulls, false, 0);
			if (ret == SPI_OK_SELECT)
			{
				TupleDesc	tupdesc = SPI_tuptable->tupdesc;
				HeapTuple	tuple = SPI_tuptable->vals[0];

				Assert(SPI_processed == 1); /* there should be 1 result at most */

				rows = DatumGetInt32(SPI_getbinval(tuple, tupdesc, 1, &isnull));

				Assert(!isnull); /* ... and ofc it must not be NULL */
			}
		}
		PG_CATCH();
		{
			ErrorData  *error;
			char	   *sleep_time_str;

			/* Switch to the original context & copy edata */
			MemoryContextSwitchTo(old_mcxt);
			error = CopyErrorData();
			FlushErrorState();

			/* Print messsage for this BGWorker to server log */
			sleep_time_str = datum_to_cstring(Float8GetDatum(part_slot->sleep_time),
											  FLOAT8OID);
			failures_count++;
			ereport(LOG,
					(errmsg("%s: %s", concurrent_part_bgw, error->message),
					 errdetail("attempt: %d/%d, sleep time: %s",
							   failures_count,
							   PART_WORKER_MAX_ATTEMPTS,
							   sleep_time_str)));
			pfree(sleep_time_str); /* free the time string */

			FreeErrorData(error);

			/*
			 * The most common exception we can catch here is a deadlock with
			 * concurrent user queries. Check that attempts count doesn't exceed
			 * some reasonable value
			 */
			if (failures_count >= PART_WORKER_MAX_ATTEMPTS)
			{
				/* Mark slot as FREE */
				cps_set_status(part_slot, CPS_FREE);

				elog(LOG,
					 "concurrent partitioning worker has canceled the task because "
					 "maximum amount of attempts (%d) had been exceeded, "
					 "see the error message below",
					 PART_WORKER_MAX_ATTEMPTS);

				return; /* exit quickly */
			}

			/* Set 'failed' flag */
			failed = true;
		}
		PG_END_TRY();

		SPI_finish();
		PopActiveSnapshot();

		if (failed)
		{
			/* Abort transaction and sleep for a second */
			AbortCurrentTransaction();
			DirectFunctionCall1(pg_sleep, Float8GetDatum(part_slot->sleep_time));
		}
		else
		{
			/* Commit transaction and reset 'failures_count' */
			CommitTransactionCommand();
			failures_count = 0;

			/* Add rows to total_rows */
			SpinLockAcquire(&part_slot->mutex);
			part_slot->total_rows += rows;
/* Report debug message */
#ifdef USE_ASSERT_CHECKING
			elog(DEBUG1, "%s: relocated %d rows, total: %lu [%u]",
				 concurrent_part_bgw, rows, part_slot->total_rows, MyProcPid);
#endif
			SpinLockRelease(&part_slot->mutex);
		}

		/* If other backend requested to stop us, quit */
		if (cps_check_status(part_slot) == CPS_STOPPING)
			break;
	}
	while(rows > 0 || failed);  /* do while there's still rows to be relocated */

	/* Reclaim the resources */
	pfree(sql);

	/* Mark slot as FREE */
	cps_set_status(part_slot, CPS_FREE);
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
	Oid		relid = PG_GETARG_OID(0);
	int		empty_slot_idx = -1,		/* do we have a slot for BGWorker? */
			i;

	/* Check if relation is a partitioned table */
	shout_if_prel_is_invalid(relid,
							 /* We also lock the parent relation */
							 get_pathman_relation_info_after_lock(relid, true),
							 /* Partitioning type does not matter here */
							 PT_INDIFFERENT);
	/*
	 * Look for an empty slot and also check that a concurrent
	 * partitioning operation for this table hasn't been started yet
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

			elog(ERROR,
				 "table \"%s\" is already being partitioned",
				 get_rel_name(relid));
		}

		/* Normally we don't want to keep it */
		if (!keep_this_lock)
			SpinLockRelease(&cur_slot->mutex);
	}

	/* Looks like we could not find an empty slot */
	if (empty_slot_idx < 0)
		elog(ERROR, "no empty worker slots found");
	else
	{
		/* Initialize concurrent part slot */
		InitConcurrentPartSlot(&concurrent_part_slots[empty_slot_idx],
							   GetAuthenticatedUserId(), CPS_WORKING,
							   MyDatabaseId, relid, 1000, 1.0);

		/* Now we can safely unlock slot for new BGWorker */
		SpinLockRelease(&concurrent_part_slots[empty_slot_idx].mutex);
	}

	/* Start worker (we should not wait) */
	start_bg_worker(concurrent_part_bgw,
					bgw_main_concurrent_part,
					Int32GetDatum(empty_slot_idx),
					false);

	/* Tell user everything's fine */
	elog(NOTICE,
		 "worker started, you can stop it "
		 "with the following command: select %s('%s');",
		 CppAsString(stop_concurrent_part_task),
		 get_rel_name(relid));

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
						   "processed", INT4OID, -1, 0);
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
		ConcurrentPartSlot *cur_slot = &concurrent_part_slots[i];
		HeapTuple			htup = NULL;

		HOLD_INTERRUPTS();
		SpinLockAcquire(&cur_slot->mutex);

		if (cur_slot->worker_status != CPS_FREE)
		{
			Datum		values[Natts_pathman_cp_tasks];
			bool		isnull[Natts_pathman_cp_tasks] = { 0 };

			values[Anum_pathman_cp_tasks_userid - 1]	= cur_slot->userid;
			values[Anum_pathman_cp_tasks_pid - 1]		= cur_slot->pid;
			values[Anum_pathman_cp_tasks_dbid - 1]		= cur_slot->dbid;
			values[Anum_pathman_cp_tasks_relid - 1]		= cur_slot->relid;
			values[Anum_pathman_cp_tasks_processed - 1]	= cur_slot->total_rows;

			/* Now build a status string */
			switch(cur_slot->worker_status)
			{
				case CPS_WORKING:
					values[Anum_pathman_cp_tasks_status - 1] =
							PointerGetDatum(cstring_to_text("working"));
					break;

				case CPS_STOPPING:
					values[Anum_pathman_cp_tasks_status - 1] =
							PointerGetDatum(cstring_to_text("stopping"));
					break;

				default:
					values[Anum_pathman_cp_tasks_status - 1] =
							PointerGetDatum(cstring_to_text("[unknown]"));
			}

			/* Form output tuple */
			htup = heap_form_tuple(funcctx->tuple_desc, values, isnull);

			/* Switch to next worker */
			userctx->cur_idx = i + 1;
		}

		SpinLockRelease(&cur_slot->mutex);
		RESUME_INTERRUPTS();

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

		HOLD_INTERRUPTS();
		SpinLockAcquire(&cur_slot->mutex);

		if (cur_slot->worker_status != CPS_FREE &&
			cur_slot->relid == relid &&
			cur_slot->dbid == MyDatabaseId)
		{
			elog(NOTICE, "worker will stop after it finishes current batch");

			/* Change worker's state & set 'worker_found' */
			cur_slot->worker_status = CPS_STOPPING;
			worker_found = true;
		}

		SpinLockRelease(&cur_slot->mutex);
		RESUME_INTERRUPTS();
	}

	if (worker_found)
		PG_RETURN_BOOL(true);
	else
	{
		elog(ERROR, "cannot find worker for relation \"%s\"",
			 get_rel_name_or_relid(relid));

		PG_RETURN_BOOL(false); /* keep compiler happy */
	}
}
