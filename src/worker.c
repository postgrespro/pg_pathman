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

#include "pathman.h"
#include "init.h"
#include "utils.h"

#include "access/xact.h"
#include "catalog/pg_type.h"
#include "executor/spi.h"
#include "miscadmin.h"
#include "postmaster/bgworker.h"
#include "storage/dsm.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "utils/datum.h"
#include "utils/typcache.h"
#include "utils/lsyscache.h"
#include "utils/resowner.h"
#include "utils/builtins.h"
#include "utils/snapmgr.h"
#include "funcapi.h"

#define WORKER_SLOTS 10

static void bg_worker_load_config(const char *bgw_name);
static void create_partitions_bg_worker_main(Datum main_arg);
static void partition_data_bg_worker_main(Datum main_arg);
static void handle_sigterm(SIGNAL_ARGS);

PG_FUNCTION_INFO_V1( partition_data_worker );
PG_FUNCTION_INFO_V1( active_workers );
PG_FUNCTION_INFO_V1( stop_worker );

static const char *create_partitions_bgw = "CreatePartitionsWorker";

/*
 * Store args, result and execution status of CreatePartitionsWorker.
 */
typedef struct
{
	Oid		result;		/* target partition */
	Oid		dbid;
	Oid		partitioned_table;

	/* Type will help us to work with Datum */
	Oid		value_type;
	Size	value_size;
	bool	value_byval;

	/* Store Datum as flexible array */
	uint8	value[FLEXIBLE_ARRAY_MEMBER];
} PartitionArgs;

typedef enum WorkerStatus
{
	WS_FREE = 0,
	WS_WORKING,
	WS_STOPPING
} WorkerStatus;

typedef struct PartitionDataArgs
{
	WorkerStatus	status;
	Oid			dbid;
	Oid			relid;
	uint32		batch_size;
	uint32		batch_count;
	pid_t		pid;
	size_t		total_rows;
} PartitionDataArgs;

PartitionDataArgs *slots;

/*
 * Initialize shared memory
 */
void
create_worker_slots()
{
	bool	found;
	size_t	size = get_worker_slots_size();

	slots = (PartitionDataArgs *)
		ShmemInitStruct("worker slots", size ,&found);

	if (!found)
		memset(slots, 0, size);
}

Size
get_worker_slots_size(void)
{
	return sizeof(PartitionDataArgs) * WORKER_SLOTS;
}

/*
 * Useful datum packing\unpacking functions for BGW.
 */

static void
PackDatumToByteArray(void *byte_array, Datum datum, Size datum_size, bool typbyval)
{
	if (typbyval)
		/* We have to copy all Datum's bytes */
		datum_size = Max(sizeof(Datum), datum_size);

	memcpy((void *) byte_array,
		   (const void *) (typbyval ?
							   (Pointer) &datum :		/* treat Datum as byte array */
							   DatumGetPointer(datum)),	/* extract pointer to data */
		   datum_size);
}

static void
UnpackDatumFromByteArray(Datum *datum, Size datum_size, bool typbyval,
						 const void *byte_array)
{
	void *dst;

	if (typbyval)
	{
		/* Write Data to Datum directly */
		dst = datum;

		/* We have to copy all Datum's bytes */
		datum_size = Max(sizeof(Datum), datum_size);
	}
	else
	{
		/* Allocate space for Datum's internals */
		dst = palloc(datum_size);

		/* Save pointer to Datum */
		*datum = PointerGetDatum(dst);
	}

	memcpy(dst, byte_array, datum_size);
}

/*
 * Common function to start background worker
 */
static void
start_bg_worker(char name[BGW_MAXLEN],
				bgworker_main_type main_func,
				uint32 arg,
				bool wait)
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
	memcpy(worker.bgw_name, name, BGW_MAXLEN);
	worker.bgw_flags = BGWORKER_SHMEM_ACCESS |
		BGWORKER_BACKEND_DATABASE_CONNECTION;
	worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
	worker.bgw_restart_time = BGW_NEVER_RESTART;
	worker.bgw_main = main_func;
	worker.bgw_main_arg = arg;
	worker.bgw_notify_pid = MyProcPid;

	/* Start dynamic worker */
	bgw_started = RegisterDynamicBackgroundWorker(&worker, &bgw_handle);
	HandleError(bgw_started == false, BGW_COULD_NOT_START);

	/* Wait till the worker starts */
	bgw_status = WaitForBackgroundWorkerStartup(bgw_handle, &pid);
	HandleError(bgw_status == BGWH_POSTMASTER_DIED, BGW_PM_DIED);

	// elog(NOTICE, "worker pid: %u", pid);
	// sleep(30);

	if(wait)
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
			elog(ERROR, "Unable to create background worker for pg_pathman");
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
 * Initialize pg_pathman's local config in BGW process.
 */
static void
bg_worker_load_config(const char *bgw_name)
{
	load_config();
	elog(LOG, "%s: loaded pg_pathman's config [%u]",
		 bgw_name, MyProcPid);
}

/*
 * Create args segment for partitions bgw.
 */
static dsm_segment *
create_partitions_bg_worker_segment(Oid relid, Datum value, Oid value_type)
{
	TypeCacheEntry	   *typcache;
	Size				datum_size;
	Size				segment_size;
	dsm_segment		   *segment;
	PartitionArgs	   *args;

	typcache = lookup_type_cache(value_type, 0);

	/* Calculate segment size */
	datum_size = datumGetSize(value, typcache->typbyval, typcache->typlen);
	segment_size = offsetof(PartitionArgs, value) + datum_size;

	segment = dsm_create(segment_size, 0);

	/* Initialize BGW args */
	args = (PartitionArgs *) dsm_segment_address(segment);
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
	PartitionArgs		   *args;
	Oid						child_oid = InvalidOid;

	/* Create a dsm segment for the worker to pass arguments */
	segment = create_partitions_bg_worker_segment(relid, value, value_type);
	segment_handle = dsm_segment_handle(segment);
	args = (PartitionArgs *) dsm_segment_address(segment);

	/* Start worker and wait for it to finish */
	start_bg_worker("create partitions worker",
					create_partitions_bg_worker_main,
					UInt32GetDatum(segment_handle),
					true);

	/* Save the result (partition Oid) */
	child_oid = args->result;

	/* Free dsm segment */
	dsm_detach(segment);

	if (child_oid == InvalidOid)
		elog(ERROR,
			 "Attempt to append new partitions to relation \"%s\" failed",
			 get_rel_name_or_relid(relid));

	return child_oid;
}

/*
 * Main worker routine. Accepts dsm_handle as an argument
 */
static void
create_partitions_bg_worker_main(Datum main_arg)
{
	dsm_handle		handle = DatumGetUInt32(main_arg);
	dsm_segment	   *segment;
	PartitionArgs   *args;
	Datum			value;

	/* Create resource owner */
	CurrentResourceOwner = ResourceOwnerCreate(NULL, create_partitions_bgw);

	if (!handle)
		elog(ERROR, "%s: invalid dsm_handle [%u]",
			 create_partitions_bgw, MyProcPid);

	/* Attach to dynamic shared memory */
	if ((segment = dsm_attach(handle)) == NULL)
		elog(ERROR, "%s: cannot attach to segment [%u]",
			 create_partitions_bgw, MyProcPid);
	args = dsm_segment_address(segment);

	/* Establish connection and start transaction */
	BackgroundWorkerInitializeConnectionByOid(args->dbid, InvalidOid);

	StartTransactionCommand();

	/* Initialize pg_pathman's local config */
	bg_worker_load_config(create_partitions_bgw);

	/* Upack Datum from segment to 'value' */
	UnpackDatumFromByteArray(&value,
							 args->value_size,
							 args->value_byval,
							 (const void *) args->value);

#ifdef USE_ASSERT_CHECKING
	elog(LOG, "%s: arg->value is '%s' [%u]",
		 create_partitions_bgw,
		 DebugPrintDatum(value, args->value_type), MyProcPid);
#endif

	/* Create partitions */
	args->result = create_partitions_internal(args->partitioned_table,
											  value, /* unpacked Datum */
											  args->value_type);

	if (args->result == InvalidOid)
		AbortCurrentTransaction();
	else
		CommitTransactionCommand();

	dsm_detach(segment);
}

/*
 * Starts background worker that redistributes data. Function returns
 * immediately
 */
Datum
partition_data_worker( PG_FUNCTION_ARGS )
{
	Oid		relid = PG_GETARG_OID(0);
	int		empty_slot_idx = -1;
	int		i;
	PartitionDataArgs  *args = NULL;

	/* TODO: lock would be nice */

	/* Check if relation is a partitioned table */
	if (get_pathman_relation_info(relid) == NULL)
		elog(ERROR,
			 "Relation '%s' isn't partitioned by pg_pathman",
			 get_rel_name(relid));

	/*
	 * Look for empty slot and also check that partitioning data for this table
	 * hasn't already starded
	 */
	for (i=0; i<WORKER_SLOTS; i++)
	{
		if (slots[i].status == WS_FREE)
		{
			if (empty_slot_idx < 0)
			{
				args = &slots[i];
				empty_slot_idx = i;
			}
		}
		else if (slots[i].relid == relid && slots[i].dbid == MyDatabaseId)
		{
			elog(ERROR,
				 "Table '%s' is already being partitioned",
				 get_rel_name(relid));
		}
	}

	if (args == NULL)
		elog(ERROR, "No empty worker slots found");

	/* Fill arguments structure */
	args->status = WS_WORKING;
	args->dbid = MyDatabaseId;
	args->relid = relid;
	args->total_rows = 0;

	/* start worker and wait for it to finish */
	start_bg_worker("partition data worker",
					partition_data_bg_worker_main,
					empty_slot_idx,
					false);
	elog(NOTICE,
		 "Worker started. You can stop it with the following command: "
		 "select stop_worker('%s');",
		 get_rel_name(relid));

	PG_RETURN_VOID();
}

/*
 * When we receive a SIGTERM, we set InterruptPending and ProcDiePending just
 * like a normal backend.  The next CHECK_FOR_INTERRUPTS() will do the right
 * thing.
 */
static void
handle_sigterm(SIGNAL_ARGS)
{
	int			save_errno = errno;

	SetLatch(MyLatch);

	if (!proc_exit_inprogress)
	{
		InterruptPending = true;
		ProcDiePending = true;
	}

	errno = save_errno;
}

/*
 * Main worker routine. Accepts dsm_handle as an argument
 */
static void
partition_data_bg_worker_main(Datum main_arg)
{
	PartitionDataArgs   *args;
	char		   *sql = NULL;
	Oid				types[2]	= { OIDOID,	INT4OID };
	Datum			vals[2];
	bool			nulls[2]	= { false, false };
	int				rows;
	int				slot_idx = DatumGetInt32(main_arg);
	MemoryContext	worker_context = CurrentMemoryContext;
	int				failures_count = 0;
	bool			failed;

	/* Create resource owner */
	CurrentResourceOwner = ResourceOwnerCreate(NULL, "PartitionDataWorker");

	args = &slots[slot_idx];
	args->pid = MyProcPid;
	vals[0] = args->relid;
	vals[1] = 10000;

	/* Establish signal handlers before unblocking signals. */
	pqsignal(SIGTERM, handle_sigterm);

	/* We're now ready to receive signals */
	BackgroundWorkerUnblockSignals();

	/* Establish connection and start transaction */
	BackgroundWorkerInitializeConnectionByOid(args->dbid, InvalidOid);

	do
	{
		failed = false;
		rows = 0;
		StartTransactionCommand();
		bg_worker_load_config("PartitionDataWorker");

		SPI_connect();
		PushActiveSnapshot(GetTransactionSnapshot());

		if (sql == NULL)
		{
			MemoryContext oldcontext;

			/*
			 * Allocate as SQL query in top memory context because current
			 * context will be destroyed after transaction finishes
			 */
			oldcontext = MemoryContextSwitchTo(worker_context);
			sql = psprintf("SELECT %s._partition_data_concurrent($1::oid, p_limit:=$2)",
				get_namespace_name(get_pathman_schema()));
			MemoryContextSwitchTo(oldcontext);
		}

		PG_TRY();
		{
			int		ret;
			bool	isnull;

			ret = SPI_execute_with_args(sql, 2, types, vals, nulls, false, 0);
			if (ret > 0)
			{
				TupleDesc	tupdesc = SPI_tuptable->tupdesc;
				HeapTuple	tuple = SPI_tuptable->vals[0];

				Assert(SPI_processed == 1);

				rows = DatumGetInt32(SPI_getbinval(tuple, tupdesc, 1, &isnull));
			}
		}
		PG_CATCH();
		{
			EmitErrorReport();
			FlushErrorState();

			elog(WARNING, "Error #%u", failures_count);
			/*
			 * The most common exception we can catch here is a deadlock with
			 * concurrent user queries. Check that attempts count doesn't exceed
			 * some reasonable value
			 */
			if (100 <= failures_count++)
			{
				pfree(sql);
				args->status = WS_FREE;
				elog(ERROR, "Failures count exceeded 100. Finishing...");
				exit(1);
			}
			failed = true;
		}
		PG_END_TRY();

		SPI_finish();
		PopActiveSnapshot();
		if (failed)
		{
			/* abort transaction and sleep for a second */
			AbortCurrentTransaction();
			DirectFunctionCall1(pg_sleep, Float8GetDatum(1));
		}
		else
		{
			/* Reset failures counter and commit transaction */
			CommitTransactionCommand();
			failures_count = 0;
			args->total_rows += rows;
		}

		/* If other backend requested to stop worker then quit */
		if (args->status == WS_STOPPING)
			break;
	}
	while(rows > 0 || failed);  /* do while there is still rows to relocate */

	pfree(sql);
	args->status = WS_FREE;
}

/* Function context for active_workers() SRF */
typedef struct PartitionDataListCtxt
{
	int			cur_idx;
} PartitionDataListCtxt;

/*
 * Returns list of active workers for partitioning data. Each record
 * contains pid, relation name and number of processed rows
 */
Datum
active_workers(PG_FUNCTION_ARGS)
{
	FuncCallContext    *funcctx;
	MemoryContext		oldcontext;
	TupleDesc			tupdesc;
	PartitionDataListCtxt *userctx;
	int					i;
	Datum				result;
	
	if (SRF_IS_FIRSTCALL())
	{
		funcctx = SRF_FIRSTCALL_INIT();
		/* Switch context when allocating stuff to be used in later calls */
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		userctx = (PartitionDataListCtxt *) palloc(sizeof(PartitionDataListCtxt));
		userctx->cur_idx = 0;
		funcctx->user_fctx = (void *) userctx;

		/* Create tuple descriptor */
		tupdesc = CreateTemplateTupleDesc(5, false);
		TupleDescInitEntry(tupdesc, (AttrNumber) 1, "pid",
						   INT4OID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 2, "dbid",
						   INT4OID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 3, "relid",
						   INT4OID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 4, "processed",
						   INT4OID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 5, "status",
						   TEXTOID, -1, 0);
		funcctx->attinmeta = TupleDescGetAttInMetadata(tupdesc);
		funcctx->tuple_desc = BlessTupleDesc(tupdesc);

		MemoryContextSwitchTo(oldcontext);
	}

	funcctx = SRF_PERCALL_SETUP();
	userctx = (PartitionDataListCtxt *) funcctx->user_fctx;

	/*
	 * Iterate through worker slots
	 */
	for (i=userctx->cur_idx; i<WORKER_SLOTS; i++)
	{
		if (slots[i].status != WS_FREE)
		{
			char	   *values[5];
			char		txtpid[16];
			char		txtdbid[16];
			char		txtrelid[16];
			char		txtrows[16];
			HeapTuple	tuple;

			sprintf(txtpid, "%d", slots[i].pid);
			sprintf(txtrows, "%lu", slots[i].total_rows);
			sprintf(txtdbid, "%u", slots[i].dbid);
			sprintf(txtrelid, "%u", slots[i].relid);
			values[0] = txtpid;
			values[1] = txtdbid;
			values[2] = txtrelid;
			values[3] = txtrows;
			switch(slots[i].status)
			{
				case WS_WORKING:
					values[4] = "working";
					break;
				case WS_STOPPING:
					values[4] = "stopping";
					break;
				default:
					values[4] = "unknown";
			}

			tuple = BuildTupleFromCStrings(funcctx->attinmeta, values);

			result = HeapTupleGetDatum(tuple);
			userctx->cur_idx = i + 1;
			SRF_RETURN_NEXT(funcctx, result);
		}
	}

	SRF_RETURN_DONE(funcctx);
}

Datum
stop_worker(PG_FUNCTION_ARGS)
{
	Oid		relid = PG_GETARG_OID(0);
	int		i;

	for (i = 0; i < WORKER_SLOTS; i++)
		if (slots[i].status != WS_FREE &&
			slots[i].relid == relid &&
			slots[i].dbid == MyDatabaseId)
		{
			slots[i].status = WS_STOPPING;
			elog(NOTICE,
				 "Worker will stop after current batch finished");
			PG_RETURN_BOOL(true);
		}

	elog(ERROR,
		 "Worker for relation '%s' not found",
		 get_rel_name(relid));
}
