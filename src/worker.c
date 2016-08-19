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
#include "utils/datum.h"
#include "utils/typcache.h"
#include "utils/lsyscache.h"
#include "utils/resowner.h"


static void bg_worker_load_config(const char *bgw_name);
static void bg_worker_main(Datum main_arg);


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
 * Initialize pg_pathman's local config in BGW process.
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
#define HandleError(condition, new_state) \
	if (condition) { exec_state = (new_state); goto handle_bg_exec_state; }

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
	dsm_segment			   *segment;
	dsm_handle				segment_handle;
	pid_t					pid;
	PartitionArgs		   *args;
	Oid						child_oid = InvalidOid;


	/* Create a dsm segment for the worker to pass arguments */
	segment = create_partitions_bg_worker_segment(relid, value, value_type);
	segment_handle = dsm_segment_handle(segment);
	args = (PartitionArgs *) dsm_segment_address(segment);

	/* Initialize worker struct */
	worker.bgw_flags		= BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
	worker.bgw_start_time	= BgWorkerStart_RecoveryFinished;
	worker.bgw_restart_time	= BGW_NEVER_RESTART;
	worker.bgw_notify_pid	= MyProcPid;
	worker.bgw_main_arg		= UInt32GetDatum(segment_handle);
	worker.bgw_main			= bg_worker_main;

	/* Set worker's name */
	memcpy((void *) &worker.bgw_name,
		   (const void *) create_partitions_bgw,
		   strlen(create_partitions_bgw));

	/* Start dynamic worker */
	bgw_started = RegisterDynamicBackgroundWorker(&worker, &bgw_handle);
	HandleError(bgw_started == false, BGW_COULD_NOT_START);

	/* Wait till the worker starts */
	bgw_status = WaitForBackgroundWorkerStartup(bgw_handle, &pid);
	HandleError(bgw_status == BGWH_POSTMASTER_DIED, BGW_PM_DIED);

	/* Wait till the worker finishes job */
	bgw_status = WaitForBackgroundWorkerShutdown(bgw_handle);
	HandleError(bgw_status == BGWH_POSTMASTER_DIED, BGW_PM_DIED);

	/* Save the result (partition Oid) */
	child_oid = args->result;


/* end execution */
handle_bg_exec_state:

	/* Free dsm segment */
	dsm_detach(segment);

	switch (exec_state)
	{
		case BGW_COULD_NOT_START:
			elog(ERROR, "Unable to create background worker for pg_pathman");
			break;

		case BGW_PM_DIED:
			ereport(ERROR,
					(errmsg("Postmaster died during the pg_pathman's background worker process"),
					 errhint("More details may be available in the server log.")));
			break;

		default:
			break;
	}

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
bg_worker_main(Datum main_arg)
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
