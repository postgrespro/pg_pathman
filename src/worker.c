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
#include "utils/snapmgr.h"
#include "utils/typcache.h"
#include "utils/lsyscache.h"


static void bg_worker_load_config(const char *bgw_name);
static void bg_worker_main(Datum main_arg);
static Oid append_partitions(Oid relid, Datum value,
							 Oid value_type, volatile bool *crashed);


/*
 * Store args, result and execution status of CreatePartitionsWorker.
 */
typedef struct
{
	bool	crashed;	/* has bgw crashed? */
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


#ifdef USE_ASSERT_CHECKING

	#include "access/htup_details.h"
	#include "utils/syscache.h"

	#define PrintUnpackedDatum(datum, typid) \
		do { \
			HeapTuple tup = SearchSysCache1(TYPEOID, \
											ObjectIdGetDatum(typid)); \
			if (HeapTupleIsValid(tup)) \
			{ \
				Form_pg_type	typtup = (Form_pg_type) GETSTRUCT(tup); \
				FmgrInfo		finfo; \
				fmgr_info(typtup->typoutput, &finfo); \
				elog(LOG, "BGW: arg->value is '%s'", \
					 DatumGetCString(FunctionCall1(&finfo, datum))); \
			} \
		} while (0)
#elif
	#define PrintUnpackedDatum(datum, typid) (true)
#endif

#define PackDatumToByteArray(array, datum, datum_size, typbyval) \
	do { \
		memcpy((void *) (array), \
			   (const void *) ((typbyval) ? \
								   (Pointer) (&datum) : \
								   DatumGetPointer(datum)), \
			   datum_size); \
	} while (0)

/*
 * 'typid' is not necessary, but it is used by PrintUnpackedDatum().
 */
#define UnpackDatumFromByteArray(array, datum, datum_size, typbyval, typid) \
	do { \
		if (typbyval) \
			memcpy((void *) &datum, (const void *) array, datum_size); \
		else \
		{ \
			datum = PointerGetDatum(palloc(datum_size)); \
			memcpy((void *) DatumGetPointer(datum), \
				   (const void *) array, \
				   datum_size); \
		} \
		PrintUnpackedDatum(datum, typid); \
	} while (0)


/*
 * Initialize pg_pathman's local config in BGW process.
 */
static void
bg_worker_load_config(const char *bgw_name)
{
	load_config();
	elog(LOG, "%s loaded pg_pathman's config [%u]",
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
	args->crashed = true; /* default value */
	args->result = InvalidOid;
	args->dbid = MyDatabaseId;
	args->partitioned_table = relid;

	/* Write value-related stuff */
	args->value_type = value_type;
	args->value_size = datum_size;
	args->value_byval = typcache->typbyval;

	PackDatumToByteArray(&args->value, value,
						 datum_size, args->value_byval);

	return segment;
}

/*
 * Starts background worker that will create new partitions,
 * waits till it finishes the job and returns the result (new partition oid)
 */
Oid
create_partitions_bg_worker(Oid relid, Datum value, Oid value_type)
{
#define HandleError(condition, new_state) \
	if (condition) { exec_state = (new_state); goto handle_exec_state; }

	/* Execution state to be checked */
	enum
	{
		BGW_OK = 0,				/* everything is fine (default) */
		BGW_COULD_NOT_START,	/* could not start worker */
		BGW_PM_DIED,			/* postmaster died */
		BGW_CRASHED				/* worker crashed */
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
handle_exec_state:

	/* Free dsm segment */
	dsm_detach(segment);

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

		case BGW_CRASHED:
			elog(ERROR, "Could not create partition due to background worker crash");
			break;

		default:
			break;
	}

	if (child_oid == InvalidOid)
		elog(ERROR, "Attempt to append new partitions to relation %u failed", relid);

	return child_oid;
}

/*
 * Main worker routine. Accepts dsm_handle as an argument
 */
static void
bg_worker_main(Datum main_arg)
{
	const char	   *bgw_name = "CreatePartitionsWorker";
	dsm_handle		handle = DatumGetUInt32(main_arg);
	dsm_segment	   *segment;
	PartitionArgs   *args;
	Datum			value;

	/* Create resource owner */
	CurrentResourceOwner = ResourceOwnerCreate(NULL, bgw_name);

	if (!handle)
		elog(ERROR, "%s: invalid dsm_handle", bgw_name);

	/* Attach to dynamic shared memory */
	if ((segment = dsm_attach(handle)) == NULL)
		elog(ERROR, "%s: cannot attach to segment", bgw_name);
	args = dsm_segment_address(segment);

	/* Establish connection and start transaction */
	BackgroundWorkerInitializeConnectionByOid(args->dbid, InvalidOid);
	StartTransactionCommand();
	SPI_connect();
	PushActiveSnapshot(GetTransactionSnapshot());

	/* Initialize pg_pathman's local config */
	bg_worker_load_config(bgw_name);

	UnpackDatumFromByteArray(&args->value, value,
							 args->value_size,
							 args->value_byval,
							 args->value_type);

	/* Create partitions */
	args->result = append_partitions(args->partitioned_table,
									 value,
									 args->value_type,
									 &args->crashed);

	/* Cleanup */
	SPI_finish();
	PopActiveSnapshot();
	CommitTransactionCommand();

	dsm_detach(segment);
}

/*
 * Append partitions and return Oid of the partition that contains value
 */
static Oid
append_partitions(Oid relid, Datum value, Oid value_type, volatile bool *crashed)
{
	Oid					oids[]	= { OIDOID,					 value_type };
	Datum				vals[]	= { ObjectIdGetDatum(relid), value };
	bool				nulls[]	= { false,					 false };
	char			   *sql;
	PartRelationInfo   *prel;
	FmgrInfo			cmp_func;
	MemoryContext		old_mcxt = CurrentMemoryContext;

	*crashed = true; /* write default value */

	if ((prel = get_pathman_relation_info(relid, NULL)) == NULL)
		elog(ERROR, "BGW: cannot fetch PartRelationInfo for relation %u", relid);

	/* Comparison function */
	fill_type_cmp_fmgr_info(&cmp_func, value_type, prel->atttype);

	/* Perform PL procedure */
	sql = psprintf("SELECT %s.append_partitions_on_demand_internal($1, $2)",
				   get_namespace_name(get_pathman_schema()));
	PG_TRY();
	{
		int		ret;
		Oid		partid = InvalidOid;
		bool	isnull;

		ret = SPI_execute_with_args(sql, 2, oids, vals, nulls, false, 0);
		if (ret == SPI_OK_SELECT)
		{
			TupleDesc	tupdesc = SPI_tuptable->tupdesc;
			HeapTuple	tuple = SPI_tuptable->vals[0];

			Assert(SPI_processed == 1);

			partid = DatumGetObjectId(SPI_getbinval(tuple, tupdesc, 1, &isnull));

			/* Update relation info */
			/* TODO: mark ShmemRelationInfo as 'dirty' to invalidate cache */
		}

		*crashed = false;
		return partid;
	}
	PG_CATCH();
	{
		ErrorData  *edata;

		MemoryContextSwitchTo(old_mcxt);
		edata = CopyErrorData();
		FlushErrorState();

		elog(LOG, "BGW: %s", edata->message);
		FreeErrorData(edata);

		return InvalidOid; /* something bad happened */
	}
	PG_END_TRY();
}
