/*-------------------------------------------------------------------------
 *
 * pathman_workers.h
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

#ifndef PATHMAN_WORKERS_H
#define PATHMAN_WORKERS_H

#include "postgres.h"
#include "storage/spin.h"


/*
 * Store args, result and execution status of CreatePartitionsWorker.
 */
typedef struct
{
	Oid		userid;			/* connect as a specified user */

	Oid		result;			/* target partition */
	Oid		dbid;			/* database which stores 'partitioned_table' */
	Oid		partitioned_table;

	/* Needed to decode Datum from 'values' */
	Oid		value_type;
	Size	value_size;
	bool	value_byval;

	/* Store Datum as flexible array */
	uint8	value[FLEXIBLE_ARRAY_MEMBER];
} SpawnPartitionArgs;


typedef enum
{
	CPS_FREE = 0,	/* slot is empty */
	CPS_WORKING,	/* occupied by live worker */
	CPS_STOPPING	/* worker is going to shutdown */

} ConcurrentPartSlotStatus;

/*
 * Store args and execution status of a single ConcurrentPartWorker.
 */
typedef struct
{
	slock_t	mutex;			/* protect slot from race conditions */

	ConcurrentPartSlotStatus worker_status;	/* status of a particular worker */

	Oid		userid;			/* connect as a specified user */
	pid_t	pid;			/* worker's PID */
	Oid		dbid;			/* database which contains the relation */
	Oid		relid;			/* table to be partitioned concurrently */
	uint64	total_rows;		/* total amount of rows processed */

	int32	batch_size;		/* number of rows in a batch */
	float8	sleep_time;		/* how long should we sleep in case of error? */
} ConcurrentPartSlot;

#define InitConcurrentPartSlot(slot, user, w_status, db, rel, batch_sz, sleep_t) \
	do { \
		(slot)->userid = (user); \
		(slot)->worker_status = (w_status); \
		(slot)->pid = 0; \
		(slot)->dbid = (db); \
		(slot)->relid = (rel); \
		(slot)->total_rows = 0; \
		(slot)->batch_size = (batch_sz); \
		(slot)->sleep_time = (sleep_t); \
	} while (0)

static inline ConcurrentPartSlotStatus
cps_check_status(ConcurrentPartSlot *slot)
{
	ConcurrentPartSlotStatus status;

	SpinLockAcquire(&slot->mutex);
	status = slot->worker_status;
	SpinLockRelease(&slot->mutex);

	return status;
}

static inline void
cps_set_status(ConcurrentPartSlot *slot, ConcurrentPartSlotStatus status)
{
	SpinLockAcquire(&slot->mutex);
	slot->worker_status = status;
	SpinLockRelease(&slot->mutex);
}



/* Number of worker slots for concurrent partitioning */
#define PART_WORKER_SLOTS			10

/* Max number of attempts per batch */
#define PART_WORKER_MAX_ATTEMPTS	60


/*
 * Definitions for the "pathman_concurrent_part_tasks" view.
 */
#define PATHMAN_CONCURRENT_PART_TASKS		"pathman_concurrent_part_tasks"
#define Natts_pathman_cp_tasks				6
#define Anum_pathman_cp_tasks_userid		1
#define Anum_pathman_cp_tasks_pid			2
#define Anum_pathman_cp_tasks_dbid			3
#define Anum_pathman_cp_tasks_relid			4
#define Anum_pathman_cp_tasks_processed		5
#define Anum_pathman_cp_tasks_status		6


/*
 * Concurrent partitioning slots are stored in shmem.
 */
Size estimate_concurrent_part_task_slots_size(void);
void init_concurrent_part_task_slots(void);


/*
 * Useful datum packing\unpacking functions for BGW.
 */

static inline void *
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

	return ((uint8 *) byte_array) + datum_size;
}

static inline void *
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

	return ((uint8 *) byte_array) + datum_size;
}

#endif
