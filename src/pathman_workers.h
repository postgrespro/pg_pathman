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


/*
 * Store args, result and execution status of CreatePartitionsWorker.
 */
typedef struct
{
	Oid		userid;		/* connect as a specified user */

	Oid		result;		/* target partition */
	Oid		dbid;		/* database which stores 'partitioned_table' */
	Oid		partitioned_table;

	/* Type will help us to work with Datum */
	Oid		value_type;
	Size	value_size;
	bool	value_byval;

	/* Store Datum as flexible array */
	uint8	value[FLEXIBLE_ARRAY_MEMBER];
} SpawnPartitionArgs;


/*
 * Store args and execution status of a single ConcurrentPartWorker.
 */
typedef struct
{
	enum
	{
		WS_FREE = 0,	/* slot is empty */
		WS_WORKING,		/* occupied by live worker */
		WS_STOPPING		/* worker is going to shutdown */

	}		worker_status;	/* status of a particular worker */

	pid_t	pid;			/* worker's PID */
	Oid		dbid;			/* database which contains relation 'relid' */
	Oid		relid;			/* table to be partitioned concurrently */
	uint64	total_rows;		/* total amount of rows processed */
} ConcurrentPartSlot;

#define InitConcurrentPartSlot(slot, w_status, db, rel) \
	do { \
		(slot)->worker_status = (w_status); \
		(slot)->dbid = (db); \
		(slot)->relid = (rel); \
		(slot)->total_rows = 0; \
	} while (0)


/* Number of worker slots for concurrent partitioning */
#define PART_WORKER_SLOTS	10


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
