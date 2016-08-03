/* ------------------------------------------------------------------------
 *
 * relation_info.h
 *		Data structures describing partitioned relations
 *
 * Copyright (c) 2016, Postgres Professional
 *
 * ------------------------------------------------------------------------
 */

#ifndef RELATION_INFO_H
#define RELATION_INFO_H

#include "dsm_array.h"

#include "postgres.h"
#include "port/atomics.h"


/*
 * Partitioning type
 */
typedef enum
{
	PT_HASH = 1,
	PT_RANGE
} PartType;

/*
 * Child relation info for RANGE partitioning
 */
typedef struct
{
	Oid				child_oid;

	Datum			min,
					max;
} RangeEntry;

/*
 * PartRelationInfo
 *		Per-relation partitioning information
 */
typedef struct
{
	Oid				key;			/* partitioned table's Oid */
	bool			valid;			/* is this entry valid? */

	uint32			children_count;
	Oid			   *children;		/* Oids of child partitions */
	RangeEntry	   *ranges;			/* per-partition range entry or NULL */

	PartType		parttype;		/* partitioning type (HASH | RANGE) */
	Index			attnum;			/* partitioned column's index */
	Oid				atttype;		/* partitioned column's type */
	int32			atttypmod;		/* partitioned column's type modifier */
	bool			attbyval;		/* is partitioned column stored by value? */

	Oid				cmp_proc,		/* comparison fuction for 'atttype' */
					hash_proc;		/* hash function for 'atttype' */
} PartRelationInfo;

/*
 * ShmemRelationInfo
 *		Per-relation misc information stored in shmem
 */
typedef struct
{
	Oid				key;			/* partitioned table's Oid */

	pg_atomic_flag	dirty;			/* is anyone performing any of the
									   partitioning-related operations
									   on this table at the moment? */
} ShmemRelationInfo;

/*
 * RelParentInfo
 *		Cached parent of the specified partition.
 *		Allows us to quickly search for PartRelationInfo.
 */
typedef struct
{
	Oid				child_rel;		/* key */
	Oid				parent_rel;
} PartParentInfo;

/*
 * PartParentSearch
 *		Represents status of a specific cached entry.
 *		Returned by [for]get_parent_of_partition().
 */
typedef enum
{
	PPS_ENTRY_NOT_FOUND = 0,
	PPS_ENTRY_FOUND,		/* entry was found in pathman's or system cache */
	PPS_NOT_SURE			/* can't determine (not transactional state) */
} PartParentSearch;


#define PrelGetChildrenArray(prel, copy) ( (prel)->children )

#define PrelGetRangesArray(prel, copy) ( (prel)->ranges )

#define PrelChildrenCount(prel) ( (prel)->children_count )

#define PrelIsValid(prel) ( (prel)->valid )


PartRelationInfo *refresh_pathman_relation_info(Oid relid,
												PartType partitioning_type,
												const char *part_column_name);
void invalidate_pathman_relation_info(Oid relid, bool *found);
void remove_pathman_relation_info(Oid relid);
PartRelationInfo *get_pathman_relation_info(Oid relid, bool *found);

void cache_parent_of_partition(Oid partition, Oid parent);
Oid forget_parent_of_partition(Oid partition, PartParentSearch *status);
Oid get_parent_of_partition(Oid partition, PartParentSearch *status);

PartType DatumGetPartType(Datum datum);

#endif
