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

#include "postgres.h"
#include "access/attnum.h"
#include "port/atomics.h"


/*
 * Partitioning type.
 */
typedef enum
{
	PT_INDIFFERENT = 0, /* for part type traits (virtual type) */
	PT_HASH,
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
	bool			enable_parent;	/* include parent to the plan */
	bool			auto_partition; /* auto partition creation */
	Oid				init_callback;	/* callback for partition creation */

	uint32			children_count;
	Oid			   *children;		/* Oids of child partitions */
	RangeEntry	   *ranges;			/* per-partition range entry or NULL */

	PartType		parttype;		/* partitioning type (HASH | RANGE) */
	AttrNumber		attnum;			/* partitioned column's index */
	Oid				atttype;		/* partitioned column's type */
	int32			atttypmod;		/* partitioned column type modifier */
	bool			attbyval;		/* is partitioned column stored by value? */
	int16			attlen;			/* length of the partitioned column's type */
	int				attalign;		/* alignment of the part column's type */
	Oid				attcollid;		/* collation of the partitioned column */

	Oid				cmp_proc,		/* comparison fuction for 'atttype' */
					hash_proc;		/* hash function for 'atttype' */
} PartRelationInfo;

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
	PPS_ENTRY_PARENT,		/* entry was found, but pg_pathman doesn't know it */
	PPS_ENTRY_PART_PARENT,	/* entry is parent and is known by pg_pathman */
	PPS_NOT_SURE			/* can't determine (not transactional state) */
} PartParentSearch;


/*
 * PartRelationInfo field access macros.
 */

#define PrelParentRelid(prel)		( (prel)->key )

#define PrelGetChildrenArray(prel)	( (prel)->children )

#define PrelGetRangesArray(prel)	( (prel)->ranges )

#define PrelChildrenCount(prel)		( (prel)->children_count )

#define PrelIsValid(prel)			( (prel) && (prel)->valid )

inline static uint32
PrelLastChild(const PartRelationInfo *prel)
{
	Assert(PrelIsValid(prel));

	if (PrelChildrenCount(prel) == 0)
		elog(ERROR, "pg_pathman's cache entry for relation %u has 0 children",
			 PrelParentRelid(prel));

	return PrelChildrenCount(prel) - 1; /* last partition */
}


const PartRelationInfo *refresh_pathman_relation_info(Oid relid,
													  PartType partitioning_type,
													  const char *part_column_name);
void invalidate_pathman_relation_info(Oid relid, bool *found);
void remove_pathman_relation_info(Oid relid);
const PartRelationInfo *get_pathman_relation_info(Oid relid);
const PartRelationInfo *get_pathman_relation_info_after_lock(Oid relid,
															 bool unlock_if_not_found);

void delay_pathman_shutdown(void);
void delay_invalidation_parent_rel(Oid parent);
void delay_invalidation_vague_rel(Oid vague_rel);
void finish_delayed_invalidation(void);

void cache_parent_of_partition(Oid partition, Oid parent);
Oid forget_parent_of_partition(Oid partition, PartParentSearch *status);
Oid get_parent_of_partition(Oid partition, PartParentSearch *status);

PartType DatumGetPartType(Datum datum);
Datum PartTypeGetTextDatum(PartType parttype);

void shout_if_prel_is_invalid(Oid parent_oid,
							  const PartRelationInfo *prel,
							  PartType expected_part_type);


/*
 * Useful static functions for freeing memory.
 */

static inline void
FreeChildrenArray(PartRelationInfo *prel)
{
	uint32	i;

	Assert(PrelIsValid(prel));

	/* Remove relevant PartParentInfos */
	if ((prel)->children)
	{
		for (i = 0; i < PrelChildrenCount(prel); i++)
		{
			Oid child = (prel)->children[i];

			/* If it's *always been* relid's partition, free cache */
			if (PrelParentRelid(prel) == get_parent_of_partition(child, NULL))
				forget_parent_of_partition(child, NULL);
		}

		pfree((prel)->children);
		(prel)->children = NULL;
	}
}

static inline void
FreeRangesArray(PartRelationInfo *prel)
{
	uint32	i;

	Assert(PrelIsValid(prel));

	/* Remove RangeEntries array */
	if ((prel)->ranges)
	{
		/* Remove persistent entries if not byVal */
		if (!(prel)->attbyval)
		{
			for (i = 0; i < PrelChildrenCount(prel); i++)
			{
				pfree(DatumGetPointer((prel)->ranges[i].min));
				pfree(DatumGetPointer((prel)->ranges[i].max));
			}
		}

		pfree((prel)->ranges);
		(prel)->ranges = NULL;
	}
}

#endif
