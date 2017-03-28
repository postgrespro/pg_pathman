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
#include "fmgr.h"
#include "port/atomics.h"
#include "storage/lock.h"
#include "utils/datum.h"
#include "nodes/primnodes.h"


/* Range bound */
typedef struct
{
	Datum	value;				/* actual value if not infinite */
	int8	is_infinite;		/* -inf | +inf | finite */
} Bound;


#define FINITE					(  0 )
#define PLUS_INFINITY			( +1 )
#define MINUS_INFINITY			( -1 )

#define IsInfinite(i)			( (i)->is_infinite != FINITE )
#define IsPlusInfinity(i)		( (i)->is_infinite == PLUS_INFINITY )
#define IsMinusInfinity(i)		( (i)->is_infinite == MINUS_INFINITY )

static inline Bound
CopyBound(const Bound *src, bool byval, int typlen)
{
	Bound bound = {
		IsInfinite(src) ?
			src->value :
			datumCopy(src->value, byval, typlen),
		src->is_infinite
	};

	return bound;
}

static inline Bound
MakeBound(Datum value)
{
	Bound bound = { value, FINITE };

	return bound;
}

static inline Bound
MakeBoundInf(int8 infinity_type)
{
	Bound bound = { (Datum) 0, infinity_type };

	return bound;
}

static inline Datum
BoundGetValue(const Bound *bound)
{
	Assert(!IsInfinite(bound));

	return bound->value;
}

static inline void
FreeBound(Bound *bound, bool byval)
{
	if (!IsInfinite(bound) && !byval)
		pfree(DatumGetPointer(BoundGetValue(bound)));
}

static inline int
cmp_bounds(FmgrInfo *cmp_func, const Bound *b1, const Bound *b2)
{
	if (IsMinusInfinity(b1) || IsPlusInfinity(b2))
		return -1;

	if (IsMinusInfinity(b2) || IsPlusInfinity(b1))
		return 1;

	Assert(cmp_func);

	return DatumGetInt32(FunctionCall2(cmp_func,
									   BoundGetValue(b1),
									   BoundGetValue(b2)));
}


/*
 * Partitioning type.
 */
typedef enum
{
	PT_ANY = 0, /* for part type traits (virtual type) */
	PT_HASH,
	PT_RANGE
} PartType;

/*
 * Child relation info for RANGE partitioning
 */
typedef struct
{
	Oid				child_oid;
	Bound			min,
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

	PartType		parttype;		/* partitioning type (HASH | RANGE) */

	uint32			children_count;
	Oid			   *children;		/* Oids of child partitions */
	RangeEntry	   *ranges;			/* per-partition range entry or NULL */

	const char	   *attname;		/* name of the partitioned column */

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
 * PartParentInfo
 *		Cached parent of the specified partition.
 *		Allows us to quickly search for PartRelationInfo.
 */
typedef struct
{
	Oid				child_rel;		/* key */
	Oid				parent_rel;
} PartParentInfo;

/*
 * PartBoundInfo
 *		Cached bounds of the specified partition.
 */
typedef struct
{
	Oid				child_rel;		/* key */

	PartType		parttype;

	/* For RANGE partitions */
	Bound			range_min;
	Bound			range_max;
	bool			byval;

	/* For HASH partitions */
	uint32			hash;
} PartBoundInfo;

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

static inline uint32
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
													  const char *part_column_name,
													  bool allow_incomplete);
void invalidate_pathman_relation_info(Oid relid, bool *found);
void remove_pathman_relation_info(Oid relid);
const PartRelationInfo *get_pathman_relation_info(Oid relid);
const PartRelationInfo *get_pathman_relation_info_after_lock(Oid relid,
															 bool unlock_if_not_found,
															 LockAcquireResult *lock_result);

/* Global invalidation routines */
void delay_pathman_shutdown(void);
void delay_invalidation_parent_rel(Oid parent);
void delay_invalidation_vague_rel(Oid vague_rel);
void finish_delayed_invalidation(void);

/* Parent cache */
void cache_parent_of_partition(Oid partition, Oid parent);
Oid forget_parent_of_partition(Oid partition, PartParentSearch *status);
Oid get_parent_of_partition(Oid partition, PartParentSearch *status);

/* Bounds cache */
void forget_bounds_of_partition(Oid partition);
PartBoundInfo * get_bounds_of_partition(Oid partition,
										const PartRelationInfo *prel);

/* Safe casts for PartType */
PartType DatumGetPartType(Datum datum);
char * PartTypeToCString(PartType parttype);

/* PartRelationInfo checker */
void shout_if_prel_is_invalid(const Oid parent_oid,
							  const PartRelationInfo *prel,
							  const PartType expected_part_type);


/*
 * Useful functions & macros for freeing memory.
 */

#define FreeIfNotNull(ptr) \
	do { \
		if (ptr) \
		{ \
			pfree((void *) ptr); \
			ptr = NULL; \
		} \
	} while(0)

static inline void
FreeChildrenArray(PartRelationInfo *prel)
{
	uint32	i;

	/* Remove relevant PartParentInfos */
	if (prel->children)
	{
		for (i = 0; i < PrelChildrenCount(prel); i++)
		{
			Oid child = prel->children[i];

			/* Skip if Oid is invalid (e.g. initialization error) */
			if (!OidIsValid(child))
				continue;

			/* If it's *always been* relid's partition, free cache */
			if (PrelParentRelid(prel) == get_parent_of_partition(child, NULL))
				forget_parent_of_partition(child, NULL);
		}

		pfree(prel->children);
		prel->children = NULL;
	}
}

static inline void
FreeRangesArray(PartRelationInfo *prel)
{
	uint32	i;

	/* Remove RangeEntries array */
	if (prel->ranges)
	{
		/* Remove persistent entries if not byVal */
		if (!prel->attbyval)
		{
			for (i = 0; i < PrelChildrenCount(prel); i++)
			{
				Oid child = prel->ranges[i].child_oid;

				/* Skip if Oid is invalid (e.g. initialization error) */
				if (!OidIsValid(child))
					continue;

				FreeBound(&prel->ranges[i].min, prel->attbyval);
				FreeBound(&prel->ranges[i].max, prel->attbyval);
			}
		}

		pfree(prel->ranges);
		prel->ranges = NULL;
	}
}

#endif /* RELATION_INFO_H */
