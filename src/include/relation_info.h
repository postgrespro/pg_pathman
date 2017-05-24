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
#include "access/sysattr.h"
#include "fmgr.h"
#include "nodes/bitmapset.h"
#include "nodes/nodes.h"
#include "nodes/primnodes.h"
#include "nodes/value.h"
#include "port/atomics.h"
#include "rewrite/rewriteManip.h"
#include "storage/lock.h"
#include "utils/datum.h"
#include "utils/lsyscache.h"


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
cmp_bounds(FmgrInfo *cmp_func,
		   const Oid collid,
		   const Bound *b1,
		   const Bound *b2)
{
	if (IsMinusInfinity(b1) || IsPlusInfinity(b2))
		return -1;

	if (IsMinusInfinity(b2) || IsPlusInfinity(b1))
		return 1;

	Assert(cmp_func);

	return DatumGetInt32(FunctionCall2Coll(cmp_func,
										   collid,
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
 * Child relation info for RANGE partitioning.
 */
typedef struct
{
	Oid				child_oid;
	Bound			min,
					max;
} RangeEntry;

/*
 * PartRelationInfo
 *		Per-relation partitioning information.
 *		Allows us to perform partition pruning.
 */
typedef struct
{
	Oid				key;			/* partitioned table's Oid */
	bool			valid,			/* is this entry valid? */
					enable_parent;	/* should plan include parent? */

	PartType		parttype;		/* partitioning type (HASH | RANGE) */

	/* Partition dispatch info */
	uint32			children_count;
	Oid			   *children;		/* Oids of child partitions */
	RangeEntry	   *ranges;			/* per-partition range entry or NULL */

	/* Partitioning expression */
	const char	   *expr_cstr;		/* original expression */
	Node		   *expr;			/* planned expression */
	List		   *expr_vars;		/* vars from expression, lazy */
	Bitmapset	   *expr_atts;		/* attnums from expression */

	/* Partitioning expression's value */
	Oid				ev_type;		/* expression type */
	int32			ev_typmod;		/* expression type modifier */
	bool			ev_byval;		/* is expression's val stored by value? */
	int16			ev_len;			/* length of the expression val's type */
	int				ev_align;		/* alignment of the expression val's type */
	Oid				ev_collid;		/* collation of the expression val */

	Oid				cmp_proc,		/* comparison fuction for 'ev_type' */
					hash_proc;		/* hash function for 'ev_type' */
} PartRelationInfo;

#define PART_EXPR_VARNO				( 1 )

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
 *		Allows us to deminish overhead of check constraints.
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
	uint32			part_idx;
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
 * PartRelationInfo field access macros & functions.
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

static inline List *
PrelExpressionColumnNames(const PartRelationInfo *prel)
{
	List   *columns = NIL;
	int		i = -1;

	while ((i = bms_next_member(prel->expr_atts, i)) >= 0)
	{
		AttrNumber	attnum = i + FirstLowInvalidHeapAttributeNumber;
		char	   *attname = get_attname(PrelParentRelid(prel), attnum);

		columns = lappend(columns, makeString(attname));
	}

	return columns;
}

static inline Node *
PrelExpressionForRelid(const PartRelationInfo *prel, Index rel_index)
{
	Node *expr;

	/* TODO: implement some kind of cache */
	if (rel_index != PART_EXPR_VARNO)
	{
		expr = copyObject(prel->expr);
		ChangeVarNodes(expr, PART_EXPR_VARNO, rel_index, 0);
	}
	else expr = prel->expr;

	return expr;
}


const PartRelationInfo *refresh_pathman_relation_info(Oid relid,
													  Datum *values,
													  bool allow_incomplete);
PartRelationInfo *invalidate_pathman_relation_info(Oid relid, bool *found);
void remove_pathman_relation_info(Oid relid);
const PartRelationInfo *get_pathman_relation_info(Oid relid);
const PartRelationInfo *get_pathman_relation_info_after_lock(Oid relid,
															 bool unlock_if_not_found,
															 LockAcquireResult *lock_result);

/* Partitioning expression routines */
Node *parse_partitioning_expression(const Oid relid,
									const char *expr_cstr,
									char **query_string_out,
									Node **parsetree_out);

Datum cook_partitioning_expression(const Oid relid,
								   const char *expr_cstr,
								   Oid *expr_type);

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
PartBoundInfo *get_bounds_of_partition(Oid partition,
									   const PartRelationInfo *prel);

/* PartType wrappers */

static inline void
WrongPartType(PartType parttype)
{
	elog(ERROR, "Unknown partitioning type %u", parttype);
}

static inline PartType
DatumGetPartType(Datum datum)
{
	uint32 parttype = DatumGetUInt32(datum);

	if (parttype < 1 || parttype > 2)
		WrongPartType(parttype);

	return (PartType) parttype;
}

static inline char *
PartTypeToCString(PartType parttype)
{
	static char *hash_str	= "1",
				*range_str	= "2";

	switch (parttype)
	{
		case PT_HASH:
			return hash_str;

		case PT_RANGE:
			return range_str;

		default:
			WrongPartType(parttype);
			return NULL; /* keep compiler happy */
	}
}


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
		if (!prel->ev_byval)
		{
			for (i = 0; i < PrelChildrenCount(prel); i++)
			{
				Oid child = prel->ranges[i].child_oid;

				/* Skip if Oid is invalid (e.g. initialization error) */
				if (!OidIsValid(child))
					continue;

				FreeBound(&prel->ranges[i].min, prel->ev_byval);
				FreeBound(&prel->ranges[i].max, prel->ev_byval);
			}
		}

		pfree(prel->ranges);
		prel->ranges = NULL;
	}
}


/* For pg_pathman.enable_bounds_cache GUC */
extern bool pg_pathman_enable_bounds_cache;

void init_relation_info_static_data(void);


#endif /* RELATION_INFO_H */

