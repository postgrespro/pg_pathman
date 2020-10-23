/* ------------------------------------------------------------------------
 *
 * relation_info.h
 *		Data structures describing partitioned relations
 *
 * Copyright (c) 2016-2020, Postgres Professional
 *
 * ------------------------------------------------------------------------
 */

#ifndef RELATION_INFO_H
#define RELATION_INFO_H

#include "compat/pg_compat.h"

#include "utils.h"

#include "access/attnum.h"
#include "access/sysattr.h"
#include "fmgr.h"
#include "nodes/bitmapset.h"
#include "nodes/nodes.h"
#include "nodes/memnodes.h"
#include "nodes/primnodes.h"
#include "nodes/value.h"
#include "port/atomics.h"
#include "rewrite/rewriteManip.h"
#include "storage/lock.h"
#include "utils/datum.h"
#include "utils/lsyscache.h"
#include "utils/relcache.h"


#ifdef USE_ASSERT_CHECKING
#define USE_RELINFO_LOGGING
#define USE_RELINFO_LEAK_TRACKER
#endif


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

static inline char *
BoundToCString(const Bound *bound, Oid value_type)
{
	return IsInfinite(bound) ?
			pstrdup("NULL") :
			datum_to_cstring(bound->value, value_type);
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


/* Partitioning type */
typedef enum
{
	PT_ANY = 0, /* for part type traits (virtual type) */
	PT_HASH,
	PT_RANGE
} PartType;

/* Child relation info for RANGE partitioning */
typedef struct
{
	Oid				child_oid;
	Bound			min,
					max;
} RangeEntry;

/*
 * PartStatusInfo
 *		Cached partitioning status of the specified relation.
 *		Allows us to quickly search for PartRelationInfo.
 */
typedef struct PartStatusInfo
{
	Oid				relid;			/* key */
	struct PartRelationInfo *prel;
} PartStatusInfo;

/*
 * PartParentInfo
 *		Cached parent of the specified partition.
 *		Allows us to quickly search for parent PartRelationInfo.
 */
typedef struct PartParentInfo
{
	Oid				child_relid;	/* key */
	Oid				parent_relid;
} PartParentInfo;

/*
 * PartBoundInfo
 *		Cached bounds of the specified partition.
 *		Allows us to deminish overhead of check constraints.
 */
typedef struct PartBoundInfo
{
	Oid				child_relid;	/* key */

	PartType		parttype;

	/* For RANGE partitions */
	Bound			range_min;
	Bound			range_max;
	bool			byval;

	/* For HASH partitions */
	uint32			part_idx;
} PartBoundInfo;

static inline void
FreePartBoundInfo(PartBoundInfo *pbin)
{
	if (pbin->parttype == PT_RANGE)
	{
		FreeBound(&pbin->range_min, pbin->byval);
		FreeBound(&pbin->range_max, pbin->byval);
	}
}

/*
 * PartRelationInfo
 *		Per-relation partitioning information.
 *		Allows us to perform partition pruning.
 */
typedef struct PartRelationInfo
{
	Oid				relid;			/* key */
	int32			refcount;		/* reference counter */
	bool			fresh;			/* is this entry fresh? */

	bool			enable_parent;	/* should plan include parent? */

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

	Oid				cmp_proc,		/* comparison function for 'ev_type' */
					hash_proc;		/* hash function for 'ev_type' */

#ifdef USE_RELINFO_LEAK_TRACKER
	List		   *owners;			/* saved callers of get_pathman_relation_info() */
	uint64			access_total;	/* total amount of accesses to this entry */
#endif

	MemoryContext	mcxt;			/* memory context holding this struct */
} PartRelationInfo;

#define PART_EXPR_VARNO				( 1 )

/*
 * PartRelationInfo field access macros & functions.
 */

#define PrelParentRelid(prel)		( (prel)->relid )

#define PrelGetChildrenArray(prel)	( (prel)->children )

#define PrelGetRangesArray(prel)	( (prel)->ranges )

#define PrelChildrenCount(prel)		( (prel)->children_count )

#define PrelReferenceCount(prel)	( (prel)->refcount )

#define PrelIsFresh(prel)			( (prel)->fresh )

static inline uint32
PrelHasPartition(const PartRelationInfo *prel, Oid partition_relid)
{
	Oid	   *children = PrelGetChildrenArray(prel);
	uint32	i;

	for (i = 0; i < PrelChildrenCount(prel); i++)
		if (children[i] == partition_relid)
			return i + 1;

	return 0;
}

static inline uint32
PrelLastChild(const PartRelationInfo *prel)
{
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
		char	   *attname = get_attname_compat(PrelParentRelid(prel), attnum);

		columns = lappend(columns, makeString(attname));
	}

	return columns;
}

static inline Node *
PrelExpressionForRelid(const PartRelationInfo *prel, Index rti)
{
	/* TODO: implement some kind of cache */
	Node *expr = copyObject(prel->expr);

	if (rti != PART_EXPR_VARNO)
		ChangeVarNodes(expr, PART_EXPR_VARNO, rti, 0);

	return expr;
}

#if PG_VERSION_NUM >= 130000
AttrMap *PrelExpressionAttributesMap(const PartRelationInfo *prel,
										TupleDesc source_tupdesc);
#else
AttrNumber *PrelExpressionAttributesMap(const PartRelationInfo *prel,
										TupleDesc source_tupdesc,
										int *map_length);
#endif


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
	switch (parttype)
	{
		case PT_HASH:
			return "1";

		case PT_RANGE:
			return "2";

		default:
			WrongPartType(parttype);
			return NULL; /* keep compiler happy */
	}
}


/* Status chache */
void forget_status_of_relation(Oid relid);
void invalidate_status_cache(void);

/* Dispatch cache */
bool has_pathman_relation_info(Oid relid);
PartRelationInfo *get_pathman_relation_info(Oid relid);
void close_pathman_relation_info(PartRelationInfo *prel);

void qsort_range_entries(RangeEntry *entries, int nentries,
						 const PartRelationInfo *prel);

void shout_if_prel_is_invalid(const Oid parent_oid,
							  const PartRelationInfo *prel,
							  const PartType expected_part_type);

/* Bounds cache */
void forget_bounds_of_rel(Oid partition);
PartBoundInfo *get_bounds_of_partition(Oid partition, const PartRelationInfo *prel);
Expr *get_partition_constraint_expr(Oid partition, bool raise_error);
void invalidate_bounds_cache(void);

/* Parents cache */
void cache_parent_of_partition(Oid partition, Oid parent);
void forget_parent_of_partition(Oid partition);
Oid get_parent_of_partition(Oid partition);
void invalidate_parents_cache(void);

/* Partitioning expression routines */
Node *parse_partitioning_expression(const Oid relid,
									const char *expr_cstr,
									char **query_string_out,
									Node **parsetree_out);

Node *cook_partitioning_expression(const Oid relid,
								   const char *expr_cstr,
								   Oid *expr_type);

char *canonicalize_partitioning_expression(const Oid relid,
										   const char *expr_cstr);

/* Global invalidation routines */
void delay_pathman_shutdown(void);
void finish_delayed_invalidation(void);

void init_relation_info_static_data(void);


/* For pg_pathman.enable_bounds_cache GUC */
extern bool			pg_pathman_enable_bounds_cache;

extern HTAB	   *prel_resowner;

/* This allows us to track leakers of PartRelationInfo */
#ifdef USE_RELINFO_LEAK_TRACKER
extern const char  *prel_resowner_function;
extern int			prel_resowner_line;

#define get_pathman_relation_info(relid) \
	( \
		prel_resowner_function = __FUNCTION__, \
		prel_resowner_line = __LINE__, \
		get_pathman_relation_info(relid) \
	)

#define close_pathman_relation_info(prel) \
	do { \
		close_pathman_relation_info(prel); \
		prel = NULL; \
	} while (0)
#endif /* USE_RELINFO_LEAK_TRACKER */


#endif /* RELATION_INFO_H */
