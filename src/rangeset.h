/* ------------------------------------------------------------------------
 *
 * rangeset.h
 *		IndexRange functions
 *
 * Copyright (c) 2015-2016, Postgres Professional
 *
 * ------------------------------------------------------------------------
 */

#ifndef PATHMAN_RANGESET_H
#define PATHMAN_RANGESET_H


#include "postgres.h"
#include "nodes/pg_list.h"


/*
 * IndexRange contains a set of selected partitions.
 */
typedef struct {
	/* lossy == should we use IndexScan? */
	/* valid == is this IndexRange valid? */

	/* Don't swap this fields */
	uint32	lower;	/* valid + lower_bound */
	uint32	upper;	/* lossy + upper_bound */
} IndexRange;


#define IRANGE_SPECIAL_BIT		( (uint32) ( ((uint32) 1) << 31) )
#define IRANGE_BONDARY_MASK		( (uint32) (~IRANGE_SPECIAL_BIT) )

#define InvalidIndexRange		{ 0, 0 }

#define is_irange_valid(irange) ( (irange.lower & IRANGE_SPECIAL_BIT) > 0 )
#define is_irange_lossy(irange)	( (irange.upper & IRANGE_SPECIAL_BIT) > 0 )
#define irange_lower(irange)	( (uint32) (irange.lower & IRANGE_BONDARY_MASK) )
#define irange_upper(irange)	( (uint32) (irange.upper & IRANGE_BONDARY_MASK) )


inline static IndexRange
make_irange(uint32 lower, uint32 upper, bool lossy)
{
	IndexRange result = { lower & IRANGE_BONDARY_MASK,
						  upper & IRANGE_BONDARY_MASK };

	/* Set VALID */
	result.lower |= IRANGE_SPECIAL_BIT;

	/* Set LOSSY if needed */
	if (lossy) result.upper |= IRANGE_SPECIAL_BIT;

	Assert(lower <= upper);

	return result;
}

inline static IndexRange *
alloc_irange(IndexRange irange)
{
	IndexRange *result = (IndexRange *) palloc(sizeof(IndexRange));

	/* Copy all fields of IndexRange */
	*result = irange;

	return result;
}

/* Return predecessor or 0 if boundary is 0 */
inline static uint32
irb_pred(uint32 boundary)
{
	if (boundary > 0)
		return (boundary - 1) & IRANGE_BONDARY_MASK;

	return 0;
}

/* Return predecessor or IRANGE_BONDARY_MASK */
inline static uint32
irb_succ(uint32 boundary)
{
	if (boundary >= IRANGE_BONDARY_MASK)
		return boundary;

	return boundary + 1;
}


#define lfirst_irange(lc)				( *(IndexRange *) lfirst(lc) )
#define lappend_irange(list, irange)	( lappend((list), alloc_irange(irange)) )
#define lcons_irange(irange, list)		( lcons(alloc_irange(irange), (list)) )
#define list_make1_irange(irange)		( lcons(alloc_irange(irange), NIL) )
#define llast_irange(list)				( lfirst_irange(list_tail(list)) )
#define linitial_irange(list)			( lfirst_irange(list_head(list)) )


/* Result of function irange_cmp_lossiness() */
typedef enum
{
	IR_EQ_LOSSINESS = 0,	/* IndexRanges share same lossiness */
	IR_A_LOSSY,				/* IndexRange 'a' is lossy ('b' is not) */
	IR_B_LOSSY				/* IndexRange 'b' is lossy ('a' is not) */
} ir_cmp_lossiness;


/* Various traits */
bool iranges_intersect(IndexRange a, IndexRange b);
bool iranges_adjoin(IndexRange a, IndexRange b);
bool irange_eq_bounds(IndexRange a, IndexRange b);
ir_cmp_lossiness irange_cmp_lossiness(IndexRange a, IndexRange b);

/* Basic operations on IndexRanges */
IndexRange irange_union_simple(IndexRange a, IndexRange b);
IndexRange irange_intersection_simple(IndexRange a, IndexRange b);

/* Operations on Lists of IndexRanges */
List *irange_list_union(List *a, List *b);
List *irange_list_intersection(List *a, List *b);

/* Utility functions */
int irange_list_length(List *rangeset);
bool irange_list_find(List *rangeset, int index, bool *lossy);

#endif
