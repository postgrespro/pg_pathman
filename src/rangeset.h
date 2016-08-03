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


#include "pathman.h"
#include "nodes/pg_list.h"


/*
 * IndexRange contains a set of selected partitions.
 */
typedef struct {
	bool	ir_valid : 1;
	bool	ir_lossy : 1;	/* should we use IndexScan? */
	uint32	ir_lower : 31;	/* lower bound */
	uint32	ir_upper : 31;	/* upper bound */
} IndexRange;


#define RANGE_MASK			0xEFFFFFFF
#define InvalidIndexRange	{ false, false, 0, 0 }


inline static IndexRange
make_irange(uint32 lower, uint32 upper, bool lossy)
{
	IndexRange result;

	result.ir_valid = true;
	result.ir_lossy = lossy;
	result.ir_lower = (lower & RANGE_MASK);
	result.ir_upper = (upper & RANGE_MASK);

	return result;
}

inline static IndexRange *
alloc_irange(IndexRange irange)
{
	IndexRange *result = (IndexRange *) palloc(sizeof(IndexRange));

	memcpy((void *) result, (void *) &irange, sizeof(IndexRange));

	return result;
}

#define lfirst_irange(lc)				( *(IndexRange *) lfirst(lc) )
#define lappend_irange(list, irange)	( lappend((list), alloc_irange(irange)) )
#define lcons_irange(irange, list)		( lcons(alloc_irange(irange), (list)) )
#define list_make1_irange(irange)		( lcons(alloc_irange(irange), NIL) )
#define llast_irange(list)				( lfirst_irange(list_tail(list)) )
#define linitial_irange(list)			( lfirst_irange(list_head(list)) )


/* rangeset.c */
bool irange_intersects(IndexRange a, IndexRange b);
bool irange_conjuncted(IndexRange a, IndexRange b);
IndexRange irange_union(IndexRange a, IndexRange b);
IndexRange irange_intersect(IndexRange a, IndexRange b);
List *irange_list_union(List *a, List *b);
List *irange_list_intersect(List *a, List *b);
int irange_list_length(List *rangeset);
bool irange_list_find(List *rangeset, int index, bool *lossy);

#endif
