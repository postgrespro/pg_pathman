/* ------------------------------------------------------------------------
 *
 * rangeset.c
 *		IndexRange functions
 *
 * Copyright (c) 2015-2016, Postgres Professional
 *
 * ------------------------------------------------------------------------
 */

#include "rangeset.h"

/* Check if two ranges are intersecting */
bool
irange_intersects(IndexRange a, IndexRange b)
{
	return (a.ir_lower <= b.ir_upper) &&
		   (b.ir_lower <= a.ir_upper);
}

/* Check if two ranges are conjuncted */
bool
irange_conjuncted(IndexRange a, IndexRange b)
{
	return (a.ir_lower - 1 <= b.ir_upper) &&
		   (b.ir_lower - 1 <= a.ir_upper);
}

/* Make union of two ranges. They should have the same lossiness. */
IndexRange
irange_union(IndexRange a, IndexRange b)
{
	Assert(a.ir_lossy == b.ir_lossy);
	return make_irange(Min(a.ir_lower, b.ir_lower),
					   Max(a.ir_upper, b.ir_upper),
					   a.ir_lossy);
}

/* Get intersection of two ranges */
IndexRange
irange_intersect(IndexRange a, IndexRange b)
{
	return make_irange(Max(a.ir_lower, b.ir_lower),
					   Min(a.ir_upper, b.ir_upper),
					   a.ir_lossy || b.ir_lossy);
}

/*
 * Make union of two index rage lists.
 */
List *
irange_list_union(List *a, List *b)
{
	ListCell   *ca,
			   *cb;
	List	   *result = NIL;
	IndexRange	cur = InvalidIndexRange;
	bool		have_cur = false;

	ca = list_head(a);
	cb = list_head(b);

	while (ca || cb)
	{
		IndexRange next = InvalidIndexRange;

		/* Fetch next range with lesser lower bound */
		if (ca && cb)
		{
			if (lfirst_irange(ca).ir_lower <= lfirst_irange(cb).ir_lower)
			{
				next = lfirst_irange(ca);
				ca = lnext(ca);
			}
			else
			{
				next = lfirst_irange(cb);
				cb = lnext(cb);
			}
		}
		else if (ca)
		{
			next = lfirst_irange(ca);
			ca = lnext(ca);
		}
		else if (cb)
		{
			next = lfirst_irange(cb);
			cb = lnext(cb);
		}

		if (!have_cur)
		{
			/* Put this range as current value if don't have it yet */
			cur = next;
			have_cur = true;
		}
		else
		{
			if (irange_conjuncted(next, cur))
			{
				/*
				 * Ranges are conjuncted, try to unify them.
				 */
				if (next.ir_lossy == cur.ir_lossy)
				{
					cur = irange_union(next, cur);
				}
				else
				{
					if (!cur.ir_lossy)
					{
						result = lappend_irange(result, cur);
						cur = make_irange(cur.ir_upper + 1,
										  next.ir_upper,
										  next.ir_lossy);
					}
					else
					{
						result = lappend_irange(result,
												make_irange(cur.ir_lower,
															next.ir_lower - 1,
															cur.ir_lossy));
						cur = next;
					}
				}
			}
			else
			{
				/*
				 * Next range is not conjuncted with current. Put current to the
				 * result list and put next as current.
				 */
				result = lappend_irange(result, cur);
				cur = next;
			}
		}
	}

	/* Put current value into result list if any */
	if (have_cur)
		result = lappend_irange(result, cur);

	return result;
}

/*
 * Find intersection of two range lists.
 */
List *
irange_list_intersect(List *a, List *b)
{
	ListCell   *ca,
			   *cb;
	List	   *result = NIL;
	IndexRange	ra, rb;

	ca = list_head(a);
	cb = list_head(b);

	while (ca && cb)
	{
		ra = lfirst_irange(ca);
		rb = lfirst_irange(cb);

		/* Only care about intersecting ranges */
		if (irange_intersects(ra, rb))
		{
			IndexRange	intersect, last;

			/*
			 * Get intersection and try to "glue" it to previous range,
			 * put it separately otherwise.
			 */
			intersect = irange_intersect(ra, rb);
			if (result != NIL)
			{
				last = llast_irange(result);
				if (irange_conjuncted(last, intersect) &&
					last.ir_lossy == intersect.ir_lossy)
				{
					llast(result) = alloc_irange(irange_union(last, intersect));
				}
				else
				{
					result = lappend_irange(result, intersect);
				}
			}
			else
			{
				result = lappend_irange(result, intersect);
			}
		}

		/*
		 * Fetch next ranges. We use upper bound of current range to determine
		 * which lists to fetch, since lower bound of next range is greater (or
		 * equal) to upper bound of current.
		 */
		if (ra.ir_upper <= rb.ir_upper)
			ca = lnext(ca);
		if (ra.ir_upper >= rb.ir_upper)
			cb = lnext(cb);
	}
	return result;
}

/* Get total number of elements in range list */
int
irange_list_length(List *rangeset)
{
	ListCell   *lc;
	int			result = 0;

	foreach (lc, rangeset)
	{
		IndexRange irange = lfirst_irange(lc);
		result += irange.ir_upper - irange.ir_lower + 1;
	}
	return result;
}

/* Find particular index in range list */
bool
irange_list_find(List *rangeset, int index, bool *lossy)
{
	ListCell   *lc;

	foreach (lc, rangeset)
	{
		IndexRange irange = lfirst_irange(lc);
		if (index >= irange.ir_lower && index <= irange.ir_upper)
		{
			if (lossy)
				*lossy = irange.ir_lossy;
			return true;
		}
	}
	return false;
}
