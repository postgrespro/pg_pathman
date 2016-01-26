#include "pathman.h"

/* Check if two ranges are intersecting */
bool
irange_intersects(IndexRange a, IndexRange b)
{
	return (irange_lower(a) <= irange_upper(b)) ||
		   (irange_lower(b) <= irange_upper(a));
}

/* Check if two ranges are conjuncted */
bool
irange_conjuncted(IndexRange a, IndexRange b)
{
	return (irange_lower(a) - 1 <= irange_upper(b)) ||
		   (irange_lower(b) - 1 <= irange_upper(a));
}

/* Make union of two ranges. They should have the same lossiness. */
IndexRange
irange_union(IndexRange a, IndexRange b)
{
	Assert(irange_is_lossy(a) == irange_is_lossy(b));
	return make_irange(Min(irange_lower(a), irange_lower(b)),
					   Max(irange_upper(a), irange_upper(b)),
					   irange_is_lossy(a));
}

/* Get intersection of two ranges */
IndexRange
irange_intersect(IndexRange a, IndexRange b)
{
	return make_irange(Max(irange_lower(a), irange_lower(b)),
					   Min(irange_upper(a), irange_upper(b)),
					   irange_is_lossy(a) || irange_is_lossy(b));
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
	IndexRange	cur;
	bool		have_cur = false;

	ca = list_head(a);
	cb = list_head(b);

	while (ca || cb)
	{
		IndexRange	next = 0;

		/* Fetch next range with lesser lower bound */
		if (ca && cb)
		{
			if (irange_lower(lfirst_irange(ca)) <= irange_lower(lfirst_irange(cb)))
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
				if (irange_is_lossy(next) == irange_is_lossy(cur))
				{
					cur = irange_union(next, cur);
				}
				else
				{
					if (!irange_is_lossy(cur))
					{
						result = lappend_irange(result, cur);
						cur = make_irange(irange_upper(cur) + 1,
												 irange_upper(next),
												 irange_is_lossy(next));
					}
					else
					{
						result = lappend_irange(result, 
									make_irange(irange_lower(cur),
												irange_lower(next) - 1,
												irange_is_lossy(cur)));
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
					irange_is_lossy(last) == irange_is_lossy(intersect))
				{
					llast_int(result) = irange_union(last, intersect);
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
		if (irange_upper(ra) <= irange_upper(rb))
			ca = lnext(ca);
		if (irange_upper(ra) >= irange_upper(rb))
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
		result += irange_upper(irange) - irange_lower(irange) + 1;
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
		if (index >= irange_lower(irange) && index <= irange_upper(irange))
		{
			if (lossy)
				*lossy = irange_is_lossy(irange) ? true : false;
			return true;
		}
	}
	return false;
}
