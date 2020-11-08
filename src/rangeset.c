/* ------------------------------------------------------------------------
 *
 * rangeset.c
 *		IndexRange functions
 *
 * Copyright (c) 2015-2020, Postgres Professional
 *
 * ------------------------------------------------------------------------
 */

#include "compat/pg_compat.h"
#include "rangeset.h"


static IndexRange irange_handle_cover_internal(IndexRange ir_covering,
											   IndexRange ir_inner,
											   List **new_iranges);

static IndexRange irange_union_internal(IndexRange first,
										IndexRange second,
										List **new_iranges);


/* Make union of two conjuncted ranges */
IndexRange
irange_union_simple(IndexRange a, IndexRange b)
{
	/* Ranges should be connected somehow */
	Assert(iranges_intersect(a, b) || iranges_adjoin(a, b));

	return make_irange(Min(irange_lower(a), irange_lower(b)),
					   Max(irange_upper(a), irange_upper(b)),
					   is_irange_lossy(a) && is_irange_lossy(b));
}

/* Get intersection of two conjuncted ranges */
IndexRange
irange_intersection_simple(IndexRange a, IndexRange b)
{
	/* Ranges should be connected somehow */
	Assert(iranges_intersect(a, b) || iranges_adjoin(a, b));

	return make_irange(Max(irange_lower(a), irange_lower(b)),
					   Min(irange_upper(a), irange_upper(b)),
					   is_irange_lossy(a) || is_irange_lossy(b));
}


/* Split covering IndexRange into several IndexRanges if needed */
static IndexRange
irange_handle_cover_internal(IndexRange ir_covering,
							 IndexRange ir_inner,
							 List **new_iranges)
{
	/* Equal lossiness should've been taken into cosideration earlier */
	Assert(is_irange_lossy(ir_covering) != is_irange_lossy(ir_inner));

	/* range 'ir_inner' is lossy */
	if (is_irange_lossy(ir_covering) == false)
		return ir_covering;

	/* range 'ir_covering' is lossy, 'ir_inner' is lossless! */
	else
	{
		IndexRange	ret; /* IndexRange to be returned */

		/* 'left_range_upper' should not be less than 'left_range_lower' */
		uint32		left_range_lower	= irange_lower(ir_covering),
					left_range_upper	= Max(irb_pred(irange_lower(ir_inner)),
											  left_range_lower);

		/* 'right_range_lower' should not be greater than 'right_range_upper' */
		uint32		right_range_upper	= irange_upper(ir_covering),
					right_range_lower	= Min(irb_succ(irange_upper(ir_inner)),
											  right_range_upper);

		/* We have to split the covering lossy IndexRange */
		Assert(is_irange_lossy(ir_covering) == true);

		/* 'ir_inner' should not cover leftmost IndexRange */
		if (irange_lower(ir_inner) > left_range_upper)
		{
			IndexRange	left_range;

			/* Leftmost IndexRange is lossy */
			left_range = make_irange(left_range_lower,
									 left_range_upper,
									 IR_LOSSY);

			/* Append leftmost IndexRange ('left_range') to 'new_iranges' */
			*new_iranges = lappend_irange(*new_iranges, left_range);
		}

		/* 'ir_inner' should not cover rightmost IndexRange */
		if (right_range_lower > irange_upper(ir_inner))
		{
			IndexRange	right_range;

			/* Rightmost IndexRange is also lossy */
			right_range = make_irange(right_range_lower,
									  right_range_upper,
									  IR_LOSSY);

			/* 'right_range' is indeed rightmost IndexRange */
			ret = right_range;

			/* Append medial IndexRange ('ir_inner') to 'new_iranges' */
			*new_iranges = lappend_irange(*new_iranges, ir_inner);
		}
		/* Else return 'ir_inner' as rightmost IndexRange */
		else ret = ir_inner;

		/* Return rightmost IndexRange (right_range | ir_inner) */
		return ret;
	}
}

/* Calculate union of two IndexRanges, return rightmost IndexRange */
static IndexRange
irange_union_internal(IndexRange first,
					  IndexRange second,
					  List **new_iranges)
{
	/* Assert that both IndexRanges are valid */
	Assert(is_irange_valid(first));
	Assert(is_irange_valid(second));

	/* Swap 'first' and 'second' if order is incorrect */
	if (irange_lower(first) > irange_lower(second))
	{
		IndexRange temp;

		temp = first;
		first = second;
		second = temp;
	}

	/* IndexRanges intersect */
	if (iranges_intersect(first, second))
	{
		/* Calculate the union of 'first' and 'second' */
		IndexRange ir_union = irange_union_simple(first, second);

		/* if lossiness is the same, unite them and skip */
		if (is_irange_lossy(first) == is_irange_lossy(second))
			return ir_union;

		/* range 'first' covers 'second' */
		if (irange_eq_bounds(ir_union, first))
		{
			/* Return rightmost IndexRange, save others to 'new_iranges' */
			return irange_handle_cover_internal(first, second, new_iranges);
		}
		/* range 'second' covers 'first' */
		else if (irange_eq_bounds(ir_union, second))
		{
			/* Retun rightmost IndexRange, save others to 'new_iranges' */
			return irange_handle_cover_internal(second, first, new_iranges);
		}
		/* No obvious leader, lossiness differs */
		else
		{
			/* range 'second' is lossy */
			if (is_irange_lossy(first) == false)
			{
				IndexRange	ret;

				/* Set new current IndexRange */
				ret = make_irange(irb_succ(irange_upper(first)),
								  irange_upper(second),
								  is_irange_lossy(second));

				/* Append lower part to 'new_iranges' */
				*new_iranges = lappend_irange(*new_iranges, first);

				/* Return a part of 'second' */
				return ret;
			}
			/* range 'first' is lossy */
			else
			{
				IndexRange	new_irange;

				new_irange = make_irange(irange_lower(first),
										 irb_pred(irange_lower(second)),
										 is_irange_lossy(first));

				/* Append lower part to 'new_iranges' */
				*new_iranges = lappend_irange(*new_iranges, new_irange);

				/* Return 'second' */
				return second;
			}
		}
	}
	/* IndexRanges do not intersect */
	else
	{
		/* Try to unite these IndexRanges if it's possible */
		if (irange_cmp_lossiness(first, second) == IR_EQ_LOSSINESS &&
			iranges_adjoin(first, second))
		{
			/* Return united IndexRange */
			return irange_union_simple(first, second);
		}
		/* IndexRanges are not adjoint */
		else
		{
			/* add 'first' to 'new_iranges' */
			*new_iranges = lappend_irange(*new_iranges, first);

			/* Return 'second' */
			return second;
		}
	}
}

/* Make union of two index rage lists */
List *
irange_list_union(List *a, List *b)
{
	ListCell   *ca,							/* iterator of A */
			   *cb;							/* iterator of B */
	List	   *result = NIL;				/* list of IndexRanges */
	IndexRange	cur = InvalidIndexRange;	/* current irange */

	/* Initialize iterators */
	ca = list_head(a);
	cb = list_head(b);

	/* Loop until we have no cells */
	while (ca || cb)
	{
		IndexRange next = InvalidIndexRange;

		/* Fetch next irange with lesser lower bound */
		if (ca && cb)
		{
			if (irange_lower(lfirst_irange(ca)) <= irange_lower(lfirst_irange(cb)))
			{
				next = lfirst_irange(ca);
				ca = lnext_compat(a, ca); /* move to next cell */
			}
			else
			{
				next = lfirst_irange(cb);
				cb = lnext_compat(b, cb); /* move to next cell */
			}
		}
		/* Fetch next irange from A */
		else if (ca)
		{
			next = lfirst_irange(ca);
			ca = lnext_compat(a, ca); /* move to next cell */
		}
		/* Fetch next irange from B */
		else if (cb)
		{
			next = lfirst_irange(cb);
			cb = lnext_compat(b, cb); /* move to next cell */
		}

		/* Put this irange to 'cur' if don't have it yet */
		if (!is_irange_valid(cur))
		{
			cur = next;
			continue; /* skip this iteration */
		}

		/* Unite 'cur' and 'next' in an appropriate way */
		cur = irange_union_internal(cur, next, &result);
	}

	/* Put current value into result list if any */
	if (is_irange_valid(cur))
		result = lappend_irange(result, cur);

	return result;
}

/* Find intersection of two range lists */
List *
irange_list_intersection(List *a, List *b)
{
	ListCell   *ca,				/* iterator of A */
			   *cb;				/* iterator of B */
	List	   *result = NIL;	/* list of IndexRanges */

	/* Initialize iterators */
	ca = list_head(a);
	cb = list_head(b);

	/* Loop until we have no cells */
	while (ca && cb)
	{
		IndexRange	ra = lfirst_irange(ca),
					rb = lfirst_irange(cb);

		/* Assert that both IndexRanges are valid */
		Assert(is_irange_valid(ra));
		Assert(is_irange_valid(rb));

		/* Only care about intersecting ranges */
		if (iranges_intersect(ra, rb))
		{
			IndexRange	ir_intersection;
			bool		glued_to_last = false;

			/*
			 * Get intersection and try to "glue" it to
			 * last irange, put it separately otherwise.
			 */
			ir_intersection = irange_intersection_simple(ra, rb);
			if (result != NIL)
			{
				IndexRange last = llast_irange(result);

				/* Test if we can glue 'last' and 'ir_intersection' */
				if (irange_cmp_lossiness(last, ir_intersection) == IR_EQ_LOSSINESS &&
					iranges_adjoin(last, ir_intersection))
				{
					IndexRange ir_union = irange_union_simple(last, ir_intersection);

					/* We allocate a new IndexRange for safety */
					llast(result) = alloc_irange(ir_union);

					/* Successfully glued them */
					glued_to_last = true;
				}
			}

			/* Append IndexRange if we couldn't glue it */
			if (!glued_to_last)
				result = lappend_irange(result, ir_intersection);
		}

		/*
		 * Fetch next iranges. We use upper bound of current irange to
		 * determine which lists to fetch, since lower bound of next
		 * irange is greater (or equal) to upper bound of current.
		 */
		if (irange_upper(ra) <= irange_upper(rb))
			ca = lnext_compat(a, ca);
		if (irange_upper(ra) >= irange_upper(rb))
			cb = lnext_compat(b, cb);
	}
	return result;
}

/* Set lossiness of rangeset */
List *
irange_list_set_lossiness(List *ranges, bool lossy)
{
	List	   *result = NIL;
	ListCell   *lc;

	if (ranges == NIL)
		return NIL;

	foreach (lc, ranges)
	{
		IndexRange ir = lfirst_irange(lc);

		result = lappend_irange(result, make_irange(irange_lower(ir),
													irange_upper(ir),
													lossy));
	}

	/* Unite adjacent and overlapping IndexRanges */
	return irange_list_union(result, NIL);
}

/* Get total number of elements in range list */
int
irange_list_length(List *rangeset)
{
	ListCell   *lc;
	uint32		result = 0;

	foreach (lc, rangeset)
	{
		IndexRange	irange = lfirst_irange(lc);
		uint32		diff = irange_upper(irange) - irange_lower(irange);

		Assert(irange_upper(irange) >= irange_lower(irange));

		result += diff + 1;
	}

	return (int) result;
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
				*lossy = is_irange_lossy(irange);

			return true;
		}
	}

	return false;
}
