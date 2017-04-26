/* ------------------------------------------------------------------------
 *
 * debug_print.c
 *		Print sophisticated structs as CSTRING
 *
 * Copyright (c) 2016, Postgres Professional
 *
 * ------------------------------------------------------------------------
 */

#include "rangeset.h"

#include "postgres.h"
#include "nodes/bitmapset.h"
#include "nodes/pg_list.h"
#include "lib/stringinfo.h"


/*
 * Print Bitmapset as cstring.
 */
#ifdef __GNUC__
__attribute__((unused))
#endif
static char *
bms_print(Bitmapset *bms)
{
	StringInfoData	str;
	int				x;

	initStringInfo(&str);
	x = -1;
	while ((x = bms_next_member(bms, x)) >= 0)
		appendStringInfo(&str, " %d", x);

	return str.data;
}

/*
 * Print list of IndexRanges as cstring.
 */
#ifdef __GNUC__
__attribute__((unused))
#endif
static char *
rangeset_print(List *rangeset)
{
	StringInfoData	str;
	ListCell	   *lc;
	bool			first_irange = true;
	char			lossy = 'L',		/* Lossy IndexRange */
					complete = 'C';		/* Complete IndexRange */

	initStringInfo(&str);

	foreach (lc, rangeset)
	{
		IndexRange	irange = lfirst_irange(lc);

		/* Append comma if needed */
		if (!first_irange)
			appendStringInfo(&str, ", ");

		if (!is_irange_valid(irange))
			appendStringInfo(&str, "X");
		else if (irange_lower(irange) == irange_upper(irange))
			appendStringInfo(&str, "%u%c",
							 irange_lower(irange),
							 (is_irange_lossy(irange) ? lossy : complete));
		else
			appendStringInfo(&str, "[%u-%u]%c",
							 irange_lower(irange), irange_upper(irange),
							 (is_irange_lossy(irange) ? lossy : complete));

		first_irange = false;
	}

	return str.data;
}

/*
 * Print IndexRange struct as cstring.
 */
#ifdef __GNUC__
__attribute__((unused))
#endif
static char *
irange_print(IndexRange irange)
{
	StringInfoData	str;

	initStringInfo(&str);

	appendStringInfo(&str, "{ valid: %s, lossy: %s, lower: %u, upper: %u }",
					 (is_irange_valid(irange) ? "true" : "false"),
					 (is_irange_lossy(irange) ? "true" : "false"),
					 irange_lower(irange),
					 irange_upper(irange));

	return str.data;
}
