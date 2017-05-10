/* ------------------------------------------------------------------------
 *
 * debug_print.c
 *		Print sophisticated structs as CSTRING
 *
 * Copyright (c) 2016, Postgres Professional
 *
 * ------------------------------------------------------------------------
 */

#include <unistd.h>
#include "rangeset.h"

#include "postgres.h"
#include "fmgr.h"
#include "executor/tuptable.h"
#include "nodes/bitmapset.h"
#include "nodes/parsenodes.h"
#include "nodes/pg_list.h"
#include "lib/stringinfo.h"
#include "utils/lsyscache.h"


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


/* ----------------
 *		printatt
 * ----------------
 */
static char *
printatt(unsigned attributeId,
		 Form_pg_attribute attributeP,
		 char *value)
{
	return psprintf("\t%2d: %s%s%s%s\t(typeid = %u, len = %d, typmod = %d, byval = %c)\n",
		   attributeId,
		   NameStr(attributeP->attname),
		   value != NULL ? " = \"" : "",
		   value != NULL ? value : "",
		   value != NULL ? "\"" : "",
		   (unsigned int) (attributeP->atttypid),
		   attributeP->attlen,
		   attributeP->atttypmod,
		   attributeP->attbyval ? 't' : 'f');
}

/* ----------------
 *		debugtup - print one tuple for an interactive backend
 * ----------------
 */
static char *
debugtup(TupleTableSlot *slot)
{
	TupleDesc	typeinfo = slot->tts_tupleDescriptor;
	int			natts = typeinfo->natts;
	int			i;
	Datum		attr;
	char	   *value;
	bool		isnull;
	Oid			typoutput;
	bool		typisvarlena;

	int			result_len = 0;
	char	   *result = (char *) palloc(result_len + 1);

	for (i = 0; i < natts; ++i)
	{
		char	*s;
		int		 len;

		attr = slot_getattr(slot, i + 1, &isnull);
		if (isnull)
			continue;
		getTypeOutputInfo(typeinfo->attrs[i]->atttypid,
						  &typoutput, &typisvarlena);

		value = OidOutputFunctionCall(typoutput, attr);

		s = printatt((unsigned) i + 1, typeinfo->attrs[i], value);
		len = strlen(s);
		result = (char *) repalloc(result, result_len + len + 1);
		strncpy(result + result_len, s, len);
		result_len += len;
	}

	result[result_len] = '\0';
	return result;
}

#ifdef __GNUC__
__attribute__((unused))
#endif
static char *
slot_print(TupleTableSlot *slot)
{
	if (TupIsNull(slot))
		return NULL;

	if (!slot->tts_tupleDescriptor)
		return NULL;

	return debugtup(slot);
}

/*
 * rt_print
 *	  return contents of range table
 */
#ifdef __GNUC__
__attribute__((unused))
#endif
static char *
rt_print(const List *rtable)
{
#define APPEND_STR(si, ...) \
{ \
	char *line = psprintf(__VA_ARGS__); \
	appendStringInfo(&si, "%s", line); \
	pfree(line); \
}

	const	ListCell *l;
	int		 i = 1;

	StringInfoData str;

	initStringInfo(&str);
	APPEND_STR(str, "resno\trefname  \trelid\tinFromCl\n");
	APPEND_STR(str, "-----\t---------\t-----\t--------\n");

	foreach(l, rtable)
	{
		RangeTblEntry *rte = lfirst(l);

		switch (rte->rtekind)
		{
			case RTE_RELATION:
				APPEND_STR(str, "%d\t%s\t%u\t%c",
					   i, rte->eref->aliasname, rte->relid, rte->relkind);
				break;
			case RTE_SUBQUERY:
				APPEND_STR(str, "%d\t%s\t[subquery]",
					   i, rte->eref->aliasname);
				break;
			case RTE_JOIN:
				APPEND_STR(str, "%d\t%s\t[join]",
					   i, rte->eref->aliasname);
				break;
			case RTE_FUNCTION:
				APPEND_STR(str, "%d\t%s\t[rangefunction]", i, rte->eref->aliasname);
				break;
			case RTE_VALUES:
				APPEND_STR(str, "%d\t%s\t[values list]", i, rte->eref->aliasname);
				break;
			case RTE_CTE:
				APPEND_STR(str, "%d\t%s\t[cte]", i, rte->eref->aliasname);
				break;
			default:
				elog(ERROR, "%d\t%s\t[unknown rtekind]",
					   i, rte->eref->aliasname);
		}

		APPEND_STR(str, "\t%s\t%s\n", (rte->inh ? "inh" : ""),
			   (rte->inFromCl ? "inFromCl" : ""));

		i++;
	}
	return str.data;
#undef APPEND_STR
}
