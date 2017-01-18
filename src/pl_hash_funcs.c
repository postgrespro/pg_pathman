/* ------------------------------------------------------------------------
 *
 * pl_hash_funcs.c
 *		Utility C functions for stored HASH procedures
 *
 * Copyright (c) 2016, Postgres Professional
 *
 * ------------------------------------------------------------------------
 */

#include "pathman.h"
#include "partition_creation.h"
#include "relation_info.h"
#include "utils.h"

#include "catalog/namespace.h"
#include "catalog/pg_type.h"
#include "utils/builtins.h"
#include "utils/typcache.h"
#include "utils/lsyscache.h"
#include "utils/builtins.h"
#include "utils/array.h"


/* Function declarations */

PG_FUNCTION_INFO_V1( create_hash_partitions_internal );

PG_FUNCTION_INFO_V1( get_type_hash_func );
PG_FUNCTION_INFO_V1( get_hash_part_idx );

PG_FUNCTION_INFO_V1( build_hash_condition );


static char **deconstruct_text_array(Datum arr, int *num_elems);


/*
 * Create HASH partitions implementation (written in C).
 */
Datum
create_hash_partitions_internal(PG_FUNCTION_ARGS)
{
	Oid		parent_relid = PG_GETARG_OID(0);
	Datum	partitioned_col_name = PG_GETARG_DATUM(1);
	Oid		partitioned_col_type;
	uint32	part_count = PG_GETARG_INT32(2),
			i;

	/* Partitions names and tablespaces */
	char  **names = NULL,
		  **tablespaces = NULL;
	int		names_size = 0,
			tablespaces_size = 0;
	RangeVar **rangevars = NULL;

	/* Check that there's no partitions yet */
	if (get_pathman_relation_info(parent_relid))
		elog(ERROR, "cannot add new HASH partitions");

	partitioned_col_type = get_attribute_type(parent_relid,
											  TextDatumGetCString(partitioned_col_name),
											  false);

	/* Get partition names and tablespaces */
	if (!PG_ARGISNULL(3))
		names = deconstruct_text_array(PG_GETARG_DATUM(3), &names_size);

	if (!PG_ARGISNULL(4))
		tablespaces = deconstruct_text_array(PG_GETARG_DATUM(4), &tablespaces_size);

	/* Convert partition names into RangeVars */
	if (names_size > 0)
	{
		rangevars = palloc(sizeof(RangeVar) * names_size);
		for (i = 0; i < names_size; i++)
		{
			List *nl = stringToQualifiedNameList(names[i]);

			rangevars[i] = makeRangeVarFromNameList(nl);
		}
	}

	for (i = 0; i < part_count; i++)
	{
		RangeVar   *rel = rangevars != NULL ? rangevars[i] : NULL;
		char 	   *tablespace = tablespaces != NULL ? tablespaces[i] : NULL;

		/* Create a partition (copy FKs, invoke callbacks etc) */
		create_single_hash_partition_internal(parent_relid, i, part_count,
											  partitioned_col_type,
											  rel, tablespace);
	}

	PG_RETURN_VOID();
}

/*
 * Convert Datum into cstring array
 */
static char **
deconstruct_text_array(Datum arr, int *num_elems)
{
	ArrayType  *arrayval;
	int16		elemlen;
	bool		elembyval;
	char		elemalign;
	Datum	   *elem_values;
	bool	   *elem_nulls;
	int16		i;

	arrayval = DatumGetArrayTypeP(arr);

	Assert(ARR_ELEMTYPE(arrayval) == TEXTOID);

	get_typlenbyvalalign(ARR_ELEMTYPE(arrayval),
						 &elemlen, &elembyval, &elemalign);
	deconstruct_array(arrayval,
					  ARR_ELEMTYPE(arrayval),
					  elemlen, elembyval, elemalign,
					  &elem_values, &elem_nulls, num_elems);

	/* If there are actual values then convert them into cstrings */
	if (num_elems > 0)
	{
		char **strings = palloc(sizeof(char *) * *num_elems);

		for (i = 0; i < *num_elems; i++)
		{
			if (elem_nulls[i])
				elog(ERROR,
					 "Partition name and tablespace arrays cannot contain nulls");

			strings[i] = TextDatumGetCString(elem_values[i]);
		}

		return strings;
	}

	return NULL;
}

/*
 * Returns hash function's OID for a specified type.
 */
Datum
get_type_hash_func(PG_FUNCTION_ARGS)
{
	TypeCacheEntry *tce;
	Oid 			type_oid = PG_GETARG_OID(0);

	tce = lookup_type_cache(type_oid, TYPECACHE_HASH_PROC);

	PG_RETURN_OID(tce->hash_proc);
}

/*
 * Wrapper for hash_to_part_index().
 */
Datum
get_hash_part_idx(PG_FUNCTION_ARGS)
{
	uint32	value = PG_GETARG_UINT32(0),
			part_count = PG_GETARG_UINT32(1);

	PG_RETURN_UINT32(hash_to_part_index(value, part_count));
}

/*
 * Build hash condition for a CHECK CONSTRAINT
 */
Datum
build_hash_condition(PG_FUNCTION_ARGS)
{
	Oid				atttype = PG_GETARG_OID(0);
	text		   *attname = PG_GETARG_TEXT_P(1);
	uint32			part_count = PG_GETARG_UINT32(2),
					part_idx = PG_GETARG_UINT32(3);

	TypeCacheEntry *tce;
	char		   *attname_cstring = text_to_cstring(attname);

	char		   *result;

	if (part_idx >= part_count)
		elog(ERROR, "'partition_index' must be lower than 'partitions_count'");

	tce = lookup_type_cache(atttype, TYPECACHE_HASH_PROC);

	/* Check that HASH function exists */
	if (!OidIsValid(tce->hash_proc))
		elog(ERROR, "no hash function for type %s", format_type_be(atttype));

	/* Create hash condition CSTRING */
	result = psprintf("%s.get_hash_part_idx(%s(%s), %u) = %u",
					  get_namespace_name(get_pathman_schema()),
					  get_func_name(tce->hash_proc),
					  attname_cstring,
					  part_count,
					  part_idx);

	PG_RETURN_TEXT_P(cstring_to_text(result));
}
