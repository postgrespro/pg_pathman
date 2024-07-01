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

#include "utils/builtins.h"
#include "utils/typcache.h"
#include "utils/lsyscache.h"


/* Function declarations */

PG_FUNCTION_INFO_V1( create_hash_partitions_internal );

PG_FUNCTION_INFO_V1( get_hash_part_idx );
PG_FUNCTION_INFO_V1( build_hash_condition );


/*
 * Create HASH partitions implementation (written in C).
 */
Datum
create_hash_partitions_internal(PG_FUNCTION_ARGS)
{
/* Free allocated arrays */
#define DeepFreeArray(arr, arr_len) \
	do { \
		int arr_elem; \
		if (!arr) break; \
		for (arr_elem = 0; arr_elem < arr_len; arr_elem++) \
			pfree(arr[arr_elem]); \
		pfree(arr); \
	} while (0)

	Oid			parent_relid = PG_GETARG_OID(0);
	uint32		partitions_count = PG_GETARG_INT32(2),
				i;

	/* Partition names and tablespaces */
	char	  **partition_names			= NULL,
			  **tablespaces				= NULL;
	int			partition_names_size	= 0,
				tablespaces_size		= 0;
	RangeVar  **rangevars				= NULL;

	/* Check that there's no partitions yet */
	if (has_pathman_relation_info(parent_relid))
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("cannot add new HASH partitions")));

	/* Extract partition names */
	if (!PG_ARGISNULL(3))
		partition_names = deconstruct_text_array(PG_GETARG_DATUM(3), &partition_names_size);

	/* Extract partition tablespaces */
	if (!PG_ARGISNULL(4))
		tablespaces = deconstruct_text_array(PG_GETARG_DATUM(4), &tablespaces_size);

	/* Validate size of 'partition_names' */
	if (partition_names && partition_names_size != partitions_count)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("size of 'partition_names' must be equal to 'partitions_count'")));

	/* Validate size of 'tablespaces' */
	if (tablespaces && tablespaces_size != partitions_count)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("size of 'tablespaces' must be equal to 'partitions_count'")));

	/* Convert partition names into RangeVars */
	rangevars = qualified_relnames_to_rangevars(partition_names, partitions_count);

	/* Finally create HASH partitions */
	for (i = 0; i < partitions_count; i++)
	{
		RangeVar   *partition_rv	= rangevars ? rangevars[i] : NULL;
		char 	   *tablespace		= tablespaces ? tablespaces[i] : NULL;

		/* Create a partition (copy FKs, invoke callbacks etc) */
		create_single_hash_partition_internal(parent_relid, i, partitions_count,
											  partition_rv, tablespace);
	}

	/* Free arrays */
	DeepFreeArray(partition_names, partition_names_size);
	DeepFreeArray(tablespaces, tablespaces_size);
	DeepFreeArray(rangevars, partition_names_size);

	PG_RETURN_VOID();
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
	Oid				expr_type	= PG_GETARG_OID(0);
	char		   *expr_cstr	= TextDatumGetCString(PG_GETARG_DATUM(1));
	uint32			part_count	= PG_GETARG_UINT32(2),
					part_idx	= PG_GETARG_UINT32(3);
	char		   *pathman_schema;

	TypeCacheEntry *tce;

	char		   *result;

	if (part_idx >= part_count)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("'partition_index' must be lower than 'partitions_count'")));

	tce = lookup_type_cache(expr_type, TYPECACHE_HASH_PROC);

	/* Check that HASH function exists */
	if (!OidIsValid(tce->hash_proc))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("no hash function for type %s",
						format_type_be(expr_type))));

	pathman_schema = get_namespace_name(get_pathman_schema());
	if (pathman_schema == NULL)
		elog(ERROR, "pg_pathman schema not initialized");

	/* Create hash condition CSTRING */
	result = psprintf("%s.get_hash_part_idx(%s(%s), %u) = %u",
					  pathman_schema,
					  get_func_name(tce->hash_proc),
					  expr_cstr,
					  part_count,
					  part_idx);

	PG_RETURN_TEXT_P(cstring_to_text(result));
}
