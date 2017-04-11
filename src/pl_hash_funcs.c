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

#include "catalog/pg_type.h"
#include "utils/builtins.h"
#include "utils/typcache.h"
#include "utils/lsyscache.h"
#include "utils/builtins.h"
#include "utils/array.h"


/* Function declarations */

PG_FUNCTION_INFO_V1( create_hash_partitions_internal );

PG_FUNCTION_INFO_V1( get_hash_part_idx );


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
	if (get_pathman_relation_info(parent_relid))
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
