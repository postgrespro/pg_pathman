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
#include "utils/builtins.h"


/* Function declarations */

PG_FUNCTION_INFO_V1( create_hash_partitions_internal );

PG_FUNCTION_INFO_V1( get_type_hash_func );
PG_FUNCTION_INFO_V1( get_hash_part_idx );

PG_FUNCTION_INFO_V1( build_hash_condition );


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

	/* Check that there's no partitions yet */
	if (get_pathman_relation_info(parent_relid))
		elog(ERROR, "cannot add new HASH partitions");

	partitioned_col_type = get_attribute_type(parent_relid,
											  TextDatumGetCString(partitioned_col_name),
											  false);

	for (i = 0; i < part_count; i++)
	{
		/* Create a partition (copy FKs, invoke callbacks etc) */
		create_single_hash_partition_internal(parent_relid, i, part_count,
											  partitioned_col_type, NULL, NULL);
	}

	PG_RETURN_VOID();
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
	Oid				parent = PG_GETARG_OID(0);
	text		   *attname = PG_GETARG_TEXT_P(1);
	uint32			part_count = PG_GETARG_UINT32(2);
	uint32			part_idx = PG_GETARG_UINT32(3);

	TypeCacheEntry *tce;
	Oid				attype;
	char		   *attname_cstring = text_to_cstring(attname);

	char		   *result;

	if (part_idx >= part_count)
		elog(ERROR, "'part_idx' must be lower than 'part_count'");

	/* Get attribute type and its hash function oid */
	attype = get_attribute_type(parent, attname_cstring, false);
	if (attype == InvalidOid)
		elog(ERROR, "relation \"%s\" has no attribute \"%s\"",
					get_rel_name(parent),
					attname_cstring);

	tce = lookup_type_cache(attype, TYPECACHE_HASH_PROC);

	/* Create hash condition CSTRING */
	result = psprintf("%s.get_hash_part_idx(%s(%s), %u) = %u",
					  get_namespace_name(get_pathman_schema()),
					  get_func_name(tce->hash_proc),
					  attname_cstring,
					  part_count,
					  part_idx);

	PG_RETURN_TEXT_P(cstring_to_text(result));
}
