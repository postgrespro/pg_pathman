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
PG_FUNCTION_INFO_V1( get_partition_hash );


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
	TypeCacheEntry *tce;

	Oid		parent = PG_GETARG_OID(0);
	text   *attname = PG_GETARG_TEXT_P(1);
	uint32	partitions_count = PG_GETARG_UINT32(2);
	uint32	partition_number = PG_GETARG_UINT32(3);
	Oid		attyp;
	char   *result;

	if (partition_number >= partitions_count)
		elog(ERROR,
			 "Partition number cannot exceed partitions count");

	/* Get attribute type and its hash function oid */
	attyp = get_attribute_type(parent, text_to_cstring(attname), false);
	if (attyp == InvalidOid)
		elog(ERROR,
			 "Relation '%s' has no attribute '%s'",
			 get_rel_name(parent),
			 text_to_cstring(attname));

	tce = lookup_type_cache(attyp, TYPECACHE_HASH_PROC);

	/* Create hash condition CSTRING */
	result = psprintf("%s.get_hash_part_idx(%s(%s), %u) = %u",
					  get_namespace_name(get_pathman_schema()),
					  get_func_name(tce->hash_proc),
					  text_to_cstring(attname),
					  partitions_count,
					  partition_number);

	PG_RETURN_TEXT_P(cstring_to_text(result));
}

/*
 * Returns hash value for specified partition (0..N)
 */
Datum
get_partition_hash(PG_FUNCTION_ARGS)
{
	const PartRelationInfo *prel;
	Oid		parent = PG_GETARG_OID(0);
	Oid		partition = PG_GETARG_OID(1);
	Oid	   *children;
	int		i;

	/* Validate partition type */
	prel = get_pathman_relation_info(parent);
	if (!prel || prel->parttype != PT_HASH)
		elog(ERROR,
			 "Relation '%s' isn't partitioned by hash",
			 get_rel_name(parent));

	/* Searching for partition */
	children = PrelGetChildrenArray(prel);
	for (i=0; i<prel->children_count; i++)
		if (children[i] == partition)
			PG_RETURN_UINT32(i);

	/* If we get here then there is no such partition for specified parent */
	elog(ERROR,
		 "Relation '%s' isn't a part of partitioned table '%s'",
		 get_rel_name(parent),
		 get_rel_name(partition));
}
