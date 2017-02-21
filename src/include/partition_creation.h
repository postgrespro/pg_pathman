/*-------------------------------------------------------------------------
 *
 * partition_creation.h
 *		Various functions for partition creation.
 *
 * Copyright (c) 2016, Postgres Professional
 *
 *-------------------------------------------------------------------------
 */

#ifndef PARTITION_CREATION_H
#define PARTITION_CREATION_H


#include "relation_info.h"

#include "postgres.h"
#include "nodes/parsenodes.h"


/* ACL privilege for partition creation */
#define ACL_SPAWN_PARTITIONS	 ACL_INSERT


/* Create RANGE partitions to store some value */
Oid create_partitions_for_value(Oid relid, Datum value, Oid value_type);
Oid create_partitions_for_value_internal(Oid relid, Datum value, Oid value_type,
										 bool is_background_worker);


/* Create one RANGE partition */
Oid create_single_range_partition_internal(Oid parent_relid,
										   const Bound *start_value,
										   const Bound *end_value,
										   Oid value_type,
										   RangeVar *partition_rv,
										   char *tablespace);

/* Create one HASH partition */
Oid create_single_hash_partition_internal(Oid parent_relid,
										  uint32 part_idx,
										  uint32 part_count,
										  Oid value_type,
										  RangeVar *partition_rv,
										  char *tablespace);


/* RANGE constraints */
Constraint * build_range_check_constraint(Oid child_relid,
										  char *attname,
										  const Bound *start_value,
										  const Bound *end_value,
										  Oid value_type);

Node * build_raw_range_check_tree(char *attname,
								  const Bound *start_value,
								  const Bound *end_value,
								  Oid value_type);

bool check_range_available(Oid parent_relid,
						   const Bound *start_value,
						   const Bound *end_value,
						   Oid value_type,
						   bool raise_error);


/* HASH constraints */
Constraint * build_hash_check_constraint(Oid child_relid,
										 char *attname,
										 uint32 part_idx,
										 uint32 part_count,
										 Oid value_type);

Node * build_raw_hash_check_tree(char *attname,
								 uint32 part_idx,
								 uint32 part_count, Oid value_type);

void drop_check_constraint(Oid relid, AttrNumber attnum);


/* Partitioning callback type */
typedef enum
{
	PT_INIT_CALLBACK = 0
} part_callback_type;

/* Args for partitioning 'init_callback' */
typedef struct
{
	part_callback_type	cb_type;
	Oid					callback;
	bool				callback_is_cached;

	PartType			parttype;

	Oid					parent_relid;
	Oid					partition_relid;

	union
	{
		struct
		{
			void   *none; /* nothing (struct should have at least 1 element) */
		}	hash_params;

		struct
		{
			Bound		start_value,
						end_value;
			Oid			value_type;
		}	range_params;

	}					params;
} init_callback_params;


#define MakeInitCallbackRangeParams(params_p, cb, parent, child, start, end, type) \
	do \
	{ \
		memset((void *) (params_p), 0, sizeof(init_callback_params)); \
		(params_p)->cb_type = PT_INIT_CALLBACK; \
		(params_p)->callback = (cb); \
		(params_p)->callback_is_cached = false; \
		(params_p)->parttype = PT_RANGE; \
		(params_p)->parent_relid = (parent); \
		(params_p)->partition_relid = (child); \
		(params_p)->params.range_params.start_value = (start); \
		(params_p)->params.range_params.end_value = (end); \
		(params_p)->params.range_params.value_type = (type); \
	} while (0)

#define MakeInitCallbackHashParams(params_p, cb, parent, child) \
	do \
	{ \
		memset((void *) (params_p), 0, sizeof(init_callback_params)); \
		(params_p)->cb_type = PT_INIT_CALLBACK; \
		(params_p)->callback = (cb); \
		(params_p)->callback_is_cached = false; \
		(params_p)->parttype = PT_HASH; \
		(params_p)->parent_relid = (parent); \
		(params_p)->partition_relid = (child); \
	} while (0)


void invoke_part_callback(init_callback_params *cb_params);
bool validate_part_callback(Oid procid, bool emit_error);


#endif /* PARTITION_CREATION_H */
