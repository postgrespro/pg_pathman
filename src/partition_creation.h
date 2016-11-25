/*-------------------------------------------------------------------------
 *
 * partition_creation.h
 *		Various functions for partition creation.
 *
 * Copyright (c) 2016, Postgres Professional
 *
 *-------------------------------------------------------------------------
 */

#include "relation_info.h"

#include "postgres.h"
#include "nodes/parsenodes.h"


Oid create_partitions_for_value(Oid relid, Datum value, Oid value_type);
Oid create_partitions_for_value_internal(Oid relid, Datum value, Oid value_type);

Oid create_single_range_partition_internal(Oid parent_relid,
										   const Infinitable *start_value,
										   const Infinitable *end_value,
										   Oid value_type,
										   RangeVar *partition_rv,
										   char *tablespace);

Constraint * build_range_check_constraint(Oid child_relid,
										  char *attname,
										  const Infinitable *start_value,
										  const Infinitable *end_value,
										  Oid value_type);

Node * build_raw_range_check_tree(char *attname,
								  const Infinitable *start_value,
								  const Infinitable *end_value,
								  Oid value_type);

bool check_range_available(Oid parent_relid,
						   const Infinitable *start_value,
						   const Infinitable *end_value,
						   Oid value_type,
						   bool raise_error);


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
			/* nothing */
		}	hash_params;

		struct
		{
			Infinitable	start_value,
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
		(params_p)->callback = (cb); \
		(params_p)->callback_is_cached = false; \
		(params_p)->cb_type = PT_INIT_CALLBACK; \
		(params_p)->parttype = PT_HASH; \
		(params_p)->parent_relid = (parent); \
		(params_p)->partition_relid = (child); \
	} while (0)

void invoke_part_callback(init_callback_params *cb_params);
