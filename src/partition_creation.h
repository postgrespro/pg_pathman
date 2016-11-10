#include "relation_info.h"

#include "postgres.h"
#include "nodes/parsenodes.h"


Oid
create_single_range_partition_internal(Oid parent_relid,
									   Datum start_value,
									   Datum end_value,
									   Oid value_type,
									   RangeVar *partition_rv,
									   char *tablespace);

Constraint * build_range_check_constraint(Oid child_relid,
										  char *attname,
										  Datum start_value,
										  Datum end_value,
										  Oid value_type);

Node * build_raw_range_check_tree(char *attname,
								  Datum start_value,
								  Datum end_value,
								  Oid value_type);

void invoke_init_callback(Oid parent_relid,
						  Oid child_relid,
						  PartType part_type,
						  Datum start_value,
						  Datum end_value,
						  Oid value_type);
