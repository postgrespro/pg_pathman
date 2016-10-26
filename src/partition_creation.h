
#include "postgres.h"
#include "nodes/parsenodes.h"

Constraint *build_range_check_constraint(char *attname,
										 Datum start_value,
										 Datum end_value,
										 Oid value_type);
