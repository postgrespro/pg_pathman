/* ------------------------------------------------------------------------
 *
 * utils.h
 *		prototypes of various support functions
 *
 * Copyright (c) 2016, Postgres Professional
 *
 * ------------------------------------------------------------------------
 */

#ifndef PATHMAN_UTILS_H
#define PATHMAN_UTILS_H

#include "pathman.h"

#include "postgres.h"
#include "utils/rel.h"
#include "nodes/relation.h"
#include "nodes/nodeFuncs.h"


/*
 * Various traits.
 */
bool clause_contains_params(Node *clause);
bool is_date_type_internal(Oid typid);
bool validate_on_part_init_cb(Oid procid, bool emit_error);
bool check_security_policy_internal(Oid relid, Oid role);

/*
 * Misc.
 */
Oid get_pathman_schema(void);
List * list_reverse(List *l);

#if PG_VERSION_NUM < 90600
char get_rel_persistence(Oid relid);
#endif
Oid get_rel_owner(Oid relid);

Datum perform_type_cast(Datum value, Oid in_type, Oid out_type, bool *success);

/*
 * Handy execution-stage functions.
 */
char * get_rel_name_or_relid(Oid relid);
Oid get_binary_operator_oid(char *opname, Oid arg1, Oid arg2);
void fill_type_cmp_fmgr_info(FmgrInfo *finfo, Oid type1, Oid type2);
char * datum_to_cstring(Datum datum, Oid typid);

#endif
