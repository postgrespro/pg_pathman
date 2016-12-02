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
#include "parser/parse_oper.h"
#include "utils/rel.h"
#include "nodes/relation.h"
#include "nodes/nodeFuncs.h"


/*
 * Various traits.
 */
bool clause_contains_params(Node *clause);
bool is_date_type_internal(Oid typid);
bool check_security_policy_internal(Oid relid, Oid role);

/*
 * Misc.
 */
Oid get_pathman_schema(void);
List * list_reverse(List *l);

/*
 * Useful functions for relations.
 */
Oid get_rel_owner(Oid relid);
char * get_rel_name_or_relid(Oid relid);
Oid get_attribute_type(Oid relid, const char *attname, bool missing_ok);
#if PG_VERSION_NUM < 90600
char get_rel_persistence(Oid relid);
#endif

/*
 * Operator-related stuff.
 */
Operator get_binary_operator(char *opname, Oid arg1, Oid arg2);
Oid get_operator_ret_type(Operator op);
void fill_type_cmp_fmgr_info(FmgrInfo *finfo, Oid type1, Oid type2);

/*
 * Print values and cast types.
 */
char * datum_to_cstring(Datum datum, Oid typid);
Datum perform_type_cast(Datum value, Oid in_type, Oid out_type, bool *success);


#endif /* PATHMAN_UTILS_H */
