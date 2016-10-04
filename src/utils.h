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


typedef struct
{
	Oid		old_varno;
	Oid		new_varno;
} change_varno_context;


/*
 * Plan tree modification.
 */
void plan_tree_walker(Plan *plan,
					  void (*visitor) (Plan *plan, void *context),
					  void *context);
List * build_index_tlist(PlannerInfo *root,
						 IndexOptInfo *index,
						 Relation heapRelation);
void change_varnos(Node *node, Oid old_varno, Oid new_varno);

/*
 * Rowmark processing.
 */
void rowmark_add_tableoids(Query *parse);
void postprocess_lock_rows(List *rtable, Plan *plan);

/*
 * Various traits.
 */
bool clause_contains_params(Node *clause);
bool is_date_type_internal(Oid typid);
bool is_string_type_internal(Oid typid);
bool validate_on_part_init_cb(Oid procid, bool emit_error);

/*
 * Misc.
 */
Oid get_pathman_schema(void);
List * list_reverse(List *l);

#if PG_VERSION_NUM < 90600
char get_rel_persistence(Oid relid);
#endif

/*
 * Handy execution-stage functions.
 */
char * get_rel_name_or_relid(Oid relid);
char * get_op_name_or_opid(Oid opid);

Oid get_binary_operator_oid(char *opname, Oid arg1, Oid arg2);
void fill_type_cmp_fmgr_info(FmgrInfo *finfo,
							 Oid type1,
							 Oid type2);
char * datum_to_cstring(Datum datum, Oid typid);

#endif
