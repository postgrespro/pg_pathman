/* ------------------------------------------------------------------------
 *
 * utils.h
 *		prototypes of various support functions
 *
 * Copyright (c) 2016, Postgres Professional
 *
 * ------------------------------------------------------------------------
 */

#ifndef UTILS_H
#define UTILS_H

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
TriggerDesc * append_trigger_descs(TriggerDesc *src,
								   TriggerDesc *more,
								   bool *grown_up);

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
bool check_rinfo_for_partitioned_attr(List *rinfo,
									  Index varno,
									  AttrNumber varattno);

/*
 * Misc.
 */
Oid get_pathman_schema(void);
List * list_reverse(List *l);

/*
 * Handy execution-stage functions.
 */
char * get_rel_name_or_relid(Oid relid);
Oid get_binary_operator_oid(char *opname, Oid arg1, Oid arg2);
void fill_type_cmp_fmgr_info(FmgrInfo *finfo,
							 Oid type1,
							 Oid type2);
void execute_on_xact_mcxt_reset(MemoryContext xact_context,
								MemoryContextCallbackFunction cb_proc,
								void *arg);
char * datum_to_cstring(Datum datum, Oid typid);


#endif
