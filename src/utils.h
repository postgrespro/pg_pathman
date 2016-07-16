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

#include "postgres.h"
#include "utils/rel.h"
#include "nodes/relation.h"
#include "nodes/nodeFuncs.h"
#include "pathman.h"


typedef struct
{
	Oid		old_varno;
	Oid		new_varno;
} change_varno_context;


void execute_on_xact_mcxt_reset(MemoryContext xact_context,
								MemoryContextCallbackFunction cb_proc,
								void *arg);

bool clause_contains_params(Node *clause);

List * build_index_tlist(PlannerInfo *root,
						 IndexOptInfo *index,
						 Relation heapRelation);

bool check_rinfo_for_partitioned_attr(List *rinfo,
									  Index varno,
									  AttrNumber varattno);

TriggerDesc * append_trigger_descs(TriggerDesc *src,
								   TriggerDesc *more,
								   bool *grown_up);

void fill_type_cmp_fmgr_info(FmgrInfo *finfo,
							 Oid type1,
							 Oid type2);

List * list_reverse(List *l);

void change_varnos(Node *node, Oid old_varno, Oid new_varno);

Oid str_to_oid(const char *cstr);

void plan_tree_walker(Plan *plan,
					  void (*visitor) (Plan *plan, void *context),
					  void *context);

void rowmark_add_tableoids(Query *parse);

void postprocess_lock_rows(List *rtable, Plan *plan);

#endif
