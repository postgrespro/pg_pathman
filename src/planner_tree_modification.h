/* ------------------------------------------------------------------------
 *
 * planner_tree_modification.h
 *		Functions for query- and plan- tree modification
 *
 * Copyright (c) 2016, Postgres Professional
 *
 * ------------------------------------------------------------------------
 */

#ifndef PLANNER_TREE_MODIFICATION_H
#define PLANNER_TREE_MODIFICATION_H


#include "pathman.h"

#include "postgres.h"
#include "utils/rel.h"
#include "nodes/relation.h"
#include "nodes/nodeFuncs.h"


/* Plan tree rewriting utility */
void plan_tree_walker(Plan *plan,
					  void (*visitor) (Plan *plan, void *context),
					  void *context);

/* Query tree rewriting utility */
void pathman_transform_query(Query *parse);

/* These functions scribble on Plan tree */
void add_partition_filters(List *rtable, Plan *plan);
void postprocess_lock_rows(List *rtable, Plan *plan);


/* used by assign_rel_parenthood_status() etc */
typedef enum
{
	PARENTHOOD_NOT_SET = 0,	/* relation hasn't been tracked */
	PARENTHOOD_DISALLOWED,	/* children are disabled (e.g. ONLY) */
	PARENTHOOD_ALLOWED		/* children are enabled (default) */
} rel_parenthood_status;

void assign_rel_parenthood_status(uint32 query_id, Oid relid,
								  rel_parenthood_status new_status);
rel_parenthood_status get_rel_parenthood_status(uint32 query_id, Oid relid);
void incr_refcount_parenthood_statuses(void);
uint32 get_refcount_parenthood_statuses(void);
void decr_refcount_parenthood_statuses(void);


#endif /* PLANNER_TREE_MODIFICATION_H */
