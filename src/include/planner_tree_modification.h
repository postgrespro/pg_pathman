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
/* #include "nodes/relation.h" */
#include "nodes/nodeFuncs.h"


/* Query ID generator */
void assign_query_id(Query *query);
void reset_query_id_generator(void);

/* Plan tree rewriting utility */
Plan * plan_tree_visitor(Plan *plan,
					   Plan *(*visitor) (Plan *plan, void *context),
					   void *context);

/* PlanState tree rewriting utility */
void state_tree_visitor(PlanState *state,
						void (*visitor) (PlanState *state, void *context),
						void *context);

/* Query tree rewriting utilities */
void pathman_transform_query(Query *parse, ParamListInfo params);
void pathman_post_analyze_query(Query *parse);

/* These functions scribble on Plan tree */
Plan *add_partition_filters(List *rtable, Plan *plan);
Plan *add_partition_routers(List *rtable, Plan *plan);


/* used by assign_rel_parenthood_status() etc */
typedef enum
{
	PARENTHOOD_NOT_SET = 0,	/* relation hasn't been tracked */
	PARENTHOOD_DISALLOWED,	/* children are disabled (e.g. ONLY) */
	PARENTHOOD_ALLOWED		/* children are enabled (default) */
} rel_parenthood_status;

void assign_rel_parenthood_status(RangeTblEntry *rte,
								  rel_parenthood_status new_status);

rel_parenthood_status get_rel_parenthood_status(RangeTblEntry *rte);


/* used to determine nested planner() calls */
void incr_planner_calls_count(void);
void decr_planner_calls_count(void);
int32 get_planner_calls_count(void);


#endif /* PLANNER_TREE_MODIFICATION_H */
