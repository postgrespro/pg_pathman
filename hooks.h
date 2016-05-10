/* ------------------------------------------------------------------------
 *
 * hooks.h
 *		prototypes of rel_pathlist and join_pathlist hooks
 *
 * Copyright (c) 2016, Postgres Professional
 *
 * ------------------------------------------------------------------------
 */
#ifndef JOIN_HOOK_H
#define JOIN_HOOK_H

#include "postgres.h"
#include "optimizer/paths.h"

extern set_join_pathlist_hook_type		set_join_pathlist_next;
extern set_rel_pathlist_hook_type		set_rel_pathlist_hook_next;

void pathman_join_pathlist_hook(PlannerInfo *root, RelOptInfo *joinrel, RelOptInfo *outerrel,
								RelOptInfo *innerrel, JoinType jointype, JoinPathExtraData *extra);

void pathman_rel_pathlist_hook(PlannerInfo *root, RelOptInfo *rel, Index rti, RangeTblEntry *rte);

void pg_pathman_enable_assign_hook(char newval, void *extra);

#endif
