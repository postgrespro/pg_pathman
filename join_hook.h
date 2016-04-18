#ifndef JOIN_HOOK_H
#define JOIN_HOOK_H

#include "postgres.h"
#include "optimizer/paths.h"

extern set_join_pathlist_hook_type	set_join_pathlist_next;

void pathman_join_pathlist_hook(PlannerInfo *root, RelOptInfo *joinrel, RelOptInfo *outerrel,
								RelOptInfo *innerrel, JoinType jointype, JoinPathExtraData *extra);

#endif
