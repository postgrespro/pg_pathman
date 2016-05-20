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

typedef struct
{
	RelOptInfo *child;
	RelOptInfo *parent;
	int			sublevels_up;
} ReplaceVarsContext;

bool clause_contains_params(Node *clause);

List * build_index_tlist(PlannerInfo *root, IndexOptInfo *index,
						 Relation heapRelation);

bool check_rinfo_for_partitioned_attr(List *rinfo,
									  Index varno,
									  AttrNumber varattno);

#endif
