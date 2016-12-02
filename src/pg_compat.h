/* ------------------------------------------------------------------------
 *
 * pg_compat.h
 *		Compatibility tools
 *
 * Copyright (c) 2016, Postgres Professional
 *
 * ------------------------------------------------------------------------
 */

#ifndef PG_COMPAT_H
#define PG_COMPAT_H


#include "postgres.h"

#include "nodes/relation.h"
#include "nodes/pg_list.h"
#include "optimizer/cost.h"
#include "optimizer/paths.h"


void set_append_rel_size_compat(PlannerInfo *root, RelOptInfo *rel, Index rti);
void adjust_targetlist_compat(PlannerInfo *root, RelOptInfo *dest,
							  RelOptInfo *rel, AppendRelInfo *appinfo);


#if PG_VERSION_NUM >= 90600


#define get_parameterized_joinrel_size_compat(root, rel, outer_path, \
											  inner_path, sjinfo, \
											  restrict_clauses) \
		get_parameterized_joinrel_size(root, rel, outer_path, \
									   inner_path, sjinfo, \
									   restrict_clauses)

#define check_index_predicates_compat(rool, rel) \
		check_index_predicates(root, rel)

#ifndef PGPRO_VERSION
#define create_append_path_compat(rel, subpaths, required_outer, parallel_workers) \
		create_append_path(rel, subpaths, required_outer, parallel_workers)
#else
#define create_append_path_compat(rel, subpaths, required_outer, parallel_workers) \
		create_append_path(rel, subpaths, required_outer, false, NIL, parallel_workers)
#endif

#define pull_var_clause_compat(node, aggbehavior, phbehavior) \
		pull_var_clause(node, aggbehavior | phbehavior)

extern void set_rel_consider_parallel(PlannerInfo *root, RelOptInfo *rel,
									  RangeTblEntry *rte);
#define set_rel_consider_parallel_compat(root, rel, rte) \
		set_rel_consider_parallel(root, rel, rte)

extern void create_plain_partial_paths(PlannerInfo *root, RelOptInfo *rel);
#define create_plain_partial_paths_compat(root, rel) \
		create_plain_partial_paths(root, rel)

extern Result *make_result(List *tlist, Node *resconstantqual, Plan *subplan);
#define make_result_compat(root, tlist, resconstantqual, subplan) \
		make_result(tlist, resconstantqual, subplan)


#else /* PG_VERSION_NUM >= 90500 */


#define get_parameterized_joinrel_size_compat(root, rel, \
											  outer_path, \
											  inner_path, \
											  sjinfo, restrict_clauses) \
		get_parameterized_joinrel_size(root, rel, \
									   (outer_path)->rows, \
									   (inner_path)->rows, \
									   sjinfo, restrict_clauses)

#define check_index_predicates_compat(rool, rel) \
		check_partial_indexes(root, rel)

#define create_append_path_compat(rel, subpaths, required_outer, parallel_workers) \
		create_append_path(rel, subpaths, required_outer)

#define pull_var_clause_compat(node, aggbehavior, phbehavior) \
		pull_var_clause(node, aggbehavior, phbehavior)

#define make_result_compat(root, tlist, resconstantqual, subplan) \
		make_result(root, tlist, resconstantqual, subplan)

#define set_rel_consider_parallel_compat(root, rel, rte) ((void) true)

#define create_plain_partial_paths_compat(root, rel) ((void) true)

void set_dummy_rel_pathlist(RelOptInfo *rel);


#endif /* PG_VERSION_NUM */


#endif /* PG_COMPAT_H */
