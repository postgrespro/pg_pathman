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


extern void set_append_rel_size_compat(PlannerInfo *root, RelOptInfo *rel,
									   Index rti, RangeTblEntry *rte);
extern void copy_targetlist_compat(RelOptInfo *dest, RelOptInfo *rel);

#if PG_VERSION_NUM >= 90600

#define get_parameterized_joinrel_size_compat(root, rel, outer_path, \
											  inner_path, sjinfo, \
											  restrict_clauses) \
		get_parameterized_joinrel_size(root, rel, outer_path, \
									   inner_path, sjinfo, \
									   restrict_clauses)

#define check_index_predicates_compat(rool, rel) \
		check_index_predicates(root, rel)

#define create_append_path_compat(rel, subpaths, required_outer, parallel_workers) \
		create_append_path(rel, subpaths, required_outer, parallel_workers)

#define pull_var_clause_compat(node, aggbehavior, phbehavior) \
		pull_var_clause(node, aggbehavior | phbehavior)

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

#endif


#endif /* PG_COMPAT_H */
