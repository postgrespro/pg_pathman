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


#if PG_VERSION_NUM >= 90600


/* adjust_appendrel_attrs() */
#define adjust_rel_targetlist_compat(root, dst_rel, src_rel, appinfo) \
	do { \
		(dst_rel)->reltarget->exprs = (List *) \
				adjust_appendrel_attrs((root), \
									   (Node *) (src_rel)->reltarget->exprs, \
									   (appinfo)); \
	} while (0)


/* create_append_path() */
#ifndef PGPRO_VERSION
#define create_append_path_compat(rel, subpaths, required_outer, parallel_workers) \
		create_append_path((rel), (subpaths), (required_outer), (parallel_workers))
#else
#define create_append_path_compat(rel, subpaths, required_outer, parallel_workers) \
		create_append_path((rel), (subpaths), (required_outer), \
						   false, NIL, (parallel_workers))
#endif


/* check_index_predicates() */
#define check_index_predicates_compat(rool, rel) \
		check_index_predicates((root), (rel))


/* create_plain_partial_paths() */
extern void create_plain_partial_paths(PlannerInfo *root,
									   RelOptInfo *rel);
#define create_plain_partial_paths_compat(root, rel) \
		create_plain_partial_paths((root), (rel))


/* get_parameterized_joinrel_size() */
#define get_parameterized_joinrel_size_compat(root, rel, outer_path, \
											  inner_path, sjinfo, \
											  restrict_clauses) \
		get_parameterized_joinrel_size((root), (rel), (outer_path), \
									   (inner_path), (sjinfo), \
									   (restrict_clauses))


/* make_result() */
extern Result *make_result(List *tlist,
						   Node *resconstantqual,
						   Plan *subplan);
#define make_result_compat(root, tlist, resconstantqual, subplan) \
		make_result((tlist), (resconstantqual), (subplan))


/* pull_var_clause() */
#define pull_var_clause_compat(node, aggbehavior, phbehavior) \
		pull_var_clause((node), (aggbehavior) | (phbehavior))


/* set_rel_consider_parallel() */
extern void set_rel_consider_parallel(PlannerInfo *root,
									  RelOptInfo *rel,
									  RangeTblEntry *rte);
#define set_rel_consider_parallel_compat(root, rel, rte) \
		set_rel_consider_parallel((root), (rel), (rte))


#else /* PG_VERSION_NUM >= 90500 */


/* adjust_appendrel_attrs() */
#define adjust_rel_targetlist_compat(root, dst_rel, src_rel, appinfo) \
	do { \
		(dst_rel)->reltargetlist = (List *) \
				adjust_appendrel_attrs((root), \
									   (Node *) (src_rel)->reltargetlist, \
									   (appinfo)); \
	} while (0)


/* create_append_path() */
#define create_append_path_compat(rel, subpaths, required_outer, parallel_workers) \
		create_append_path((rel), (subpaths), (required_outer))


/* check_partial_indexes() */
#define check_index_predicates_compat(rool, rel) \
		check_partial_indexes((root), (rel))


/* create_plain_partial_paths() */
#define create_plain_partial_paths_compat(root, rel) ((void) true)


/* get_parameterized_joinrel_size() */
#define get_parameterized_joinrel_size_compat(root, rel, \
											  outer_path, \
											  inner_path, \
											  sjinfo, restrict_clauses) \
		get_parameterized_joinrel_size((root), (rel), \
									   (outer_path)->rows, \
									   (inner_path)->rows, \
									   (sjinfo), (restrict_clauses))


/* make_result() */
#define make_result_compat(root, tlist, resconstantqual, subplan) \
		make_result((root), (tlist), (resconstantqual), (subplan))


/* pull_var_clause() */
#define pull_var_clause_compat(node, aggbehavior, phbehavior) \
		pull_var_clause((node), (aggbehavior), (phbehavior))


/* set_rel_consider_parallel() */
#define set_rel_consider_parallel_compat(root, rel, rte) ((void) true)


/* set_dummy_rel_pathlist() */
void set_dummy_rel_pathlist(RelOptInfo *rel);


#endif /* PG_VERSION_NUM */


#endif /* PG_COMPAT_H */
