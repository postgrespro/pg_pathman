/* ------------------------------------------------------------------------
 *
 * pg_compat.h
 *		Compatibility tools for PostgreSQL API
 *
 * Copyright (c) 2016, Postgres Professional
 *
 * ------------------------------------------------------------------------
 */

#ifndef PG_COMPAT_H
#define PG_COMPAT_H

/* Check PostgreSQL version (9.5.4 contains an important fix for BGW) */
#include "pg_config.h"
#if PG_VERSION_NUM < 90503
	#error "Cannot build pg_pathman with PostgreSQL version lower than 9.5.3"
#elif PG_VERSION_NUM < 90504
	#warning "It is STRONGLY recommended to use pg_pathman with PostgreSQL 9.5.4 since it contains important fixes"
#endif

#include "compat/debug_compat_features.h"

#include "postgres.h"
#include "nodes/memnodes.h"
#include "nodes/relation.h"
#include "nodes/pg_list.h"
#include "optimizer/cost.h"
#include "optimizer/paths.h"
#include "utils/memutils.h"


/*
 * ----------
 *  Variants
 * ----------
 */


/*
 * adjust_appendrel_attrs()
 */
#if PG_VERSION_NUM >= 90600
#define adjust_rel_targetlist_compat(root, dst_rel, src_rel, appinfo) \
	do { \
		(dst_rel)->reltarget->exprs = (List *) \
				adjust_appendrel_attrs((root), \
									   (Node *) (src_rel)->reltarget->exprs, \
									   (appinfo)); \
	} while (0)
#elif PG_VERSION_NUM >= 90500
#define adjust_rel_targetlist_compat(root, dst_rel, src_rel, appinfo) \
	do { \
		(dst_rel)->reltargetlist = (List *) \
				adjust_appendrel_attrs((root), \
									   (Node *) (src_rel)->reltargetlist, \
									   (appinfo)); \
	} while (0)
#endif


/*
 * Define ALLOCSET_DEFAULT_SIZES for our precious MemoryContexts
 */
#if PG_VERSION_NUM >= 90500 && PG_VERSION_NUM < 90600
#define ALLOCSET_DEFAULT_SIZES \
	ALLOCSET_DEFAULT_MINSIZE, ALLOCSET_DEFAULT_INITSIZE, ALLOCSET_DEFAULT_MAXSIZE
#endif


/*
 * DefineRelation()
 *
 * for v10 set NULL into 'queryString' argument as it's used only under vanilla
 * partition creating
 */
#if PG_VERSION_NUM >= 100000
#define DefineRelationCompat(createstmt, relkind, ownerId, typaddress) \
	DefineRelation((createstmt), (relkind), (ownerId), (typaddress), NULL)
#elif PG_VERSION_NUM >= 90500
#define DefineRelationCompat(createstmt, relkind, ownerId, typaddress) \
	DefineRelation((createstmt), (relkind), (ownerId), (typaddress))
#endif


/*
 * CatalogIndexInsert()
 */
#if PG_VERSION_NUM >= 100000
#include "catalog/indexing.h"
void CatalogIndexInsert(CatalogIndexState indstate, HeapTuple heapTuple);
#endif


/*
 * CatalogTupleUpdate()
 */
#if PG_VERSION_NUM >= 90500 && PG_VERSION_NUM < 100000
#define CatalogTupleUpdate(heapRel, updTid, heapTuple) \
	do { \
		simple_heap_update((heapRel), (updTid), (heapTuple)); \
		CatalogUpdateIndexes((heapRel), (heapTuple)); \
	} while (0)
#endif


/*
 * check_index_predicates()
 */
#if PG_VERSION_NUM >= 90600
#define check_index_predicates_compat(rool, rel) \
		check_index_predicates((root), (rel))
#elif PG_VERSION_NUM >= 90500
#define check_index_predicates_compat(rool, rel) \
		check_partial_indexes((root), (rel))
#endif


/*
 * create_append_path()
 */
#if PG_VERSION_NUM >= 100000
#define create_append_path_compat(rel, subpaths, required_outer, parallel_workers) \
		create_append_path((rel), (subpaths), (required_outer), (parallel_workers), NULL)
#elif PG_VERSION_NUM >= 90600

#ifndef PGPRO_VERSION
#define create_append_path_compat(rel, subpaths, required_outer, parallel_workers) \
		create_append_path((rel), (subpaths), (required_outer), (parallel_workers))
#else /* ifdef PGPRO_VERSION */
#define create_append_path_compat(rel, subpaths, required_outer, parallel_workers) \
		create_append_path((rel), (subpaths), (required_outer), \
						   false, NIL, (parallel_workers))
#endif /* PGPRO_VERSION */

#elif PG_VERSION_NUM >= 90500
#define create_append_path_compat(rel, subpaths, required_outer, parallel_workers) \
		create_append_path((rel), (subpaths), (required_outer))
#endif /* PG_VERSION_NUM */


/*
 * create_plain_partial_paths()
 */
#if PG_VERSION_NUM >= 90600
extern void create_plain_partial_paths(PlannerInfo *root,
									   RelOptInfo *rel);
#define create_plain_partial_paths_compat(root, rel) \
		create_plain_partial_paths((root), (rel))
#endif


/*
 * get_parameterized_joinrel_size()
 */
#if PG_VERSION_NUM >= 90600
#define get_parameterized_joinrel_size_compat(root, rel, outer_path, \
											  inner_path, sjinfo, \
											  restrict_clauses) \
		get_parameterized_joinrel_size((root), (rel), (outer_path), \
									   (inner_path), (sjinfo), \
									   (restrict_clauses))
#elif PG_VERSION_NUM >= 90500
#define get_parameterized_joinrel_size_compat(root, rel, \
											  outer_path, \
											  inner_path, \
											  sjinfo, restrict_clauses) \
		get_parameterized_joinrel_size((root), (rel), \
									   (outer_path)->rows, \
									   (inner_path)->rows, \
									   (sjinfo), (restrict_clauses))
#endif


/*
 * InitResultRelInfo
 *
 * for v10 set NULL into 'partition_root' argument to specify that result
 * relation is not vanilla partition
 */
#if PG_VERSION_NUM >= 100000
#define InitResultRelInfoCompat(resultRelInfo, resultRelationDesc, \
								resultRelationIndex, instrument_options) \
		InitResultRelInfo((resultRelInfo), (resultRelationDesc), \
						  (resultRelationIndex), NULL, (instrument_options))
#elif PG_VERSION_NUM >= 90500
#define InitResultRelInfoCompat(resultRelInfo, resultRelationDesc, \
								resultRelationIndex, instrument_options) \
		InitResultRelInfo((resultRelInfo), (resultRelationDesc), \
						  (resultRelationIndex), (instrument_options))
#endif


/*
 * make_result()
 */
#if PG_VERSION_NUM >= 90600
extern Result *make_result(List *tlist,
						   Node *resconstantqual,
						   Plan *subplan);
#define make_result_compat(root, tlist, resconstantqual, subplan) \
		make_result((tlist), (resconstantqual), (subplan))
#elif PG_VERSION_NUM >= 90500
#define make_result_compat(root, tlist, resconstantqual, subplan) \
		make_result((root), (tlist), (resconstantqual), (subplan))
#endif


/*
 * McxtStatsInternal()
 */
#if PG_VERSION_NUM >= 90600
void McxtStatsInternal(MemoryContext context, int level,
					   bool examine_children,
					   MemoryContextCounters *totals);
#endif


/*
 * pull_var_clause()
 */
#if PG_VERSION_NUM >= 90600
#define pull_var_clause_compat(node, aggbehavior, phbehavior) \
		pull_var_clause((node), (aggbehavior) | (phbehavior))
#elif PG_VERSION_NUM >= 90500
#define pull_var_clause_compat(node, aggbehavior, phbehavior) \
		pull_var_clause((node), (aggbehavior), (phbehavior))
#endif


/*
 * set_dummy_rel_pathlist()
 */
#if PG_VERSION_NUM >= 90500 && PG_VERSION_NUM < 90600
void set_dummy_rel_pathlist(RelOptInfo *rel);
#endif


/*
 * set_rel_consider_parallel()
 */
#if PG_VERSION_NUM >= 90600
extern void set_rel_consider_parallel(PlannerInfo *root,
									  RelOptInfo *rel,
									  RangeTblEntry *rte);
#define set_rel_consider_parallel_compat(root, rel, rte) \
		set_rel_consider_parallel((root), (rel), (rte))
#endif


/*
 * get_rel_persistence()
 */
#if PG_VERSION_NUM >= 90500 && PG_VERSION_NUM < 90600
char get_rel_persistence(Oid relid);
#endif


/*
 * -------------
 *  Common code
 * -------------
 */

void set_append_rel_size_compat(PlannerInfo *root, RelOptInfo *rel, Index rti);


#endif /* PG_COMPAT_H */
