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
#include "executor/executor.h"
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
 * CatalogIndexInsert()
 */
#if PG_VERSION_NUM >= 100000
#include "catalog/indexing.h"
void CatalogIndexInsert(CatalogIndexState indstate, HeapTuple heapTuple);
#endif


/*
 * CatalogTupleInsert()
 */
#if PG_VERSION_NUM >= 90500 && PG_VERSION_NUM < 100000
#define CatalogTupleInsert(heapRel, heapTuple) \
	do { \
		simple_heap_insert((heapRel), (heapTuple)); \
		CatalogUpdateIndexes((heapRel), (heapTuple)); \
	} while (0)
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
 * create_nestloop_path()
 */
#if PG_VERSION_NUM >= 100000
#define create_nestloop_path_compat(root, joinrel, jointype, workspace, extra, \
									outer, inner, filtered_joinclauses, pathkeys, \
									required_outer) \
		create_nestloop_path((root), (joinrel), (jointype), (workspace), (extra), \
							 (outer), (inner), (filtered_joinclauses), (pathkeys), \
							 (required_outer))
#elif PG_VERSION_NUM >= 90500
#define create_nestloop_path_compat(root, joinrel, jointype, workspace, extra, \
									outer, inner, filtered_joinclauses, pathkeys, \
									required_outer) \
		create_nestloop_path((root), (joinrel), (jointype), (workspace), \
							 (extra)->sjinfo, &(extra)->semifactors, (outer), \
							 (inner), (filtered_joinclauses), (pathkeys), \
							 (required_outer))
#endif


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
 * ExecEvalExpr
 *
 * 'errmsg' specifies error string when result of ExecEvalExpr doesn't return
 * 		a single value
 */
#if PG_VERSION_NUM >= 100000
#define ExecEvalExprCompat(expr, econtext, isNull, errHandler) \
	ExecEvalExpr((expr), (econtext), (isNull))
#elif PG_VERSION_NUM >= 90500
#include "partition_filter.h"
extern Datum exprResult;
extern ExprDoneCond isDone;
static inline void
not_signle_result_handler()
{
	elog(ERROR, ERR_PART_ATTR_MULTIPLE_RESULTS);
}
#define ExecEvalExprCompat(expr, econtext, isNull, errHandler) \
( \
	exprResult = ExecEvalExpr((expr), (econtext), (isNull), &isDone), \
	(isDone != ExprSingleResult) ? (errHandler)() : (0), \
	exprResult \
)
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
 * get_rel_persistence()
 */
#if PG_VERSION_NUM >= 90500 && PG_VERSION_NUM < 90600
char get_rel_persistence(Oid relid);
#endif


/*
 * initial_cost_nestloop
 */
#if PG_VERSION_NUM >= 100000 || (defined(PGPRO_VERSION) && PG_VERSION_NUM >= 90603)
#define initial_cost_nestloop_compat(root, workspace, jointype, outer_path, \
									 inner_path, extra) \
		initial_cost_nestloop((root), (workspace), (jointype), (outer_path), \
							  (inner_path), (extra))
#elif PG_VERSION_NUM >= 90500
#define initial_cost_nestloop_compat(root, workspace, jointype, outer_path, \
									 inner_path, extra) \
		initial_cost_nestloop((root), (workspace), (jointype), (outer_path), \
							  (inner_path), (extra)->sjinfo, &(extra)->semifactors)
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
 * pg_analyze_and_rewrite
 *
 * for v10 cast first arg to RawStmt type
 */
#if PG_VERSION_NUM >= 100000
#define pg_analyze_and_rewrite_compat(parsetree, query_string, paramTypes, \
									  numParams, queryEnv) \
		pg_analyze_and_rewrite((RawStmt *) (parsetree), (query_string), \
							   (paramTypes), (numParams), (queryEnv))
#elif PG_VERSION_NUM >= 90500
#define pg_analyze_and_rewrite_compat(parsetree, query_string, paramTypes, \
									  numParams, queryEnv) \
		pg_analyze_and_rewrite((Node *) (parsetree), (query_string), \
							   (paramTypes), (numParams))
#endif


/*
 * ProcessUtility
 *
 * for v10 set NULL into 'queryEnv' argument
 */
#if PG_VERSION_NUM >= 100000
#define ProcessUtilityCompat(parsetree, queryString, context, params, dest, \
							 completionTag) \
		do { \
			PlannedStmt *stmt = makeNode(PlannedStmt); \
			stmt->commandType = CMD_UTILITY; \
			stmt->canSetTag = true; \
			stmt->utilityStmt = (parsetree); \
			stmt->stmt_location = -1; \
			stmt->stmt_len = 0; \
			ProcessUtility(stmt, (queryString), (context), (params), NULL, \
						   (dest), (completionTag)); \
		} while (0)
#elif PG_VERSION_NUM >= 90500
#define ProcessUtilityCompat(parsetree, queryString, context, params, dest, \
							 completionTag) \
		ProcessUtility((parsetree), (queryString), (context), (params), \
					   (dest), (completionTag))
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
 * -------------
 *  Common code
 * -------------
 */

void set_append_rel_size_compat(PlannerInfo *root, RelOptInfo *rel, Index rti);
List *init_createstmts_for_partition(RangeVar *parent_rv,
									 RangeVar *partition_rv,
									 char	  *tablespace);


#endif /* PG_COMPAT_H */
