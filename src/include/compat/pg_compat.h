/* ------------------------------------------------------------------------
 *
 * pg_compat.h
 *		Compatibility tools for PostgreSQL API
 *
 * Copyright (c) 2016-2020, Postgres Professional
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
#include "access/tupdesc.h"
#include "commands/trigger.h"
#include "executor/executor.h"
#include "nodes/memnodes.h"
#include "nodes/nodeFuncs.h"
#if PG_VERSION_NUM >= 120000
#include "nodes/pathnodes.h"
#else
#include "nodes/relation.h"
#endif
#include "nodes/pg_list.h"
#if PG_VERSION_NUM >= 120000
#include "optimizer/appendinfo.h"
#endif
#include "optimizer/cost.h"
#include "optimizer/paths.h"
#include "optimizer/pathnode.h"
#include "optimizer/prep.h"
#include "utils/memutils.h"

/*
 * ----------
 *  Variants
 * ----------
 */

/*
 * get_attname()
 */
#if PG_VERSION_NUM >= 110000
#define get_attname_compat(relid, attnum) \
	get_attname((relid), (attnum), false)
#else
#define get_attname_compat(relid, attnum) \
	get_attname((relid), (attnum))
#endif


/*
 * calc_nestloop_required_outer
 */
#if PG_VERSION_NUM >= 110000
#define calc_nestloop_required_outer_compat(outer, inner) \
		calc_nestloop_required_outer((outer)->parent->relids, PATH_REQ_OUTER(outer), \
								 (inner)->parent->relids, PATH_REQ_OUTER(inner))
#else
#define calc_nestloop_required_outer_compat(outer, inner) \
		calc_nestloop_required_outer((outer), (inner))
#endif


/*
 * adjust_appendrel_attrs()
 */
#if PG_VERSION_NUM >= 110000
#define adjust_appendrel_attrs_compat(root, node, appinfo) \
		adjust_appendrel_attrs((root), \
							   (node), \
							   1, &(appinfo))
#elif PG_VERSION_NUM >= 90500
#define adjust_appendrel_attrs_compat(root, node, appinfo) \
		adjust_appendrel_attrs((root), \
							   (node), \
							   (appinfo))
#endif


#if PG_VERSION_NUM >= 110000
#define adjust_rel_targetlist_compat(root, dst_rel, src_rel, appinfo) \
	do { \
		(dst_rel)->reltarget->exprs = (List *) \
				adjust_appendrel_attrs((root), \
									   (Node *) (src_rel)->reltarget->exprs, \
									   1,	\
									   &(appinfo)); \
	} while (0)
#elif PG_VERSION_NUM >= 90600
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
 * CheckValidResultRel()
 */
#if PG_VERSION_NUM >= 170000
#define	CheckValidResultRelCompat(rri, cmd) \
		CheckValidResultRel((rri), (cmd), NIL)
#elif PG_VERSION_NUM >= 100000
#define	CheckValidResultRelCompat(rri, cmd) \
		CheckValidResultRel((rri), (cmd))
#elif PG_VERSION_NUM >= 90500
#define	CheckValidResultRelCompat(rri, cmd) \
		CheckValidResultRel((rri)->ri_RelationDesc, (cmd))
#endif

/*
 * BeginCopyFrom()
 */
#if PG_VERSION_NUM >= 140000
#define BeginCopyFromCompat(pstate, rel, filename, is_program, data_source_cb, \
							attnamelist, options) \
		BeginCopyFrom((pstate), (rel), NULL, (filename), (is_program), \
					  (data_source_cb), (attnamelist), (options))
#elif PG_VERSION_NUM >= 100000
#define BeginCopyFromCompat(pstate, rel, filename, is_program, data_source_cb, \
							attnamelist, options) \
		BeginCopyFrom((pstate), (rel), (filename), (is_program), \
					  (data_source_cb), (attnamelist), (options))
#elif PG_VERSION_NUM >= 90500
#define BeginCopyFromCompat(pstate, rel, filename, is_program, data_source_cb, \
							attnamelist, options) \
		BeginCopyFrom((rel), (filename), (is_program), (attnamelist), (options))
#endif


/*
 * build_simple_rel()
 */
#if PG_VERSION_NUM >= 100000
#define build_simple_rel_compat(root, childRTindex, parent_rel) \
		build_simple_rel((root), (childRTindex), (parent_rel))
#elif PG_VERSION_NUM >= 90500
#define build_simple_rel_compat(root, childRTindex, parent_rel) \
		build_simple_rel((root), (childRTindex), \
			(parent_rel) ? RELOPT_OTHER_MEMBER_REL : RELOPT_BASEREL)
#endif


/*
 * Define ALLOCSET_xxx_SIZES for our precious MemoryContexts
 */
#if PG_VERSION_NUM >= 90500 && PG_VERSION_NUM < 90600
#define ALLOCSET_DEFAULT_SIZES \
	ALLOCSET_DEFAULT_MINSIZE, ALLOCSET_DEFAULT_INITSIZE, ALLOCSET_DEFAULT_MAXSIZE

#define ALLOCSET_SMALL_SIZES \
	ALLOCSET_SMALL_MINSIZE, ALLOCSET_SMALL_INITSIZE, ALLOCSET_SMALL_MAXSIZE
#endif


/*
 * call_process_utility_compat()
 *
 * the parameter 'first_arg' is:
 * 	- in pg 10 PlannedStmt object
 * 	- in pg 9.6 and lower Node parsetree
 */
#if PG_VERSION_NUM >= 140000
#define call_process_utility_compat(process_utility, first_arg, query_string, \
									readOnlyTree, context, params, query_env, \
									dest, completion_tag) \
		(process_utility)((first_arg), (query_string), readOnlyTree, \
						  (context), (params), \
						  (query_env), (dest), (completion_tag))
#elif PG_VERSION_NUM >= 100000
#define call_process_utility_compat(process_utility, first_arg, query_string, \
									context, params, query_env, dest, \
									completion_tag) \
		(process_utility)((first_arg), (query_string), (context), (params), \
						  (query_env), (dest), (completion_tag))
#elif PG_VERSION_NUM >= 90500
#define call_process_utility_compat(process_utility, first_arg, query_string, \
									context, params, query_env, dest, \
									completion_tag) \
		(process_utility)((first_arg), (query_string), (context), (params), \
						  (dest), (completion_tag))
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
#if PG_VERSION_NUM >= 140000
#define create_append_path_compat(rel, subpaths, required_outer, parallel_workers) \
	create_append_path(NULL, (rel), (subpaths), NIL, NIL, (required_outer), \
					   (parallel_workers), false, -1)
#elif PG_VERSION_NUM >= 130000
/*
 * PGPRO-3938 made create_append_path compatible with vanilla again
 */
#define create_append_path_compat(rel, subpaths, required_outer, parallel_workers) \
	create_append_path(NULL, (rel), (subpaths), NIL, NIL, (required_outer), \
					   (parallel_workers), false, NIL, -1)
#elif PG_VERSION_NUM >= 120000

#ifndef PGPRO_VERSION
#define create_append_path_compat(rel, subpaths, required_outer, parallel_workers) \
	create_append_path(NULL, (rel), (subpaths), NIL, NIL, (required_outer), \
					   (parallel_workers), false, NIL, -1)
#else
/* TODO pgpro version? Looks like something is not ported yet */
#define create_append_path_compat(rel, subpaths, required_outer, parallel_workers) \
	create_append_path(NULL, (rel), (subpaths), NIL, NIL, (required_outer), \
					   (parallel_workers), false, NIL, -1, false)
#endif							/* PGPRO_VERSION */

#elif PG_VERSION_NUM >= 110000

#ifndef PGPRO_VERSION
#define create_append_path_compat(rel, subpaths, required_outer, parallel_workers) \
	create_append_path(NULL, (rel), (subpaths), NIL, (required_outer), \
					   (parallel_workers), false, NIL, -1)
#else
#define create_append_path_compat(rel, subpaths, required_outer, parallel_workers) \
	create_append_path(NULL, (rel), (subpaths), NIL, (required_outer), \
					   (parallel_workers), false, NIL, -1, false, NIL)
#endif							/* PGPRO_VERSION */

#elif PG_VERSION_NUM >= 100000

#ifndef PGPRO_VERSION
#define create_append_path_compat(rel, subpaths, required_outer, parallel_workers) \
		create_append_path((rel), (subpaths), (required_outer), (parallel_workers), NIL)
#else
#define create_append_path_compat(rel, subpaths, required_outer, parallel_workers) \
		create_append_path((rel), (subpaths), (required_outer), (parallel_workers), NIL, \
						   false, NIL)
#endif							/* PGPRO_VERSION */

#elif PG_VERSION_NUM >= 90600

#ifndef PGPRO_VERSION
#define create_append_path_compat(rel, subpaths, required_outer, parallel_workers) \
		create_append_path((rel), (subpaths), (required_outer), (parallel_workers))
#else
#define create_append_path_compat(rel, subpaths, required_outer, parallel_workers) \
		create_append_path((rel), (subpaths), (required_outer), \
						   false, NIL, (parallel_workers))
#endif							/* PGPRO_VERSION */

#elif PG_VERSION_NUM >= 90500
#define create_append_path_compat(rel, subpaths, required_outer, parallel_workers) \
		create_append_path((rel), (subpaths), (required_outer))
#endif							/* PG_VERSION_NUM */


/*
 * create_merge_append_path()
 */
#if PG_VERSION_NUM >= 140000
#define create_merge_append_path_compat(root, rel, subpaths, pathkeys, \
										required_outer) \
		create_merge_append_path((root), (rel), (subpaths), (pathkeys), \
								 (required_outer))
#elif PG_VERSION_NUM >= 100000
#define create_merge_append_path_compat(root, rel, subpaths, pathkeys, \
										required_outer) \
		create_merge_append_path((root), (rel), (subpaths), (pathkeys), \
								 (required_outer), NIL)
#elif PG_VERSION_NUM >= 90500
#define create_merge_append_path_compat(root, rel, subpaths, pathkeys, \
										required_outer) \
		create_merge_append_path((root), (rel), (subpaths), (pathkeys), \
								 (required_outer))
#endif


/*
 * create_nestloop_path()
 */
#if PG_VERSION_NUM >= 100000 || (defined(PGPRO_VERSION) && PG_VERSION_NUM >= 90603)
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
 * DoCopy()
 */
#if PG_VERSION_NUM >= 100000
#define DoCopyCompat(pstate, copy_stmt, stmt_location, stmt_len, processed) \
	DoCopy((pstate), (copy_stmt), (stmt_location), (stmt_len), (processed))
#elif PG_VERSION_NUM >= 90500
#define DoCopyCompat(pstate, copy_stmt, stmt_location, stmt_len, processed) \
	DoCopy((copy_stmt), (pstate)->p_sourcetext, (processed))
#endif


/*
 * ExecBuildProjectionInfo()
 */
#if PG_VERSION_NUM >= 100000
#define ExecBuildProjectionInfoCompat(targetList, econtext, resultSlot, \
									  ownerPlanState, inputDesc) \
		ExecBuildProjectionInfo((targetList), (econtext), (resultSlot), \
								(ownerPlanState), (inputDesc))
#elif PG_VERSION_NUM >= 90500
#define ExecBuildProjectionInfoCompat(targetList, econtext, resultSlot, \
									  ownerPlanState, inputDesc) \
		ExecBuildProjectionInfo((List *) ExecInitExpr((Expr *) (targetList), \
													  (ownerPlanState)), \
								(econtext), (resultSlot), (inputDesc))
#endif


/*
 * ExecEvalExpr()
 */
#if PG_VERSION_NUM >= 100000
#define ExecEvalExprCompat(expr, econtext, isNull) \
	ExecEvalExpr((expr), (econtext), (isNull))
#elif PG_VERSION_NUM >= 90500
static inline Datum
ExecEvalExprCompat(ExprState *expr, ExprContext *econtext, bool *isnull)
{
	ExprDoneCond isdone;
	Datum		result = ExecEvalExpr(expr, econtext, isnull, &isdone);

	if (isdone != ExprSingleResult)
		elog(ERROR, "expression should return single value");

	return result;
}
#endif


/*
 * ExecCheck()
 */
#if PG_VERSION_NUM < 100000
static inline bool
ExecCheck(ExprState *state, ExprContext *econtext)
{
	Datum		ret;
	bool		isnull;
	MemoryContext old_mcxt;

	/* short-circuit (here and in ExecInitCheck) for empty restriction list */
	if (state == NULL)
		return true;

	old_mcxt = MemoryContextSwitchTo(econtext->ecxt_per_tuple_memory);
	ret = ExecEvalExprCompat(state, econtext, &isnull);
	MemoryContextSwitchTo(old_mcxt);

	if (isnull)
		return true;

	return DatumGetBool(ret);
}
#endif


/*
 * extract_actual_join_clauses()
 */
#if (PG_VERSION_NUM >= 100003) || \
	(PG_VERSION_NUM <  100000 && PG_VERSION_NUM >= 90609) || \
	(PG_VERSION_NUM <   90600 && PG_VERSION_NUM >= 90513)
#define extract_actual_join_clauses_compat(restrictinfo_list, \
										   joinrelids, \
										   joinquals, \
										   otherquals) \
	extract_actual_join_clauses((restrictinfo_list), \
								(joinrelids), \
								(joinquals), \
								(otherquals))
#else
#define extract_actual_join_clauses_compat(restrictinfo_list, \
										   joinrelids, \
										   joinquals, \
										   otherquals) \
	extract_actual_join_clauses((restrictinfo_list), \
								(joinquals), \
								(otherquals))
#endif


/*
 * get_all_actual_clauses()
 */
#if PG_VERSION_NUM >= 100000
extern List *get_all_actual_clauses(List *restrictinfo_list);
#endif


/*
 * get_cheapest_path_for_pathkeys()
 */
#if PG_VERSION_NUM >= 100000
#define get_cheapest_path_for_pathkeys_compat(paths, pathkeys, required_outer, \
											  cost_criterion, \
											  require_parallel_safe) \
		get_cheapest_path_for_pathkeys((paths), (pathkeys), (required_outer), \
									   (cost_criterion), \
									   (require_parallel_safe))
#elif PG_VERSION_NUM >= 90500
#define get_cheapest_path_for_pathkeys_compat(paths, pathkeys, required_outer, \
											  cost_criterion, \
											  require_parallel_safe) \
		get_cheapest_path_for_pathkeys((paths), (pathkeys), (required_outer), \
									   (cost_criterion))
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
char		get_rel_persistence(Oid relid);
#endif


/*
 * initial_cost_nestloop()
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
 * InitResultRelInfo()
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
 * ItemPointerIndicatesMovedPartitions()
 *
 * supported since v11, provide a stub for previous versions.
 */
#if PG_VERSION_NUM < 110000
#define ItemPointerIndicatesMovedPartitions(ctid) ( false )
#endif


/*
 * make_restrictinfo()
 */
#if PG_VERSION_NUM >= 100000
extern List *make_restrictinfos_from_actual_clauses(PlannerInfo *root,
													List *clause_list);
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
void		McxtStatsInternal(MemoryContext context, int level,
							  bool examine_children,
							  MemoryContextCounters *totals);
#endif


/*
 * oid_cmp()
 */
#if PG_VERSION_NUM >=90500 && PG_VERSION_NUM < 100000
extern int	oid_cmp(const void *p1, const void *p2);
#endif


/*
 * parse_analyze()
 *
 * for v10 cast first arg to RawStmt type
 */
#if PG_VERSION_NUM >= 150000	/* for commit 791b1b71da35 */
#define parse_analyze_compat(parse_tree, query_string, param_types, nparams, \
							 query_env) \
		parse_analyze_fixedparams((RawStmt *) (parse_tree), (query_string), (param_types), \
					  (nparams), (query_env))
#elif PG_VERSION_NUM >= 100000
#define parse_analyze_compat(parse_tree, query_string, param_types, nparams, \
							 query_env) \
		parse_analyze((RawStmt *) (parse_tree), (query_string), (param_types), \
					  (nparams), (query_env))
#elif PG_VERSION_NUM >= 90500
#define parse_analyze_compat(parse_tree, query_string, param_types, nparams, \
							 query_env) \
		parse_analyze((Node *) (parse_tree), (query_string), (param_types), \
					  (nparams))
#endif


/*
 * pg_analyze_and_rewrite()
 *
 * for v10 cast first arg to RawStmt type
 */
#if PG_VERSION_NUM >= 150000	/* for commit 791b1b71da35 */
#define pg_analyze_and_rewrite_compat(parsetree, query_string, param_types, \
									  nparams, query_env) \
		pg_analyze_and_rewrite_fixedparams((RawStmt *) (parsetree), (query_string), \
							   (param_types), (nparams), (query_env))
#elif PG_VERSION_NUM >= 100000
#define pg_analyze_and_rewrite_compat(parsetree, query_string, param_types, \
									  nparams, query_env) \
		pg_analyze_and_rewrite((RawStmt *) (parsetree), (query_string), \
							   (param_types), (nparams), (query_env))
#elif PG_VERSION_NUM >= 90500
#define pg_analyze_and_rewrite_compat(parsetree, query_string, param_types, \
									  nparams, query_env) \
		pg_analyze_and_rewrite((Node *) (parsetree), (query_string), \
							   (param_types), (nparams))
#endif


/*
 * ProcessUtility()
 *
 * for v10 set NULL into 'queryEnv' argument
 */
#if PG_VERSION_NUM >= 140000
#define ProcessUtilityCompat(parsetree, queryString, context, params, dest, \
							 completionTag) \
		do { \
			PlannedStmt *stmt = makeNode(PlannedStmt); \
			stmt->commandType	= CMD_UTILITY; \
			stmt->canSetTag		= true; \
			stmt->utilityStmt	= (parsetree); \
			stmt->stmt_location	= -1; \
			stmt->stmt_len		= 0; \
			ProcessUtility(stmt, (queryString), false, (context), (params), NULL, \
						   (dest), (completionTag)); \
		} while (0)
#elif PG_VERSION_NUM >= 100000
#define ProcessUtilityCompat(parsetree, queryString, context, params, dest, \
							 completionTag) \
		do { \
			PlannedStmt *stmt = makeNode(PlannedStmt); \
			stmt->commandType	= CMD_UTILITY; \
			stmt->canSetTag		= true; \
			stmt->utilityStmt	= (parsetree); \
			stmt->stmt_location	= -1; \
			stmt->stmt_len		= 0; \
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
void		set_dummy_rel_pathlist(RelOptInfo *rel);
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
 * tlist_member_ignore_relabel()
 *
 * in compat version the type of first argument is (Expr *)
 */
#if PG_VERSION_NUM >= 100000
#if PG_VERSION_NUM >= 140000	/* function removed in
								 * 375398244168add84a884347625d14581a421e71 */
extern TargetEntry *tlist_member_ignore_relabel(Expr *node, List *targetlist);
#endif
#define tlist_member_ignore_relabel_compat(expr, targetlist) \
		tlist_member_ignore_relabel((expr), (targetlist))
#elif PG_VERSION_NUM >= 90500
#define tlist_member_ignore_relabel_compat(expr, targetlist) \
		tlist_member_ignore_relabel((Node *) (expr), (targetlist))
#endif


/*
 * convert_tuples_by_name_map()
 */
#if (PG_VERSION_NUM >= 90500 && PG_VERSION_NUM <= 90505) || \
	(PG_VERSION_NUM >= 90600 && PG_VERSION_NUM <= 90601)
extern AttrNumber *convert_tuples_by_name_map(TupleDesc indesc,
											  TupleDesc outdesc,
											  const char *msg);
#else
#include "access/tupconvert.h"
#endif

/*
 * ExecBRUpdateTriggers()
 */
#if PG_VERSION_NUM >= 160000
#define ExecBRUpdateTriggersCompat(estate, epqstate, relinfo, \
					 tupleid, fdw_trigtuple, newslot) \
	ExecBRUpdateTriggers((estate), (epqstate), (relinfo), (tupleid), \
						 (fdw_trigtuple), (newslot), NULL, NULL)
#elif PG_VERSION_NUM >= 150000	/* for commit 7103ebb7aae8 */
#define ExecBRUpdateTriggersCompat(estate, epqstate, relinfo, \
					 tupleid, fdw_trigtuple, newslot) \
	ExecBRUpdateTriggers((estate), (epqstate), (relinfo), (tupleid), \
						 (fdw_trigtuple), (newslot), NULL)
#else
#define ExecBRUpdateTriggersCompat(estate, epqstate, relinfo, \
					 tupleid, fdw_trigtuple, newslot) \
	ExecBRUpdateTriggers((estate), (epqstate), (relinfo), (tupleid), \
						 (fdw_trigtuple), (newslot))
#endif

/*
 * ExecARInsertTriggers()
 */
#if PG_VERSION_NUM >= 100000
#define ExecARInsertTriggersCompat(estate, relinfo, trigtuple, \
								   recheck_indexes, transition_capture) \
	ExecARInsertTriggers((estate), (relinfo), (trigtuple), \
						 (recheck_indexes), (transition_capture))
#elif PG_VERSION_NUM >= 90500
#define ExecARInsertTriggersCompat(estate, relinfo, trigtuple, \
								   recheck_indexes, transition_capture) \
	ExecARInsertTriggers((estate), (relinfo), (trigtuple), (recheck_indexes))
#endif


/*
 * ExecBRDeleteTriggers()
 */
#if PG_VERSION_NUM >= 160000
#define ExecBRDeleteTriggersCompat(estate, epqstate, relinfo, tupleid, \
								   fdw_trigtuple, epqslot) \
	ExecBRDeleteTriggers((estate), (epqstate), (relinfo), (tupleid), \
						 (fdw_trigtuple), (epqslot), NULL, NULL)
#elif PG_VERSION_NUM >= 110000
#define ExecBRDeleteTriggersCompat(estate, epqstate, relinfo, tupleid, \
								   fdw_trigtuple, epqslot) \
	ExecBRDeleteTriggers((estate), (epqstate), (relinfo), (tupleid), \
						 (fdw_trigtuple), (epqslot))
#else
#define ExecBRDeleteTriggersCompat(estate, epqstate, relinfo, tupleid, \
								   fdw_trigtuple, epqslot) \
	ExecBRDeleteTriggers((estate), (epqstate), (relinfo), (tupleid), \
						 (fdw_trigtuple))
#endif


/*
 * ExecARDeleteTriggers()
 */
#if PG_VERSION_NUM >= 150000	/* for commit ba9a7e392171 */
#define ExecARDeleteTriggersCompat(estate, relinfo, tupleid, \
								   fdw_trigtuple, transition_capture) \
	ExecARDeleteTriggers((estate), (relinfo), (tupleid), \
						 (fdw_trigtuple), (transition_capture), false)
#elif PG_VERSION_NUM >= 100000
#define ExecARDeleteTriggersCompat(estate, relinfo, tupleid, \
								   fdw_trigtuple, transition_capture) \
	ExecARDeleteTriggers((estate), (relinfo), (tupleid), \
						 (fdw_trigtuple), (transition_capture))
#elif PG_VERSION_NUM >= 90500
#define ExecARDeleteTriggersCompat(estate, relinfo, tupleid, \
								   fdw_trigtuple, transition_capture) \
	ExecARDeleteTriggers((estate), (relinfo), (tupleid), (fdw_trigtuple))
#endif


/*
 * ExecASInsertTriggers()
 */
#if PG_VERSION_NUM >= 100000
#define ExecASInsertTriggersCompat(estate, relinfo, transition_capture) \
	ExecASInsertTriggers((estate), (relinfo), (transition_capture))
#elif PG_VERSION_NUM >= 90500
#define ExecASInsertTriggersCompat(estate, relinfo, transition_capture) \
	ExecASInsertTriggers((estate), (relinfo))
#endif


/*
 * map_variable_attnos()
 */
#if PG_VERSION_NUM >= 100000
#define map_variable_attnos_compat(node, varno, \
								   sublevels_up, map, map_len, \
								   to_rowtype, found_wholerow) \
		map_variable_attnos((node), (varno), \
							(sublevels_up), (map), (map_len), \
							(to_rowtype), (found_wholerow))
#elif PG_VERSION_NUM >= 90500
#define map_variable_attnos_compat(node, varno, \
								   sublevels_up, map, map_len, \
								   to_rowtype, found_wholerow) \
		map_variable_attnos((node), (varno), \
							(sublevels_up), (map), (map_len), \
							(found_wholerow))
#endif

#ifndef TupleDescAttr
#define TupleDescAttr(tupdesc, i) ((tupdesc)->attrs[(i)])
#endif


/*
 * RegisterCustomScanMethods()
 */
#if PG_VERSION_NUM < 90600
#define RegisterCustomScanMethods(methods)
#endif

/*
 * MakeTupleTableSlot()
 */
#if PG_VERSION_NUM >= 120000
#define MakeTupleTableSlotCompat(tts_ops) \
	MakeTupleTableSlot(NULL, (tts_ops))
#elif PG_VERSION_NUM >= 110000
#define MakeTupleTableSlotCompat(tts_ops) \
	MakeTupleTableSlot(NULL)
#else
#define MakeTupleTableSlotCompat(tts_ops) \
	MakeTupleTableSlot()
#endif

/*
 * BackgroundWorkerInitializeConnectionByOid()
 */
#if PG_VERSION_NUM >= 110000
#define BackgroundWorkerInitializeConnectionByOidCompat(dboid, useroid) \
	BackgroundWorkerInitializeConnectionByOid((dboid), (useroid), 0)
#else
#define BackgroundWorkerInitializeConnectionByOidCompat(dboid, useroid) \
	BackgroundWorkerInitializeConnectionByOid((dboid), (useroid))
#endif

/*
 * heap_delete()
 */
#if PG_VERSION_NUM >= 110000
#define heap_delete_compat(relation, tid, cid, crosscheck, \
						   wait, hufd, changing_part) \
	heap_delete((relation), (tid), (cid), (crosscheck), \
				(wait), (hufd), (changing_part))
#else
#define heap_delete_compat(relation, tid, cid, crosscheck, \
						   wait, hufd, changing_part) \
	heap_delete((relation), (tid), (cid), (crosscheck), \
				(wait), (hufd))
#endif

/*
 * compute_parallel_worker()
 */
#if PG_VERSION_NUM >= 110000
#define compute_parallel_worker_compat(rel, heap_pages, index_pages) \
	compute_parallel_worker((rel), (heap_pages), (index_pages), \
							max_parallel_workers_per_gather)
#elif PG_VERSION_NUM >= 100000
#define compute_parallel_worker_compat(rel, heap_pages, index_pages) \
	compute_parallel_worker((rel), (heap_pages), (index_pages))
#endif


/*
 * generate_gather_paths()
 */
#if PG_VERSION_NUM >= 110000
#define generate_gather_paths_compat(root, rel) \
	generate_gather_paths((root), (rel), false)
#elif PG_VERSION_NUM >= 90600
#define generate_gather_paths_compat(root, rel) \
	generate_gather_paths((root), (rel))
#else
#define generate_gather_paths_compat(root, rel)
#endif


/*
 * find_childrel_appendrelinfo()
 */
#if PG_VERSION_NUM >= 110000
#define find_childrel_appendrelinfo_compat(root, rel) \
	((root)->append_rel_array[(rel)->relid])
#else
#define find_childrel_appendrelinfo_compat(root, rel) \
		find_childrel_appendrelinfo((root), (rel))
#endif

/*
 * HeapTupleGetXmin()
 * Vanilla PostgreSQL has HeaptTupleHeaderGetXmin, but for 64-bit xid
 * we need access to entire tuple, not just its header.
 */
#ifdef XID_IS_64BIT
#define HeapTupleGetXminCompat(htup) HeapTupleGetXmin(htup)
#else
#define HeapTupleGetXminCompat(htup) HeapTupleHeaderGetXmin((htup)->t_data)
#endif

/*
 * is_andclause
 */
#if PG_VERSION_NUM >= 120000
#define is_andclause_compat(clause) is_andclause(clause)
#else
#define is_andclause_compat(clause) and_clause(clause)
#endif

/*
 * GetDefaultTablespace
 */
#if PG_VERSION_NUM >= 120000
#define GetDefaultTablespaceCompat(relpersistence, partitioned) \
	GetDefaultTablespace((relpersistence), (partitioned))
#else
#define GetDefaultTablespaceCompat(relpersistence, partitioned) \
	GetDefaultTablespace((relpersistence))
#endif

/*
 * CreateTemplateTupleDesc
 */
#if PG_VERSION_NUM >= 120000
#define CreateTemplateTupleDescCompat(natts, hasoid) CreateTemplateTupleDesc(natts)
#else
#define CreateTemplateTupleDescCompat(natts, hasoid) CreateTemplateTupleDesc((natts), (hasoid))
#endif

/*
 * addRangeTableEntryForRelation
 */
#if PG_VERSION_NUM >= 120000
#define addRangeTableEntryForRelationCompat(pstate, rel, lockmode, alias, inh, inFromCl) \
	addRangeTableEntryForRelation((pstate), (rel), (lockmode), (alias), (inh), (inFromCl))
#else
#define addRangeTableEntryForRelationCompat(pstate, rel, lockmode, alias, inh, inFromCl) \
	addRangeTableEntryForRelation((pstate), (rel), (alias), (inh), (inFromCl))
#endif

/*
 * nextCopyFrom (WITH_OIDS removed)
 */
#if PG_VERSION_NUM >= 120000
#define NextCopyFromCompat(cstate, econtext, values, nulls, tupleOid) \
	NextCopyFrom((cstate), (econtext), (values), (nulls))
#else
#define NextCopyFromCompat(cstate, econtext, values, nulls, tupleOid) \
	NextCopyFrom((cstate), (econtext), (values), (nulls), (tupleOid))
#endif

/*
 * ExecInsertIndexTuples. Since 12 slot contains tupleid.
 * Since 14: new fields "resultRelInfo", "update".
 * Since 16: new bool field "onlySummarizing".
 */
#if PG_VERSION_NUM >= 160000
#define ExecInsertIndexTuplesCompat(resultRelInfo, slot, tupleid, estate, update, noDupError, specConflict, arbiterIndexes, onlySummarizing) \
	ExecInsertIndexTuples((resultRelInfo), (slot), (estate), (update), (noDupError), (specConflict), (arbiterIndexes), (onlySummarizing))
#elif PG_VERSION_NUM >= 140000
#define ExecInsertIndexTuplesCompat(resultRelInfo, slot, tupleid, estate, update, noDupError, specConflict, arbiterIndexes, onlySummarizing) \
	ExecInsertIndexTuples((resultRelInfo), (slot), (estate), (update), (noDupError), (specConflict), (arbiterIndexes))
#elif PG_VERSION_NUM >= 120000
#define ExecInsertIndexTuplesCompat(resultRelInfo, slot, tupleid, estate, update, noDupError, specConflict, arbiterIndexes, onlySummarizing) \
	ExecInsertIndexTuples((slot), (estate), (noDupError), (specConflict), (arbiterIndexes))
#else
#define ExecInsertIndexTuplesCompat(resultRelInfo, slot, tupleid, estate, update, noDupError, specConflict, arbiterIndexes, onlySummarizing) \
	ExecInsertIndexTuples((slot), (tupleid), (estate), (noDupError), (specConflict), (arbiterIndexes))
#endif

/*
 * RenameRelationInternal
 */
#if PG_VERSION_NUM >= 120000
#define RenameRelationInternalCompat(myrelid, newname, is_internal, is_index) \
	RenameRelationInternal((myrelid), (newname), (is_internal), (is_index))
#else
#define RenameRelationInternalCompat(myrelid, newname, is_internal, is_index) \
	RenameRelationInternal((myrelid), (newname), (is_internal))
#endif

/*
 *  getrelid
 */
#if PG_VERSION_NUM >= 120000
#define getrelid(rangeindex,rangetable) \
	(rt_fetch(rangeindex, rangetable)->relid)
#endif

/*
 * AddRelationNewConstraints
 */
#if PG_VERSION_NUM >= 120000
#define AddRelationNewConstraintsCompat(rel, newColDefaults, newConstrains, allow_merge, is_local, is_internal) \
	AddRelationNewConstraints((rel), (newColDefaults), (newConstrains), (allow_merge), (is_local), (is_internal), NULL)
#else
#define AddRelationNewConstraintsCompat(rel, newColDefaults, newConstrains, allow_merge, is_local, is_internal) \
	AddRelationNewConstraints((rel), (newColDefaults), (newConstrains), (allow_merge), (is_local), (is_internal))
#endif

/*
 * [PGPRO-3725] Since 11.7 and 12.1 in pgpro standard and ee PGPRO-2843
 * appeared, changing the signature, wow. There is no numeric pgpro edition
 * macro (and never will be, for old versions), so distinguish via macro added
 * by the commit.
 */
#if defined(QTW_DONT_COPY_DEFAULT) && (PG_VERSION_NUM < 140000)
#define expression_tree_mutator_compat(node, mutator, context) \
	expression_tree_mutator((node), (mutator), (context), 0)
#else
#define expression_tree_mutator_compat(node, mutator, context) \
	expression_tree_mutator((node), (mutator), (context))
#endif

/*
 * stringToQualifiedNameList
 */
#if PG_VERSION_NUM >= 160000
#define stringToQualifiedNameListCompat(string) \
	stringToQualifiedNameList((string), NULL)
#else
#define stringToQualifiedNameListCompat(string) \
	stringToQualifiedNameList((string))
#endif

/*
 * -------------
 *  Common code
 * -------------
 */
#if PG_VERSION_NUM >= 120000
#define ExecInitExtraTupleSlotCompat(estate, tdesc, tts_ops) \
	ExecInitExtraTupleSlot((estate), (tdesc), (tts_ops));
#else
#define ExecInitExtraTupleSlotCompat(estate, tdesc, tts_ops) \
	ExecInitExtraTupleSlotCompatHorse((estate), (tdesc))
static inline TupleTableSlot *
ExecInitExtraTupleSlotCompatHorse(EState *s, TupleDesc t)
{
#if PG_VERSION_NUM >= 110000
	return ExecInitExtraTupleSlot(s, t);
#else
	TupleTableSlot *res = ExecInitExtraTupleSlot(s);

	if (t)
		ExecSetSlotDescriptor(res, t);

	return res;
#endif
}
#endif

/* See ExecEvalParamExtern() */
static inline ParamExternData *
CustomEvalParamExternCompat(Param *param,
							ParamListInfo params,
							ParamExternData *prmdata)
{
	ParamExternData *prm;

#if PG_VERSION_NUM >= 110000
	if (params->paramFetch != NULL)
		prm = params->paramFetch(params, param->paramid, false, prmdata);
	else
		prm = &params->params[param->paramid - 1];
#else
	prm = &params->params[param->paramid - 1];

	if (!OidIsValid(prm->ptype) && params->paramFetch != NULL)
		params->paramFetch(params, param->paramid);
#endif

	return prm;
}

void		set_append_rel_size_compat(PlannerInfo *root, RelOptInfo *rel, Index rti);

/*
 * lnext()
 * In >=13 list implementation was reworked (1cff1b95ab6)
 */
#if PG_VERSION_NUM >= 130000
#define lnext_compat(l, lc)                                    lnext((l), (lc))
#else
#define lnext_compat(l, lc)                                    lnext((lc))
#endif

/*
 * heap_open()
 * heap_openrv()
 * heap_close()
 * In >=13 heap_* was replaced with table_* (e0c4ec07284)
 */
#if PG_VERSION_NUM >= 130000
#define heap_open_compat(r, l)                                 table_open((r), (l))
#define heap_openrv_compat(r, l)                               table_openrv((r), (l))
#define heap_close_compat(r, l)                                table_close((r), (l))
#else
#define heap_open_compat(r, l)                                 heap_open((r), (l))
#define heap_openrv_compat(r, l)                               heap_openrv((r), (l))
#define heap_close_compat(r, l)                                heap_close((r), (l))
#endif

/*
 * convert_tuples_by_name()
 * In >=13 msg parameter in convert_tuples_by_name function was removed (fe66125974c)
 */
#if PG_VERSION_NUM >= 130000
#define convert_tuples_by_name_compat(i, o, m)                 convert_tuples_by_name((i), (o))
#else
#define convert_tuples_by_name_compat(i, o, m)                 convert_tuples_by_name((i), (o), (m))
#endif

/*
 * raw_parser()
 * In 14 new argument was added (844fe9f159a)
 */
#if PG_VERSION_NUM >= 140000
#define raw_parser_compat(s)                                   raw_parser((s), RAW_PARSE_DEFAULT)
#else
#define raw_parser_compat(s)                                   raw_parser(s)
#endif

/*
 * make_restrictinfo()
 * In >=16 4th, 5th and 9th arguments were added (991a3df227e)
 * In >=16 3th and 9th arguments were removed (b448f1c8d83)
 * In >=14 new argument was added (55dc86eca70)
 */
#if PG_VERSION_NUM >= 160000
#define make_restrictinfo_compat(r, c, ipd, od, p, sl, rr, or, nr)  make_restrictinfo((r), (c), (ipd), false, false, (p), (sl), (rr), NULL, (or))
#else
#if PG_VERSION_NUM >= 140000
#define make_restrictinfo_compat(r, c, ipd, od, p, sl, rr, or, nr)  make_restrictinfo((r), (c), (ipd), (od), (p), (sl), (rr), (or), (nr))
#else
#define make_restrictinfo_compat(r, c, ipd, od, p, sl, rr, or, nr)  make_restrictinfo((c), (ipd), (od), (p), (sl), (rr), (or), (nr))
#endif							/* #if PG_VERSION_NUM >= 140000 */
#endif							/* #if PG_VERSION_NUM >= 160000 */

/*
 * pull_varnos()
 * In >=14 new argument was added (55dc86eca70)
 */
#if PG_VERSION_NUM >= 140000
#define pull_varnos_compat(r, n)                               pull_varnos((r), (n))
#else
#define pull_varnos_compat(r, n)                               pull_varnos(n)
#endif

/*
 * build_expression_pathkey()
 * In >=16 argument was removed (b448f1c8d83)
 */
#if PG_VERSION_NUM >= 160000
#define build_expression_pathkey_compat(root, expr, nullable_relids, opno, rel, create_it)   build_expression_pathkey(root, expr, opno, rel, create_it)
#else
#define build_expression_pathkey_compat(root, expr, nullable_relids, opno, rel, create_it)   build_expression_pathkey(root, expr, nullable_relids, opno, rel, create_it)
#endif

/*
 * EvalPlanQualInit()
 * In >=16 argument was added (70b42f27902)
 */
#if PG_VERSION_NUM >= 160000
#define EvalPlanQualInit_compat(epqstate, parentestate, subplan, auxrowmarks, epqParam)    EvalPlanQualInit(epqstate, parentestate, subplan, auxrowmarks, epqParam, NIL)
#else
#define EvalPlanQualInit_compat(epqstate, parentestate, subplan, auxrowmarks, epqParam)    EvalPlanQualInit(epqstate, parentestate, subplan, auxrowmarks, epqParam)
#endif

#endif							/* PG_COMPAT_H */
