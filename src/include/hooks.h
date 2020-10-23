/* ------------------------------------------------------------------------
 *
 * hooks.h
 *		prototypes of rel_pathlist and join_pathlist hooks
 *
 * Copyright (c) 2016-2020, Postgres Professional
 *
 * ------------------------------------------------------------------------
 */

#ifndef PATHMAN_HOOKS_H
#define PATHMAN_HOOKS_H


#include "postgres.h"
#include "executor/executor.h"
#include "optimizer/planner.h"
#include "optimizer/paths.h"
#include "parser/analyze.h"
#include "storage/ipc.h"
#include "tcop/utility.h"


extern set_join_pathlist_hook_type		pathman_set_join_pathlist_next;
extern set_rel_pathlist_hook_type		pathman_set_rel_pathlist_hook_next;
extern planner_hook_type				pathman_planner_hook_next;
extern post_parse_analyze_hook_type		pathman_post_parse_analyze_hook_next;
extern shmem_startup_hook_type			pathman_shmem_startup_hook_next;
extern ProcessUtility_hook_type			pathman_process_utility_hook_next;
extern ExecutorRun_hook_type			pathman_executor_run_hook_next;


void pathman_join_pathlist_hook(PlannerInfo *root,
								RelOptInfo *joinrel,
								RelOptInfo *outerrel,
								RelOptInfo *innerrel,
								JoinType jointype,
								JoinPathExtraData *extra);

void pathman_rel_pathlist_hook(PlannerInfo *root,
							   RelOptInfo *rel,
							   Index rti,
							   RangeTblEntry *rte);

void pathman_enable_assign_hook(bool newval, void *extra);

PlannedStmt * pathman_planner_hook(Query *parse,
#if PG_VERSION_NUM >= 130000
								   const char *query_string,
#endif
								   int cursorOptions,
								   ParamListInfo boundParams);

void pathman_post_parse_analyze_hook(ParseState *pstate,
									  Query *query);

void pathman_shmem_startup_hook(void);

void pathman_relcache_hook(Datum arg, Oid relid);

#if PG_VERSION_NUM >= 130000
void pathman_process_utility_hook(PlannedStmt *pstmt,
								  const char *queryString,
								  ProcessUtilityContext context,
								  ParamListInfo params,
								  QueryEnvironment *queryEnv,
								  DestReceiver *dest,
								  QueryCompletion *qc);
#elif PG_VERSION_NUM >= 100000
void pathman_process_utility_hook(PlannedStmt *pstmt,
								  const char *queryString,
								  ProcessUtilityContext context,
								  ParamListInfo params,
								  QueryEnvironment *queryEnv,
								  DestReceiver *dest,
								  char *completionTag);
#else
void pathman_process_utility_hook(Node *parsetree,
								  const char *queryString,
								  ProcessUtilityContext context,
								  ParamListInfo params,
								  DestReceiver *dest,
								  char *completionTag);
#endif

#if PG_VERSION_NUM >= 90600
typedef uint64 ExecutorRun_CountArgType;
#else
typedef long ExecutorRun_CountArgType;
#endif

#if PG_VERSION_NUM >= 100000
void pathman_executor_hook(QueryDesc *queryDesc,
						   ScanDirection direction,
						   ExecutorRun_CountArgType count,
						   bool execute_once);
#else
void pathman_executor_hook(QueryDesc *queryDesc,
						   ScanDirection direction,
						   ExecutorRun_CountArgType count);
#endif

#endif /* PATHMAN_HOOKS_H */
