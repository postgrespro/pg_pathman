/* ------------------------------------------------------------------------
 *
 * partition_filter.h
 *		Select partition for INSERT operation
 *
 * Copyright (c) 2016, Postgres Professional
 *
 * ------------------------------------------------------------------------
 */

#ifndef RUNTIME_INSERT_H
#define RUNTIME_INSERT_H

#include "relation_info.h"
#include "pathman.h"

#include "postgres.h"
#include "commands/explain.h"
#include "optimizer/planner.h"
#include "nodes/extensible.h"

typedef struct
{
	Oid					partid;
	ResultRelInfo	   *resultRelInfo;
} ResultRelInfoHolder;

typedef struct
{
	CustomScanState		css;

	Oid					partitioned_table;
	OnConflictAction	onConflictAction;
	ResultRelInfo	   *savedRelInfo;

	Plan			   *subplan;
	Const				temp_const;		/* temporary const for expr walker */

	HTAB			   *result_rels_table;
	HASHCTL				result_rels_table_config;

	bool				warning_triggered;
} PartitionFilterState;


extern bool					pg_pathman_enable_partition_filter;

extern CustomScanMethods	partition_filter_plan_methods;
extern CustomExecMethods	partition_filter_exec_methods;


void rowmark_add_tableoids(Query *parse);

void postprocess_lock_rows(List *rtable, Plan *plan);

void add_partition_filters(List *rtable, Plan *plan);

void init_partition_filter_static_data(void);

Plan * make_partition_filter(Plan *subplan,
							 Oid partitioned_table,
							 OnConflictAction conflict_action);

Node * partition_filter_create_scan_state(CustomScan *node);

void partition_filter_begin(CustomScanState *node,
							EState *estate,
							int eflags);

TupleTableSlot * partition_filter_exec(CustomScanState *node);

void partition_filter_end(CustomScanState *node);

void partition_filter_rescan(CustomScanState *node);

void partition_filter_explain(CustomScanState *node,
							  List *ancestors,
							  ExplainState *es);

#endif
