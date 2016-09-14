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


/*
 * Single element of 'result_rels_table'.
 */
typedef struct
{
	Oid					partid;
	ResultRelInfo	   *result_rel_info;
} ResultRelInfoHolder;

/*
 * Cached ResultRelInfos of partitions.
 */
typedef struct
{
	ResultRelInfo	   *saved_rel_info;
	HTAB			   *result_rels_table;
	HASHCTL				result_rels_table_config;
} ResultPartsStorage;

typedef struct
{
	CustomScanState		css;

	Oid					partitioned_table;
	OnConflictAction	on_conflict_action;

	Plan			   *subplan;		/* proxy variable to store subplan */
	ResultPartsStorage	result_parts;

	bool				warning_triggered;
} PartitionFilterState;


extern bool					pg_pathman_enable_partition_filter;

extern CustomScanMethods	partition_filter_plan_methods;
extern CustomExecMethods	partition_filter_exec_methods;


void init_partition_filter_static_data(void);

void add_partition_filters(List *rtable, Plan *plan);

/* ResultPartsStorage init\fini\scan function */
void init_result_parts_storage(ResultPartsStorage *parts_storage);
void fini_result_parts_storage(ResultPartsStorage *parts_storage);
ResultRelInfo * scan_result_parts_storage(Oid partid,
										  ResultPartsStorage *storage,
										  bool speculative_insertion);

/* */
Oid *find_partitions_for_value(Datum value, const PartRelationInfo *prel,
							   ExprContext *econtext, int *nparts);

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
