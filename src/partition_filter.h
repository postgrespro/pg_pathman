#ifndef RUNTIME_INSERT_H
#define RUNTIME_INSERT_H

#include "postgres.h"
#include "optimizer/paths.h"
#include "optimizer/pathnode.h"

#include "pathman.h"
#include "nodes_common.h"


typedef struct
{
	CustomScanState		css;
	bool				firstStart;
	ResultRelInfo	   *savedRelInfo;
	Plan			   *subplan;
} PartitionFilterState;


extern bool					pg_pathman_enable_partition_filter;

extern CustomScanMethods	partition_filter_plan_methods;
extern CustomExecMethods	partition_filter_exec_methods;


void add_partition_filters(List *rtable, ModifyTable *modify_table);

void init_partition_filter_static_data(void);

Plan * create_partition_filter_plan(Plan *subplan, PartRelationInfo *prel);

Node * partition_filter_create_scan_state(CustomScan *node);

void partition_filter_begin(CustomScanState *node, EState *estate, int eflags);

TupleTableSlot * partition_filter_exec(CustomScanState *node);

void partition_filter_end(CustomScanState *node);

void partition_filter_rescan(CustomScanState *node);

void partition_filter_explain(CustomScanState *node, List *ancestors, ExplainState *es);

#endif
