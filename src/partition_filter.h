#ifndef RUNTIME_INSERT_H
#define RUNTIME_INSERT_H

#include "postgres.h"

#include "pathman.h"
#include "nodes_common.h"


typedef struct
{
	Oid					partid;
	ResultRelInfo	   *resultRelInfo;
} ResultRelInfoHandle;

typedef struct
{
	CustomScanState		css;

	Oid					partitioned_table;
	PartRelationInfo   *prel;
	OnConflictAction	onConflictAction;
	ResultRelInfo	   *savedRelInfo;

	Plan			   *subplan;
	Const				temp_const;		/* temporary const for expr walker */

	HTAB			   *result_rels_table;
	HASHCTL				result_rels_table_config;

	WalkerContext		wcxt;
	bool				wcxt_cached;	/* does wcxt contain cached data,
										   e.g. RangeEntry array? */
} PartitionFilterState;


extern bool					pg_pathman_enable_partition_filter;

extern CustomScanMethods	partition_filter_plan_methods;
extern CustomExecMethods	partition_filter_exec_methods;


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
