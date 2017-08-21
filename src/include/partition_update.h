/* ------------------------------------------------------------------------
 *
 * partition_update.h
 *		Insert row to right partition in UPDATE operation
 *
 * Copyright (c) 2017, Postgres Professional
 *
 * ------------------------------------------------------------------------
 */

#ifndef PARTITION_UPDATE_H
#define PARTITION_UPDATE_H

#include "relation_info.h"
#include "utils.h"

#include "postgres.h"
#include "commands/explain.h"
#include "optimizer/planner.h"

#if PG_VERSION_NUM >= 90600
#include "nodes/extensible.h"
#endif

#define UPDATE_NODE_DESCRIPTION ("PartitionRoute")

typedef struct PartitionUpdateState
{
	CustomScanState		css;

	Oid					partitioned_table;
	JunkFilter		   *junkfilter;
	Plan			   *subplan;				/* proxy variable to store subplan */
} PartitionUpdateState;

extern bool					 pg_pathman_enable_partition_update;

extern CustomScanMethods	partition_update_plan_methods;
extern CustomExecMethods	partition_update_exec_methods;

void init_partition_update_static_data(void);
Node *partition_update_create_scan_state(CustomScan *node);

void partition_update_begin(CustomScanState *node, EState *estate, int eflags);
void partition_update_end(CustomScanState *node);
void partition_update_rescan(CustomScanState *node);
void partition_update_explain(CustomScanState *node, List *ancestors,
							  ExplainState *es);

TupleTableSlot *partition_update_exec(CustomScanState *node);

Plan *make_partition_update(Plan *subplan,
							Oid parent_relid,
							Index parent_rti,
							List *returning_list);

#endif /* PARTITION_UPDATE_H */
