/* ------------------------------------------------------------------------
 *
 * partition_overseer.h
 *		Restart ModifyTable for unobvious reasons
 *
 * Copyright (c) 2018, Postgres Professional
 *
 * ------------------------------------------------------------------------
 */

#ifndef PARTITION_OVERSEER_H
#define PARTITION_OVERSEER_H

#include "relation_info.h"
#include "utils.h"

#include "postgres.h"
#include "access/tupconvert.h"
#include "commands/explain.h"
#include "optimizer/planner.h"

#if PG_VERSION_NUM >= 90600
#include "nodes/extensible.h"
#endif


#define OVERSEER_NODE_NAME "PartitionOverseer"


extern CustomScanMethods	partition_overseer_plan_methods;
extern CustomExecMethods	partition_overseer_exec_methods;


void init_partition_overseer_static_data(void);
Plan *make_partition_overseer(Plan *subplan);

Node *partition_overseer_create_scan_state(CustomScan *node);

void partition_overseer_begin(CustomScanState *node,
							  EState *estate,
							  int eflags);

TupleTableSlot *partition_overseer_exec(CustomScanState *node);

void partition_overseer_end(CustomScanState *node);

void partition_overseer_rescan(CustomScanState *node);

void partition_overseer_explain(CustomScanState *node,
								List *ancestors,
								ExplainState *es);


#endif /* PARTITION_OVERSEER_H */
