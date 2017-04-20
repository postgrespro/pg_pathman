/* ------------------------------------------------------------------------
 *
 * partition_update.c
 *		Insert row to right partition in UPDATE operation
 *
 * Copyright (c) 2017, Postgres Professional
 * Portions Copyright (c) 1996-2016, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * ------------------------------------------------------------------------
 */

#include "partition_filter.h"
#include "partition_update.h"

#include "access/xact.h"
#include "executor/nodeModifyTable.h"
#include "utils/guc.h"

bool				pg_pathman_enable_partition_update = true;

CustomScanMethods	partition_update_plan_methods;
CustomExecMethods	partition_update_exec_methods;

static TupleTableSlot *ExecDeleteInternal(ItemPointer tupleid, HeapTuple oldtuple,
	EPQState *epqstate, EState *estate);

void
init_partition_update_static_data(void)
{
	partition_update_plan_methods.CustomName 			= "PrepareInsert";
	partition_update_plan_methods.CreateCustomScanState	= partition_update_create_scan_state;

	partition_update_exec_methods.CustomName			= "PrepareInsert";
	partition_update_exec_methods.BeginCustomScan		= partition_update_begin;
	partition_update_exec_methods.ExecCustomScan		= partition_update_exec;
	partition_update_exec_methods.EndCustomScan			= partition_update_end;
	partition_update_exec_methods.ReScanCustomScan		= partition_update_rescan;
	partition_update_exec_methods.MarkPosCustomScan		= NULL;
	partition_update_exec_methods.RestrPosCustomScan	= NULL;
	partition_update_exec_methods.ExplainCustomScan		= partition_update_explain;

	DefineCustomBoolVariable("pg_pathman.enable_partitionupdate",
							 "Enables the planner's use of PartitionUpdate custom node.",
							 NULL,
							 &pg_pathman_enable_partition_update,
							 true,
							 PGC_USERSET,
							 0,
							 NULL,
							 NULL,
							 NULL);
}


Plan *
make_partition_update(Plan *subplan,
					  Oid parent_relid,
					  List *returning_list)

{
	Plan		*pfilter;
	CustomScan	*cscan = makeNode(CustomScan);

	/* Copy costs etc */
	cscan->scan.plan.startup_cost = subplan->startup_cost;
	cscan->scan.plan.total_cost = subplan->total_cost;
	cscan->scan.plan.plan_rows = subplan->plan_rows;
	cscan->scan.plan.plan_width = subplan->plan_width;

	/* Setup methods and child plan */
	cscan->methods = &partition_update_plan_methods;
	pfilter = make_partition_filter(subplan, parent_relid, ONCONFLICT_NONE,
									returning_list);
	cscan->custom_plans = list_make1(pfilter);
	cscan->scan.plan.targetlist = pfilter->targetlist;

	/* No physical relation will be scanned */
	cscan->scan.scanrelid = 0;
	cscan->custom_scan_tlist = subplan->targetlist;
	cscan->custom_private = NULL;

	return &cscan->scan.plan;
}

Node *
partition_update_create_scan_state(CustomScan *node)
{
	PartitionUpdateState   *state;

	state = (PartitionUpdateState *) palloc0(sizeof(PartitionUpdateState));
	NodeSetTag(state, T_CustomScanState);

	state->css.flags = node->flags;
	state->css.methods = &partition_update_exec_methods;

	/* Extract necessary variables */
	state->subplan = (Plan *) linitial(node->custom_plans);
	return (Node *) state;
}

void
partition_update_begin(CustomScanState *node, EState *estate, int eflags)
{
	PartitionUpdateState   *state = (PartitionUpdateState *) node;

	/* Initialize PartitionFilter child node */
	node->custom_ps = list_make1(ExecInitNode(state->subplan, estate, eflags));
}

TupleTableSlot *
partition_update_exec(CustomScanState *node)
{
	PlanState				*child_ps = (PlanState *) linitial(node->custom_ps);
	EState					*estate = node->ss.ps.state;
	TupleTableSlot			*slot;
	ResultRelInfo			*saved_rel_info;

	/* save original ResultRelInfo */
	saved_rel_info = estate->es_result_relation_info;

	/* execute PartitionFilter child node */
	slot = ExecProcNode(child_ps);

	if (!TupIsNull(slot))
	{
		char			 relkind;
		Datum			 datum;
		bool			 isNull;
		ResultRelInfo	*resultRelInfo;
		HeapTuple		 oldtuple;
		ItemPointer		 tupleid;
		ItemPointerData	 tuple_ctid;
		JunkFilter		*junkfilter;
		EPQState		 epqstate;
		AttrNumber		 ctid_attno;

		resultRelInfo = estate->es_result_relation_info;
		junkfilter = resultRelInfo->ri_junkFilter;
		Assert(junkfilter != NULL);

		EvalPlanQualSetSlot(&epqstate, slot);
		oldtuple = NULL;

		/*
		 * extract the 'ctid' junk attribute.
		 */
		relkind = resultRelInfo->ri_RelationDesc->rd_rel->relkind;
		Assert(relkind == RELKIND_RELATION);
		ctid_attno = ExecFindJunkAttribute(junkfilter, "ctid");
		datum = ExecGetJunkAttribute(slot, ctid_attno, &isNull);
		/* shouldn't ever get a null result... */
		if (isNull)
			elog(ERROR, "ctid is NULL");

		tupleid = (ItemPointer) DatumGetPointer(datum);
		tuple_ctid = *tupleid;		/* be sure we don't free
									 * ctid!! */
		tupleid = &tuple_ctid;

		/* delete old tuple */
		estate->es_result_relation_info = saved_rel_info;
		ExecDeleteInternal(tupleid, oldtuple, &epqstate, estate);
		estate->es_result_relation_info = resultRelInfo;

		/* we got the slot that can be inserted to child partition */
		return slot;
	}

	return NULL;
}

void
partition_update_end(CustomScanState *node)
{
	Assert(list_length(node->custom_ps) == 1);
	ExecEndNode((PlanState *) linitial(node->custom_ps));
}

void
partition_update_rescan(CustomScanState *node)
{
	Assert(list_length(node->custom_ps) == 1);
	ExecReScan((PlanState *) linitial(node->custom_ps));
}

void
partition_update_explain(CustomScanState *node, List *ancestors, ExplainState *es)
{
	/* Nothing to do here now */
}


/* ----------------------------------------------------------------
 *		ExecDeleteInternal
 *		Basicly copy of ExecDelete from executor/nodeModifyTable.c
 * ----------------------------------------------------------------
 */
static TupleTableSlot *
ExecDeleteInternal(ItemPointer tupleid,
		   HeapTuple oldtuple,
		   EPQState *epqstate,
		   EState *estate)
{
	ResultRelInfo *resultRelInfo;
	Relation	resultRelationDesc;
	HTSU_Result result;
	HeapUpdateFailureData hufd;

	/*
	 * get information on the (current) result relation
	 */
	resultRelInfo = estate->es_result_relation_info;
	resultRelationDesc = resultRelInfo->ri_RelationDesc;

ldelete:;
	result = heap_delete(resultRelationDesc, tupleid,
						 estate->es_output_cid,
						 estate->es_crosscheck_snapshot,
						 true /* wait for commit */ ,
						 &hufd);
	switch (result)
	{
		case HeapTupleSelfUpdated:
			if (hufd.cmax != estate->es_output_cid)
				ereport(ERROR,
						(errcode(ERRCODE_TRIGGERED_DATA_CHANGE_VIOLATION),
						 errmsg("tuple to be updated was already modified by an operation triggered by the current command"),
						 errhint("Consider using an AFTER trigger instead of a BEFORE trigger to propagate changes to other rows.")));

			/* Else, already deleted by self; nothing to do */
			return NULL;

		case HeapTupleMayBeUpdated:
			break;

		case HeapTupleUpdated:
			if (IsolationUsesXactSnapshot())
				ereport(ERROR,
						(errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),
						 errmsg("could not serialize access due to concurrent update")));
			if (!ItemPointerEquals(tupleid, &hufd.ctid))
			{
				TupleTableSlot *epqslot;

				epqslot = EvalPlanQual(estate,
									   epqstate,
									   resultRelationDesc,
									   resultRelInfo->ri_RangeTableIndex,
									   LockTupleExclusive,
									   &hufd.ctid,
									   hufd.xmax);
				if (!TupIsNull(epqslot))
				{
					*tupleid = hufd.ctid;
					goto ldelete;
				}
			}
			/* tuple already deleted; nothing to do */
			return NULL;

		default:
			elog(ERROR, "unrecognized heap_delete status: %u", result);
			return NULL;
	}

	return NULL;
}
