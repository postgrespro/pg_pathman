/* ------------------------------------------------------------------------
 *
 * partition_router.c
 *		Route row to a right partition in UPDATE operation
 *
 * Copyright (c) 2017, Postgres Professional
 * Portions Copyright (c) 1996-2016, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * ------------------------------------------------------------------------
 */

#include "partition_filter.h"
#include "partition_router.h"
#include "compat/pg_compat.h"

#include "access/xact.h"
#include "access/htup_details.h"
#include "catalog/pg_class.h"
#include "commands/trigger.h"
#include "executor/nodeModifyTable.h"
#include "foreign/fdwapi.h"
#include "utils/guc.h"
#include "utils/rel.h"

bool				pg_pathman_enable_partition_router = true;

CustomScanMethods	partition_router_plan_methods;
CustomExecMethods	partition_router_exec_methods;

static TupleTableSlot *ExecDeleteInternal(ItemPointer tupleid,
										  EPQState *epqstate,
										  EState *estate);

void
init_partition_router_static_data(void)
{
	partition_router_plan_methods.CustomName 			= UPDATE_NODE_NAME;
	partition_router_plan_methods.CreateCustomScanState	= partition_router_create_scan_state;

	partition_router_exec_methods.CustomName			= UPDATE_NODE_NAME;
	partition_router_exec_methods.BeginCustomScan		= partition_router_begin;
	partition_router_exec_methods.ExecCustomScan		= partition_router_exec;
	partition_router_exec_methods.EndCustomScan			= partition_router_end;
	partition_router_exec_methods.ReScanCustomScan		= partition_router_rescan;
	partition_router_exec_methods.MarkPosCustomScan		= NULL;
	partition_router_exec_methods.RestrPosCustomScan	= NULL;
	partition_router_exec_methods.ExplainCustomScan		= partition_router_explain;

	DefineCustomBoolVariable("pg_pathman.enable_partitionrouter",
							 "Enables the planner's use of " UPDATE_NODE_NAME " custom node.",
							 NULL,
							 &pg_pathman_enable_partition_router,
							 false,
							 PGC_USERSET,
							 0,
							 NULL,
							 NULL,
							 NULL);

	RegisterCustomScanMethods(&partition_router_plan_methods);
}

Plan *
make_partition_router(Plan *subplan,
					  Oid parent_relid,
					  Index parent_rti,
					  List *returning_list)

{
	CustomScan *cscan = makeNode(CustomScan);
	Plan	   *pfilter;

	/* Create child PartitionFilter node */
	pfilter = make_partition_filter(subplan,
									parent_relid,
									parent_rti,
									ONCONFLICT_NONE,
									returning_list,
									CMD_UPDATE);

	/* Copy costs etc */
	cscan->scan.plan.startup_cost	= subplan->startup_cost;
	cscan->scan.plan.total_cost		= subplan->total_cost;
	cscan->scan.plan.plan_rows		= subplan->plan_rows;
	cscan->scan.plan.plan_width		= subplan->plan_width;

	/* Setup methods and child plan */
	cscan->methods = &partition_router_plan_methods;
	cscan->custom_plans = list_make1(pfilter);

	/* Build an appropriate target list */
	cscan->scan.plan.targetlist = pfilter->targetlist;

	/* No physical relation will be scanned */
	cscan->scan.scanrelid = 0;

	/* FIXME: should we use the same tlist? */
	cscan->custom_scan_tlist = subplan->targetlist;

	return &cscan->scan.plan;
}

Node *
partition_router_create_scan_state(CustomScan *node)
{
	PartitionRouterState   *state;

	state = (PartitionRouterState *) palloc0(sizeof(PartitionRouterState));
	NodeSetTag(state, T_CustomScanState);

	state->css.flags = node->flags;
	state->css.methods = &partition_router_exec_methods;

	/* Extract necessary variables */
	state->subplan = (Plan *) linitial(node->custom_plans);
	return (Node *) state;
}

void
partition_router_begin(CustomScanState *node, EState *estate, int eflags)
{
	PartitionRouterState   *state = (PartitionRouterState *) node;

	/* It's convenient to store PlanState in 'custom_ps' */
	node->custom_ps = list_make1(ExecInitNode(state->subplan, estate, eflags));
}

TupleTableSlot *
partition_router_exec(CustomScanState *node)
{
	EState					*estate = node->ss.ps.state;
	PlanState				*child_ps = (PlanState *) linitial(node->custom_ps);
	TupleTableSlot			*slot;
	PartitionRouterState	*state = (PartitionRouterState *) node;

	/* execute PartitionFilter child node */
	slot = ExecProcNode(child_ps);

	if (!TupIsNull(slot))
	{
		ResultRelInfo		   *new_rri,	/* new tuple owner */
							   *old_rri;	/* previous tuple owner */
		EPQState				epqstate;
		PartitionFilterState   *child_state;
		char					relkind;
		ItemPointerData			ctid;

		ItemPointerSetInvalid(&ctid);

		child_state = (PartitionFilterState *) child_ps;
		Assert(child_state->command_type == CMD_UPDATE);

		old_rri = child_state->result_parts.base_rri;
		new_rri = estate->es_result_relation_info;

		/* Build new junkfilter if we have to */
		if (state->junkfilter == NULL)
		{
			state->junkfilter =
				ExecInitJunkFilter(state->subplan->targetlist,
								   old_rri->ri_RelationDesc->rd_att->tdhasoid,
								   ExecInitExtraTupleSlotCompat(estate));

			state->junkfilter->jf_junkAttNo =
				ExecFindJunkAttribute(state->junkfilter, "ctid");

			if (!AttributeNumberIsValid(state->junkfilter->jf_junkAttNo))
				elog(ERROR, "could not find junk ctid column");
		}

		relkind = old_rri->ri_RelationDesc->rd_rel->relkind;
		if (relkind == RELKIND_RELATION)
		{
			Datum	ctid_datum;
			bool	ctid_isnull;

			ctid_datum = ExecGetJunkAttribute(child_state->subplan_slot,
											  state->junkfilter->jf_junkAttNo,
											  &ctid_isnull);

			/* shouldn't ever get a null result... */
			if (ctid_isnull)
				elog(ERROR, "ctid is NULL");

			/* Get item pointer to tuple */
			ctid = *(ItemPointer) DatumGetPointer(ctid_datum);
		}
		else if (relkind == RELKIND_FOREIGN_TABLE)
			elog(ERROR, UPDATE_NODE_NAME " does not support foreign tables");
		else
			elog(ERROR, UPDATE_NODE_NAME " cannot handle relkind %u", relkind);

		/*
		 * Clean from junk attributes before INSERT,
		 * but only if slot wasn't transformed in PartitionFilter.
		 */
		if (TupIsNull(child_state->tup_convert_slot))
			slot = ExecFilterJunk(state->junkfilter, slot);

		/* Magic: replace current ResultRelInfo with parent's one (DELETE) */
		estate->es_result_relation_info = old_rri;

		/* Delete tuple from old partition */
		Assert(ItemPointerIsValid(&ctid));
		EvalPlanQualSetSlot(&epqstate, child_state->subplan_slot);
		ExecDeleteInternal(&ctid, &epqstate, estate);

		/* Magic: replace parent's ResultRelInfo with child's one (INSERT) */
		estate->es_result_relation_info = new_rri;

		/* Tuple will be inserted by ModifyTable */
		return slot;
	}

	return NULL;
}

void
partition_router_end(CustomScanState *node)
{
	Assert(list_length(node->custom_ps) == 1);
	ExecEndNode((PlanState *) linitial(node->custom_ps));
}

void
partition_router_rescan(CustomScanState *node)
{
	Assert(list_length(node->custom_ps) == 1);
	ExecReScan((PlanState *) linitial(node->custom_ps));
}

void
partition_router_explain(CustomScanState *node, List *ancestors, ExplainState *es)
{
	/* Nothing to do here now */
}


/*
 * ----------------------------------------------------------------
 *  ExecDeleteInternal
 *		Basicly is a copy of ExecDelete from executor/nodeModifyTable.c
 * ----------------------------------------------------------------
 */

static TupleTableSlot *
ExecDeleteInternal(ItemPointer tupleid,
				   EPQState *epqstate,
				   EState *estate)
{
	ResultRelInfo			*resultRelInfo;
	Relation				 resultRelationDesc;
	HTSU_Result				 result;
	HeapUpdateFailureData	 hufd;

	/*
	 * get information on the (current) result relation
	 */
	resultRelInfo = estate->es_result_relation_info;
	resultRelationDesc = resultRelInfo->ri_RelationDesc;

	/* BEFORE ROW DELETE Triggers */
	if (resultRelInfo->ri_TrigDesc &&
		resultRelInfo->ri_TrigDesc->trig_delete_before_row)
	{
		bool		dodelete;

		dodelete = ExecBRDeleteTriggers(estate, epqstate, resultRelInfo,
										tupleid, NULL);

		if (!dodelete)
			elog(ERROR, "the old row always should be deleted from child table");
	}

	if (tupleid != NULL)
	{
		/* delete the tuple */
ldelete:
		result = heap_delete_compat(resultRelationDesc, tupleid,
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
						Assert(tupleid != NULL);
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
	}
	else
		elog(ERROR, "tupleid should be specified for deletion");

	/* AFTER ROW DELETE Triggers */
	ExecARDeleteTriggersCompat(estate, resultRelInfo, tupleid, NULL, NULL);

	return NULL;
}
