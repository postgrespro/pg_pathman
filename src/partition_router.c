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

static TupleTableSlot *router_delete_tuple(TupleTableSlot *slot,
										   ItemPointer tupleid,
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
					  int epq_param,
					  List *returning_list)

{
	CustomScan *cscan = makeNode(CustomScan);

	/* Copy costs etc */
	cscan->scan.plan.startup_cost	= subplan->startup_cost;
	cscan->scan.plan.total_cost		= subplan->total_cost;
	cscan->scan.plan.plan_rows		= subplan->plan_rows;
	cscan->scan.plan.plan_width		= subplan->plan_width;

	/* Setup methods, child plan and param number for EPQ */
	cscan->methods = &partition_router_plan_methods;
	cscan->custom_plans = list_make1(subplan);
	cscan->custom_private = list_make1(makeInteger(epq_param));

	/* No physical relation will be scanned */
	cscan->scan.scanrelid = 0;

	/* Build an appropriate target list */
	cscan->scan.plan.targetlist = pfilter_build_tlist(subplan);

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
	state->epqparam = intVal(linitial(node->custom_private));
	state->subplan = (Plan *) linitial(node->custom_plans);

	return (Node *) state;
}

void
partition_router_begin(CustomScanState *node, EState *estate, int eflags)
{
	PartitionRouterState   *state = (PartitionRouterState *) node;

	/* Remember current relation we're going to delete from */
	state->current_rri = estate->es_result_relation_info;

	EvalPlanQualInit(&state->epqstate, estate,
					 state->subplan, NIL,
					 state->epqparam);

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

take_next_tuple:
	/* execute PartitionFilter child node */
	slot = ExecProcNode(child_ps);

	if (!TupIsNull(slot))
	{
		ResultRelInfo	   *current_rri = state->current_rri;
		char				relkind;
		ItemPointerData		ctid;

		ItemPointerSetInvalid(&ctid);

		/* Build new junkfilter if we have to */
		if (state->junkfilter == NULL)
		{
			state->junkfilter =
				ExecInitJunkFilter(state->subplan->targetlist,
								   current_rri->ri_RelationDesc->rd_att->tdhasoid,
								   ExecInitExtraTupleSlotCompat(estate));

			state->junkfilter->jf_junkAttNo =
				ExecFindJunkAttribute(state->junkfilter, "ctid");

			if (!AttributeNumberIsValid(state->junkfilter->jf_junkAttNo))
				elog(ERROR, "could not find junk ctid column");
		}

		/* Additional checks based on 'relkind' */
		relkind = current_rri->ri_RelationDesc->rd_rel->relkind;
		if (relkind == RELKIND_RELATION)
		{
			Datum	ctid_datum;
			bool	ctid_isnull;

			ctid_datum = ExecGetJunkAttribute(slot,
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

		/* Magic: replace parent's ResultRelInfo with ours */
		estate->es_result_relation_info = current_rri;

		/* Delete tuple from old partition */
		Assert(ItemPointerIsValid(&ctid));
		slot = router_delete_tuple(slot, &ctid, &state->epqstate, estate);

		/* We require a tuple */
		if (TupIsNull(slot))
			goto take_next_tuple;

		/* Tuple will be inserted by ModifyTable */
		return ExecFilterJunk(state->junkfilter, slot);
	}

	return NULL;
}

void
partition_router_end(CustomScanState *node)
{
	PartitionRouterState *state = (PartitionRouterState *) node;

	Assert(list_length(node->custom_ps) == 1);
	EvalPlanQualEnd(&state->epqstate);
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
 *		This is a modified copy of ExecDelete from executor/nodeModifyTable.c
 * ----------------------------------------------------------------
 */

static TupleTableSlot *
router_delete_tuple(TupleTableSlot *slot,
					ItemPointer tupleid,
					EPQState *epqstate,
					EState *estate)
{
	ResultRelInfo			*rri;
	Relation				 rel;
	HTSU_Result				 result;
	HeapUpdateFailureData	 hufd;

	EvalPlanQualSetSlot(epqstate, slot);

	/* Get information on the (current) result relation */
	rri = estate->es_result_relation_info;
	rel = rri->ri_RelationDesc;

	/* BEFORE ROW UPDATE triggers */
	if (rri->ri_TrigDesc &&
		rri->ri_TrigDesc->trig_update_before_row)
	{
		slot = ExecBRUpdateTriggers(estate, epqstate, rri, tupleid, NULL, slot);
		if (TupIsNull(slot))
			return NULL;
	}

	/* BEFORE ROW DELETE triggers */
	if (rri->ri_TrigDesc &&
		rri->ri_TrigDesc->trig_delete_before_row)
	{
		if (!ExecBRDeleteTriggers(estate, epqstate, rri, tupleid, NULL))
			return NULL;
	}

	if (tupleid != NULL)
	{
ldelete:
		/* Delete the tuple */
		result = heap_delete_compat(rel, tupleid,
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

				/* Already deleted by self; nothing to do */
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
										   rel,
										   rri->ri_RangeTableIndex,
										   LockTupleExclusive,
										   &hufd.ctid,
										   hufd.xmax);

					if (!TupIsNull(epqslot))
					{
						Assert(tupleid != NULL);
						*tupleid = hufd.ctid;
						slot = epqslot;
						goto ldelete;
					}
				}

				/* Tuple already deleted; nothing to do */
				return NULL;

			default:
				elog(ERROR, "unrecognized heap_delete status: %u", result);
		}
	}
	else elog(ERROR, "tupleid should be specified for deletion");

	/* AFTER ROW DELETE triggers */
	ExecARDeleteTriggersCompat(estate, rri, tupleid, NULL, NULL);

	return slot;
}
