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

#if PG_VERSION_NUM >= 120000
#include "access/table.h"
#include "access/tableam.h"
#endif
#include "access/xact.h"
#if PG_VERSION_NUM >= 120000
#include "access/heapam.h" /* direct heap_delete, no-no */
#endif
#include "access/htup_details.h"
#include "catalog/pg_class.h"
#include "commands/trigger.h"
#include "executor/nodeModifyTable.h"
#include "foreign/fdwapi.h"
#if PG_VERSION_NUM >= 120000
#include "nodes/makefuncs.h" /* make_ands_explicit */
#include "optimizer/optimizer.h"
#endif
#include "optimizer/clauses.h"
#include "storage/bufmgr.h"
#include "utils/guc.h"
#include "utils/rel.h"


#define MTDisableStmtTriggers(mt_state, pr_state) \
	do { \
		TriggerDesc *triggers = (mt_state)->resultRelInfo->ri_TrigDesc; \
		\
		if (triggers) \
		{ \
			(pr_state)->insert_stmt_triggers |= triggers->trig_insert_after_statement; \
			(pr_state)->update_stmt_triggers |= triggers->trig_update_after_statement; \
			triggers->trig_insert_after_statement = false; \
			triggers->trig_update_after_statement = false; \
		} \
	} while (0)

#define MTEnableStmtTriggers(mt_state, pr_state) \
	do { \
		TriggerDesc *triggers = (mt_state)->resultRelInfo->ri_TrigDesc; \
		\
		if (triggers) \
		{ \
			triggers->trig_insert_after_statement = (pr_state)->insert_stmt_triggers; \
			triggers->trig_update_after_statement = (pr_state)->update_stmt_triggers; \
		} \
	} while (0)



bool				pg_pathman_enable_partition_router = true;

CustomScanMethods	partition_router_plan_methods;
CustomExecMethods	partition_router_exec_methods;

static TupleTableSlot *router_set_slot(PartitionRouterState *state,
									   TupleTableSlot *slot,
									   CmdType operation);
static TupleTableSlot *router_get_slot(PartitionRouterState *state,
									   bool *should_process);

static void router_lazy_init_constraint(PartitionRouterState *state);

static ItemPointerData router_extract_ctid(PartitionRouterState *state,
										   TupleTableSlot *slot);

static TupleTableSlot *router_lock_or_delete_tuple(PartitionRouterState *state,
												   TupleTableSlot *slot,
												   ItemPointer tupleid,
												   bool *deleted);

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
make_partition_router(Plan *subplan, int epq_param)
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
	EState				   *estate = node->ss.ps.state;
	PartitionRouterState   *state = (PartitionRouterState *) node;
	TupleTableSlot		   *slot;
	bool					should_process;

take_next_tuple:
	/* Get next tuple for processing */
	slot = router_get_slot(state, &should_process);

	if (should_process)
	{
		CmdType				new_cmd;
		bool				deleted;
		ItemPointerData		ctid;

		ItemPointerSetInvalid(&ctid);

		/* Build new junkfilter if needed */
		if (state->junkfilter == NULL)
			state->junkfilter = state->current_rri->ri_junkFilter;

		/* Build recheck constraint state lazily */
		router_lazy_init_constraint(state);

		/* Extract item pointer from current tuple */
		ctid = router_extract_ctid(state, slot);

		/* Magic: replace parent's ResultRelInfo with ours */
		estate->es_result_relation_info = state->current_rri;

		/* Lock or delete tuple from old partition */
		Assert(ItemPointerIsValid(&ctid));
		slot = router_lock_or_delete_tuple(state, slot,
										   &ctid, &deleted);

		/* We require a tuple (previous one has vanished) */
		if (TupIsNull(slot))
			goto take_next_tuple;

		/* Should we use UPDATE or DELETE + INSERT? */
		new_cmd = deleted ? CMD_INSERT : CMD_UPDATE;

		/* Alter ModifyTable's state and return */
		return router_set_slot(state, slot, new_cmd);
	}

	return slot;
}

void
partition_router_end(CustomScanState *node)
{
	PartitionRouterState *state = (PartitionRouterState *) node;

	Assert(list_length(node->custom_ps) == 1);
	ExecEndNode((PlanState *) linitial(node->custom_ps));

	EvalPlanQualEnd(&state->epqstate);
}

void
partition_router_rescan(CustomScanState *node)
{
	elog(ERROR, "partition_router_rescan is not implemented");
}

void
partition_router_explain(CustomScanState *node,
						 List *ancestors,
						 ExplainState *es)
{
	/* Nothing to do here now */
}

/* Return tuple OR yield it and change ModifyTable's operation */
static TupleTableSlot *
router_set_slot(PartitionRouterState *state,
				TupleTableSlot *slot,
				CmdType operation)
{
	ModifyTableState   *mt_state = state->mt_state;

	/* Fast path for correct operation type */
	if (mt_state->operation == operation)
		return slot;

	/* HACK: alter ModifyTable's state */
	MTHackField(mt_state, mt_nplans) = -mt_state->mt_whichplan;
	MTHackField(mt_state, operation) = operation;

	/* HACK: disable AFTER STATEMENT triggers */
	MTDisableStmtTriggers(mt_state, state);

	if (!TupIsNull(slot))
	{
		/* We should've cached junk filter already */
		Assert(state->junkfilter);

		/* HACK: conditionally disable junk filter in result relation */
		state->current_rri->ri_junkFilter = (operation == CMD_UPDATE) ?
												state->junkfilter :
												NULL;

		/* Don't forget to set saved_slot! */
		state->yielded_slot = ExecInitExtraTupleSlotCompat(mt_state->ps.state,
														   slot->tts_tupleDescriptor,
														   &TTSOpsHeapTuple);
		ExecCopySlot(state->yielded_slot, slot);
	}

	/* Yield */
	state->yielded = true;
	return NULL;
}

/* Fetch next tuple (either fresh or yielded) */
static TupleTableSlot *
router_get_slot(PartitionRouterState *state,
				bool *should_process)
{
	TupleTableSlot *slot;

	/* Do we have a preserved slot? */
	if (state->yielded)
	{
		/* HACK: enable AFTER STATEMENT triggers */
		MTEnableStmtTriggers(state->mt_state, state);

		/* Reset saved slot */
		slot = state->yielded_slot;
		state->yielded_slot = NULL;
		state->yielded = false;

		/* We shouldn't process preserved slot... */
		*should_process = false;
	}
	else
	{
		/* Fetch next tuple */
		slot = ExecProcNode((PlanState *) linitial(state->css.custom_ps));

		/* Restore operation type for AFTER STATEMENT triggers */
		if (TupIsNull(slot))
			slot = router_set_slot(state, NULL, CMD_UPDATE);

		/* But we have to process non-empty slot */
		*should_process = !TupIsNull(slot);
	}

	return slot;
}

static void
router_lazy_init_constraint(PartitionRouterState *state)
{
	if (state->constraint == NULL)
	{
		Relation	rel = state->current_rri->ri_RelationDesc;
		Oid			relid = RelationGetRelid(rel);
		List	   *clauses = NIL;
		Expr	   *expr;

		while (OidIsValid(relid))
		{
			/* It's probably OK if expression is NULL */
			expr = get_partition_constraint_expr(relid, false);
			expr = expression_planner(expr);

			if (!expr)
				break;

			/* Add this constraint to set */
			clauses = lappend(clauses, expr);

			/* Consider parent's check constraint as well */
			relid = get_parent_of_partition(relid);
		}

		if (!clauses)
			elog(ERROR, "no recheck constraint for relid %d", relid);

		state->constraint = ExecInitExpr(make_ands_explicit(clauses), NULL);
	}
}

/* Extract ItemPointer from tuple using JunkFilter */
static ItemPointerData
router_extract_ctid(PartitionRouterState *state, TupleTableSlot *slot)
{
	Relation	rel = state->current_rri->ri_RelationDesc;
	char		relkind = RelationGetForm(rel)->relkind;

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
		return *(ItemPointer) DatumGetPointer(ctid_datum);
	}
	else if (relkind == RELKIND_FOREIGN_TABLE)
		elog(ERROR, UPDATE_NODE_NAME " does not support foreign tables");
	else
		elog(ERROR, UPDATE_NODE_NAME " cannot handle relkind %u", relkind);
}

/* This is a heavily modified copy of ExecDelete from nodeModifyTable.c */
static TupleTableSlot *
router_lock_or_delete_tuple(PartitionRouterState *state,
							TupleTableSlot *slot,
							ItemPointer tupleid,
							bool *deleted	/* return value #1 */)
{
	ResultRelInfo		   *rri;
	Relation				rel;

	EState				   *estate = state->css.ss.ps.state;
	ExprContext			   *econtext = GetPerTupleExprContext(estate);
	ExprState			   *constraint = state->constraint;

	/* Maintaining both >= 12 and earlier is quite horrible there, you know */
#if PG_VERSION_NUM >= 120000
	TM_FailureData	tmfd;
	TM_Result		result;
#else
	HeapUpdateFailureData	tmfd;
	HTSU_Result				result;
#endif

	EPQState			   *epqstate = &state->epqstate;

	LOCKMODE				lockmode;
	bool					try_delete;

	*deleted = false;

	EvalPlanQualSetSlot(epqstate, slot);

	/* Get information on the (current) result relation */
	rri = estate->es_result_relation_info;
	rel = rri->ri_RelationDesc;
	lockmode = ExecUpdateLockMode(estate, rri);

recheck:
	/* Does tuple still belong to current partition? */
	econtext->ecxt_scantuple = slot;
	try_delete = !ExecCheck(constraint, econtext);

	/* Lock or delete tuple */
	if (try_delete)
	{
		/* BEFORE ROW UPDATE triggers */
		if (rri->ri_TrigDesc &&
			rri->ri_TrigDesc->trig_update_before_row)
		{
#if PG_VERSION_NUM >= 120000
			if (!ExecBRUpdateTriggers(estate, epqstate, rri, tupleid, NULL, slot))
				return NULL;
#else
			slot = ExecBRUpdateTriggers(estate, epqstate, rri, tupleid, NULL, slot);
			if (TupIsNull(slot))
				return NULL;
#endif
		}

		/* BEFORE ROW DELETE triggers */
		if (rri->ri_TrigDesc &&
			rri->ri_TrigDesc->trig_delete_before_row)
		{
			if (!ExecBRDeleteTriggersCompat(estate, epqstate, rri, tupleid, NULL, NULL))
				return NULL;
		}

		/* Delete the tuple */
		result = heap_delete_compat(rel, tupleid,
									estate->es_output_cid,
									estate->es_crosscheck_snapshot,
									true /* wait for commit */, &tmfd,
									true /* changing partition */);
	}
	else
	{
		HeapTupleData	tuple;
		Buffer			buffer;

		tuple.t_self = *tupleid;
		/* xxx why we ever need this? */
		result = heap_lock_tuple(rel, &tuple,
								 estate->es_output_cid,
								 lockmode, LockWaitBlock,
								 false, &buffer, &tmfd);

		ReleaseBuffer(buffer);
	}

	/* Check lock/delete status */
	switch (result)
	{
#if PG_VERSION_NUM >= 120000
		case TM_SelfModified:
#else
		case HeapTupleSelfUpdated:
#endif
			if (tmfd.cmax != estate->es_output_cid)
				ereport(ERROR,
						(errcode(ERRCODE_TRIGGERED_DATA_CHANGE_VIOLATION),
						 errmsg("tuple to be updated was already modified by an operation triggered by the current command"),
						 errhint("Consider using an AFTER trigger instead of a BEFORE trigger to propagate changes to other rows.")));

			/* Already deleted by self; nothing to do */
			return NULL;

#if PG_VERSION_NUM >= 120000
		case TM_Ok:
#else
		case HeapTupleMayBeUpdated:
#endif
			break;

#if PG_VERSION_NUM >= 120000  /* TM_Deleted/TM_Updated */
		case TM_Updated:
			{
				/* not sure this stuff is correct at all */
				TupleTableSlot *inputslot;
				TupleTableSlot *epqslot;

				if (IsolationUsesXactSnapshot())
					ereport(ERROR,
							(errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),
							 errmsg("could not serialize access due to concurrent update")));

				/*
				 * Already know that we're going to need to do EPQ, so
				 * fetch tuple directly into the right slot.
				 */
				inputslot = EvalPlanQualSlot(epqstate, rel, rri->ri_RangeTableIndex);

				result = table_tuple_lock(rel, tupleid,
										  estate->es_snapshot,
										  inputslot, estate->es_output_cid,
										  LockTupleExclusive, LockWaitBlock,
										  TUPLE_LOCK_FLAG_FIND_LAST_VERSION,
										  &tmfd);

				switch (result)
				{
					case TM_Ok:
						Assert(tmfd.traversed);
						epqslot = EvalPlanQual(epqstate,
											   rel,
											   rri->ri_RangeTableIndex,
											   inputslot);
						if (TupIsNull(epqslot))
							/* Tuple not passing quals anymore, exiting... */
							return NULL;

						/* just copied from below, ha */
						*tupleid = tmfd.ctid;
						slot = epqslot;
						goto recheck;

					case TM_SelfModified:

						/*
						 * This can be reached when following an update
						 * chain from a tuple updated by another session,
						 * reaching a tuple that was already updated in
						 * this transaction. If previously updated by this
						 * command, ignore the delete, otherwise error
						 * out.
						 *
						 * See also TM_SelfModified response to
						 * table_tuple_delete() above.
						 */
						if (tmfd.cmax != estate->es_output_cid)
							ereport(ERROR,
									(errcode(ERRCODE_TRIGGERED_DATA_CHANGE_VIOLATION),
									 errmsg("tuple to be deleted was already modified by an operation triggered by the current command"),
									 errhint("Consider using an AFTER trigger instead of a BEFORE trigger to propagate changes to other rows.")));
						return NULL;

					case TM_Deleted:
						/* tuple already deleted; nothing to do */
						return NULL;

					default:

						/*
						 * TM_Invisible should be impossible because we're
						 * waiting for updated row versions, and would
						 * already have errored out if the first version
						 * is invisible.
						 *
						 * TM_Updated should be impossible, because we're
						 * locking the latest version via
						 * TUPLE_LOCK_FLAG_FIND_LAST_VERSION.
						 */
						elog(ERROR, "unexpected table_tuple_lock status: %u",
							 result);
						return NULL;
				}

				Assert(false);
				break;
			}


		case TM_Deleted:
			if (IsolationUsesXactSnapshot())
				ereport(ERROR,
						(errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),
						 errmsg("could not serialize access due to concurrent delete")));
			/* tuple already deleted; nothing to do */
			return NULL;

#else
		case HeapTupleUpdated:
			if (IsolationUsesXactSnapshot())
				ereport(ERROR,
						(errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),
						 errmsg("could not serialize access due to concurrent update")));
			if (ItemPointerIndicatesMovedPartitions(&tmfd.ctid))
				ereport(ERROR,
						(errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),
						 errmsg("tuple to be updated was already moved to another partition due to concurrent update")));

			if (!ItemPointerEquals(tupleid, &tmfd.ctid))
			{
				TupleTableSlot *epqslot;

				epqslot = EvalPlanQual(estate,
									   epqstate,
									   rel,
									   rri->ri_RangeTableIndex,
									   LockTupleExclusive,
									   &tmfd.ctid,
									   tmfd.xmax);

				if (!TupIsNull(epqslot))
				{
					Assert(tupleid != NULL);
					*tupleid = tmfd.ctid;
					slot = epqslot;
					goto recheck;
				}
			}

			/* Tuple already deleted; nothing to do */
			return NULL;
#endif  /* TM_Deleted/TM_Updated */

#if PG_VERSION_NUM >= 120000
		case TM_Invisible:
#else
		case HeapTupleInvisible:
#endif
			elog(ERROR, "attempted to lock invisible tuple");
			break;

		default:
			elog(ERROR, "unrecognized heap_delete status: %u", result);
			break;
	}

	/* Additional work for delete s*/
	if (try_delete)
	{
		/* AFTER ROW DELETE triggers */
		ExecARDeleteTriggersCompat(estate, rri, tupleid, NULL, NULL);
	}

	*deleted = try_delete;
	return slot;
}
