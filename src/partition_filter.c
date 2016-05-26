#include "partition_filter.h"
#include "utils/guc.h"
#include "nodes/nodeFuncs.h"


bool				pg_pathman_enable_partition_filter = true;

CustomScanMethods	partition_filter_plan_methods;
CustomExecMethods	partition_filter_exec_methods;


static List * pfilter_build_tlist(List *tlist);

void
init_partition_filter_static_data(void)
{
	partition_filter_plan_methods.CustomName 			= "PartitionFilter";
	partition_filter_plan_methods.CreateCustomScanState	= partition_filter_create_scan_state;

	partition_filter_exec_methods.CustomName			= "PartitionFilter";
	partition_filter_exec_methods.BeginCustomScan		= partition_filter_begin;
	partition_filter_exec_methods.ExecCustomScan		= partition_filter_exec;
	partition_filter_exec_methods.EndCustomScan			= partition_filter_end;
	partition_filter_exec_methods.ReScanCustomScan		= partition_filter_rescan;
	partition_filter_exec_methods.MarkPosCustomScan		= NULL;
	partition_filter_exec_methods.RestrPosCustomScan	= NULL;
	partition_filter_exec_methods.ExplainCustomScan		= partition_filter_explain;

	DefineCustomBoolVariable("pg_pathman.enable_partitionfilter",
							 "Enables the planner's use of PartitionFilter custom node.",
							 NULL,
							 &pg_pathman_enable_partition_filter,
							 true,
							 PGC_USERSET,
							 0,
							 NULL,
							 NULL,
							 NULL);
}

Plan *
make_partition_filter_plan(Plan *subplan, PartRelationInfo *prel)
{
	CustomScan *cscan = makeNode(CustomScan);

	cscan->scan.plan.startup_cost = subplan->startup_cost;
	cscan->scan.plan.total_cost = subplan->total_cost;
	cscan->scan.plan.plan_rows = subplan->plan_rows;
	cscan->scan.plan.plan_width = subplan->plan_width;

	cscan->methods = &partition_filter_plan_methods;
	cscan->custom_plans = list_make1(subplan);

	cscan->scan.plan.targetlist = pfilter_build_tlist(subplan->targetlist);

	/* No relation will be scanned */
	cscan->scan.scanrelid = 0;
	cscan->custom_scan_tlist = subplan->targetlist;

	/* Save partitioned table's Oid */
	cscan->custom_private = list_make1_int(prel->key.relid);

	return &cscan->scan.plan;
}

Node *
partition_filter_create_scan_state(CustomScan *node)
{
	PartitionFilterState   *state = palloc0(sizeof(PartitionFilterState));

	NodeSetTag(state, T_CustomScanState);

	state->css.flags = node->flags;
	state->css.methods = &partition_filter_exec_methods;

	/* Extract necessary variables */
	state->subplan = (Plan *) linitial(node->custom_plans);
	state->partitioned_table = linitial_int(node->custom_private);

	/* Prepare dummy Const node */
	NodeSetTag(&state->temp_const, T_Const);
	state->temp_const.location = -1;

	return (Node *) state;
}

void
partition_filter_begin(CustomScanState *node, EState *estate, int eflags)
{
	PartitionFilterState   *state = (PartitionFilterState *) node;

	node->custom_ps = list_make1(ExecInitNode(state->subplan, estate, eflags));
	state->prel = get_pathman_relation_info(state->partitioned_table, NULL);
	state->firstStart = true;
}

TupleTableSlot *
partition_filter_exec(CustomScanState *node)
{
#define CopyToTempConst(const_field, attr_field) \
	( state->temp_const.const_field = \
		slot->tts_tupleDescriptor->attrs[attnum - 1]->attr_field )

	PartitionFilterState   *state = (PartitionFilterState *) node;

	EState				   *estate = node->ss.ps.state;
	PlanState			   *child_ps = (PlanState *) linitial(node->custom_ps);
	TupleTableSlot		   *slot;

	if (state->firstStart)
		state->savedRelInfo = estate->es_result_relation_info;

	slot = ExecProcNode(child_ps);

	if (!TupIsNull(slot))
	{
		WalkerContext	wcxt;
		bool			isnull;
		AttrNumber		attnum = state->prel->attnum;
		Datum			value = slot_getattr(slot, attnum, &isnull);

		state->temp_const.constvalue = value;
		state->temp_const.constisnull = isnull;

		CopyToTempConst(consttype,   atttypid);
		CopyToTempConst(consttypmod, atttypmod);
		CopyToTempConst(constcollid, attcollation);
		CopyToTempConst(constlen,    attlen);
		CopyToTempConst(constbyval,  attbyval);

		wcxt.prel = state->prel;
		wcxt.econtext = NULL;
		wcxt.hasLeast = false;
		wcxt.hasGreatest = false;

		walk_expr_tree((Expr *) &state->temp_const, &wcxt);

		/* estate->es_result_relation_info = NULL; */

		return slot;
	}

	return NULL;
}

void
partition_filter_end(CustomScanState *node)
{
	Assert(list_length(node->custom_ps) == 1);

	ExecEndNode((PlanState *) linitial(node->custom_ps));
}

void
partition_filter_rescan(CustomScanState *node)
{
	Assert(list_length(node->custom_ps) == 1);

	ExecReScan((PlanState *) linitial(node->custom_ps));
}

void
partition_filter_explain(CustomScanState *node, List *ancestors, ExplainState *es)
{
	/* Nothing to do here now */
}

/*
 * Build partition filter's target list pointing to subplan tuple's elements
 */
static List *
pfilter_build_tlist(List *tlist)
{
	List	   *result_tlist = NIL;
	ListCell   *lc;
	int			i = 1;

	foreach (lc, tlist)
	{
		TargetEntry *tle = (TargetEntry *) lfirst(lc);

		Var *var = makeVar(INDEX_VAR,	/* point to subplan's elements */
						   i,			/* direct attribute mapping */
						   exprType((Node *) tle->expr),
						   exprTypmod((Node *) tle->expr),
						   exprCollation((Node *) tle->expr),
						   0);

		result_tlist = lappend(result_tlist,
							   makeTargetEntry((Expr *) var,
											   i,
											   NULL,
											   tle->resjunk));
		i++; /* next resno */
	}

	return result_tlist;
}

/* Add proxy PartitionFilter nodes to subplans of ModifyTable node */
void
add_partition_filters(List *rtable, ModifyTable *modify_table)
{
	ListCell *lc1,
			 *lc2;

	Assert(IsA(modify_table, ModifyTable));

	if (!pg_pathman_enable_partition_filter)
		return;

	forboth (lc1, modify_table->plans, lc2, modify_table->resultRelations)
	{
		Index				rindex = lfirst_int(lc2);
		PartRelationInfo   *prel = get_pathman_relation_info(getrelid(rindex, rtable),
															 NULL);
		if (prel)
			lfirst(lc1) = make_partition_filter_plan((Plan *) lfirst(lc1), prel);
	}
}
