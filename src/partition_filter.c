/* ------------------------------------------------------------------------
 *
 * partition_filter.c
 *		Select partition for INSERT operation
 *
 * Copyright (c) 2016, Postgres Professional
 *
 * ------------------------------------------------------------------------
 */

#include "init.h"
#include "nodes_common.h"
#include "partition_filter.h"
#include "utils.h"

#include "foreign/fdwapi.h"
#include "foreign/foreign.h"
#include "nodes/nodeFuncs.h"
#include "utils/guc.h"
#include "utils/memutils.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"


#define ALLOC_EXP	2


/*
 * We use this struct as an argument for fake
 * MemoryContextCallback pf_memcxt_callback()
 * in order to attach some additional info to
 * EState (estate->es_query_cxt is involved).
 */
typedef struct
{
	int		estate_alloc_result_rels;	/* number of allocated result rels */
	bool	estate_not_modified;		/* did we modify EState somehow? */
} estate_mod_data;

/*
 * Allow INSERTs into any FDW \ postgres_fdw \ no FDWs at all.
 */
typedef enum
{
	PF_FDW_INSERT_DISABLED = 0,		/* INSERTs into FDWs are prohibited */
	PF_FDW_INSERT_POSTGRES,			/* INSERTs into postgres_fdw are OK */
	PF_FDW_INSERT_ANY_FDW			/* INSERTs into any FDWs are OK */
} PF_insert_fdw_mode;

static const struct config_enum_entry pg_pathman_insert_into_fdw_options[] = {
	{ "disabled",	PF_FDW_INSERT_DISABLED,	false },
	{ "postgres",	PF_FDW_INSERT_POSTGRES,	false },
	{ "any_fdw",	PF_FDW_INSERT_ANY_FDW,	false },
	{ NULL,			0,						false }
};


bool				pg_pathman_enable_partition_filter = true;
int					pg_pathman_insert_into_fdw = PF_FDW_INSERT_POSTGRES;

CustomScanMethods	partition_filter_plan_methods;
CustomExecMethods	partition_filter_exec_methods;


static estate_mod_data * fetch_estate_mod_data(EState *estate);
static void partition_filter_visitor(Plan *plan, void *context);
static List * pfilter_build_tlist(List *tlist);
static Index append_rte_to_estate(EState *estate, RangeTblEntry *rte);
static int append_rri_to_estate(EState *estate, ResultRelInfo *rri);
static void prepare_rri_fdw_for_insert(EState *estate,
									   ResultRelInfoHolder *rri_holder,
									   void *arg);


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

	DefineCustomEnumVariable("pg_pathman.insert_into_fdw",
							 "Allow INSERTS into FDW partitions.",
							 NULL,
							 &pg_pathman_insert_into_fdw,
							 PF_FDW_INSERT_POSTGRES,
							 pg_pathman_insert_into_fdw_options,
							 PGC_SUSET,
							 0,
							 NULL,
							 NULL,
							 NULL);
}


/*
 * Add PartitionFilter nodes to the plan tree
 */
void
add_partition_filters(List *rtable, Plan *plan)
{
	if (pg_pathman_enable_partition_filter)
		plan_tree_walker(plan, partition_filter_visitor, rtable);
}


/*
 * Initialize ResultPartsStorage (hash table etc).
 */
void
init_result_parts_storage(ResultPartsStorage *parts_storage,
						  EState *estate,
						  bool speculative_inserts,
						  Size table_entry_size,
						  on_new_rri_holder on_new_rri_holder_cb,
						  void *on_new_rri_holder_cb_arg)
{
	HASHCTL *result_rels_table_config = &parts_storage->result_rels_table_config;

	memset(result_rels_table_config, 0, sizeof(HASHCTL));
	result_rels_table_config->keysize = sizeof(Oid);

	/* Use sizeof(ResultRelInfoHolder) if table_entry_size is 0 */
	if (table_entry_size == ResultPartsStorageStandard)
		result_rels_table_config->entrysize = sizeof(ResultRelInfoHolder);
	else
		result_rels_table_config->entrysize = table_entry_size;

	parts_storage->result_rels_table = hash_create("ResultRelInfo storage", 10,
												   result_rels_table_config,
												   HASH_ELEM | HASH_BLOBS);
	parts_storage->estate = estate;
	parts_storage->saved_rel_info = NULL;

	parts_storage->on_new_rri_holder_callback = on_new_rri_holder_cb;
	parts_storage->callback_arg = on_new_rri_holder_cb_arg;

	/* Currenly ResultPartsStorage is used only for INSERTs */
	parts_storage->command_type = CMD_INSERT;
	parts_storage->speculative_inserts = speculative_inserts;

	/* Partitions must remain locked till transaction's end */
	parts_storage->head_open_lock_mode = RowExclusiveLock;
	parts_storage->heap_close_lock_mode = NoLock;
}

/*
 * Free ResultPartsStorage (close relations etc).
 */
void
fini_result_parts_storage(ResultPartsStorage *parts_storage, bool close_rels)
{
	/* Close partitions and their indices if asked to */
	if (close_rels)
	{
		HASH_SEQ_STATUS			stat;
		ResultRelInfoHolder	   *rri_holder; /* ResultRelInfo holder */

		hash_seq_init(&stat, parts_storage->result_rels_table);
		while ((rri_holder = (ResultRelInfoHolder *) hash_seq_search(&stat)) != NULL)
		{
			ExecCloseIndices(rri_holder->result_rel_info);

			heap_close(rri_holder->result_rel_info->ri_RelationDesc,
					   parts_storage->heap_close_lock_mode);
		}
	}

	/* Finally destroy hash table */
	hash_destroy(parts_storage->result_rels_table);
}

/*
 * Find a ResultRelInfo for the partition using ResultPartsStorage.
 */
ResultRelInfoHolder *
scan_result_parts_storage(Oid partid, ResultPartsStorage *parts_storage)
{
#define CopyToResultRelInfo(field_name) \
	( part_result_rel_info->field_name = parts_storage->saved_rel_info->field_name )

	ResultRelInfoHolder	   *rri_holder;
	bool					found;

	rri_holder = hash_search(parts_storage->result_rels_table,
							 (const void *) &partid,
							 HASH_ENTER, &found);

	/* If not found, create & cache new ResultRelInfo */
	if (!found)
	{
		Relation		child_rel;
		RangeTblEntry  *child_rte,
					   *parent_rte;
		Index			child_rte_idx;
		ResultRelInfo  *part_result_rel_info;

		/* Lock partition and check if it exists */
		LockRelationOid(partid, parts_storage->head_open_lock_mode);
		if(!SearchSysCacheExists1(RELOID, ObjectIdGetDatum(partid)))
		{
			UnlockRelationOid(partid, parts_storage->head_open_lock_mode);
			return NULL;
		}

		parent_rte = rt_fetch(parts_storage->saved_rel_info->ri_RangeTableIndex,
							  parts_storage->estate->es_range_table);

		/* Open relation and check if it is a valid target */
		child_rel = heap_open(partid, NoLock);
		CheckValidResultRel(child_rel, parts_storage->command_type);

		/* Create RangeTblEntry for partition */
		child_rte = makeNode(RangeTblEntry);

		child_rte->rtekind = RTE_RELATION;
		child_rte->relid = partid;
		child_rte->relkind = child_rel->rd_rel->relkind;
		child_rte->eref = parent_rte->eref;
		child_rte->requiredPerms = parent_rte->requiredPerms;
		child_rte->checkAsUser = parent_rte->checkAsUser;
		child_rte->insertedCols = parent_rte->insertedCols;

		/* Check permissions for partition */
		ExecCheckRTPerms(list_make1(child_rte), true);

		/* Append RangeTblEntry to estate->es_range_table */
		child_rte_idx = append_rte_to_estate(parts_storage->estate, child_rte);

		/* Create ResultRelInfo for partition */
		part_result_rel_info = makeNode(ResultRelInfo);

		/* Check that 'saved_rel_info' is set */
		if (!parts_storage->saved_rel_info)
			elog(ERROR, "ResultPartsStorage contains no saved_rel_info");

		InitResultRelInfo(part_result_rel_info,
						  child_rel,
						  child_rte_idx,
						  parts_storage->estate->es_instrument);

		if (parts_storage->command_type != CMD_DELETE)
			ExecOpenIndices(part_result_rel_info, parts_storage->speculative_inserts);

		/* Copy necessary fields from saved ResultRelInfo */
		CopyToResultRelInfo(ri_WithCheckOptions);
		CopyToResultRelInfo(ri_WithCheckOptionExprs);
		CopyToResultRelInfo(ri_junkFilter);
		CopyToResultRelInfo(ri_projectReturning);
		CopyToResultRelInfo(ri_onConflictSetProj);
		CopyToResultRelInfo(ri_onConflictSetWhere);

		/* ri_ConstraintExprs will be initialized by ExecRelCheck() */
		part_result_rel_info->ri_ConstraintExprs = NULL;

		/* Finally fill the ResultRelInfo holder */
		rri_holder->partid = partid;
		rri_holder->result_rel_info = part_result_rel_info;

		/* Call on_new_rri_holder_callback() if needed */
		if (parts_storage->on_new_rri_holder_callback)
			parts_storage->on_new_rri_holder_callback(parts_storage->estate,
													  rri_holder,
													  parts_storage->callback_arg);

		/* Append ResultRelInfo to storage->es_alloc_result_rels */
		append_rri_to_estate(parts_storage->estate, part_result_rel_info);
	}

	return rri_holder;
}

/*
 * Find matching partitions for 'value' using PartRelationInfo.
 */
Oid *
find_partitions_for_value(Datum value, const PartRelationInfo *prel,
						  ExprContext *econtext, int *nparts)
{
#define CopyToTempConst(const_field, attr_field) \
	( temp_const.const_field = prel->attr_field )

	Const			temp_const;	/* temporary const for expr walker */
	WalkerContext	wcxt;
	List		   *ranges = NIL;

	/* Prepare dummy Const node */
	NodeSetTag(&temp_const, T_Const);
	temp_const.location = -1;

	/* Fill const with value ... */
	temp_const.constvalue = value;
	temp_const.constisnull = false;

	/* ... and some other important data */
	CopyToTempConst(consttype,   atttype);
	CopyToTempConst(consttypmod, atttypmod);
	CopyToTempConst(constcollid, attcollid);
	CopyToTempConst(constlen,    attlen);
	CopyToTempConst(constbyval,  attbyval);

	InitWalkerContext(&wcxt, prel, econtext, true);
	ranges = walk_expr_tree((Expr *) &temp_const, &wcxt)->rangeset;
	return get_partition_oids(ranges, nparts, prel, false);
}


Plan *
make_partition_filter(Plan *subplan, Oid partitioned_table,
					  OnConflictAction conflict_action)
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

	/* Pack partitioned table's Oid and conflict_action */
	cscan->custom_private = list_make2_int(partitioned_table, conflict_action);

	return &cscan->scan.plan;
}

Node *
partition_filter_create_scan_state(CustomScan *node)
{
	PartitionFilterState   *state;

	state = (PartitionFilterState *) palloc0(sizeof(PartitionFilterState));
	NodeSetTag(state, T_CustomScanState);

	state->css.flags = node->flags;
	state->css.methods = &partition_filter_exec_methods;

	/* Extract necessary variables */
	state->subplan = (Plan *) linitial(node->custom_plans);
	state->partitioned_table = linitial_int(node->custom_private);
	state->on_conflict_action = lsecond_int(node->custom_private);

	/* Check boundaries */
	Assert(state->on_conflict_action >= ONCONFLICT_NONE ||
		   state->on_conflict_action <= ONCONFLICT_UPDATE);

	/* There should be exactly one subplan */
	Assert(list_length(node->custom_plans) == 1);

	return (Node *) state;
}

void
partition_filter_begin(CustomScanState *node, EState *estate, int eflags)
{
	PartitionFilterState   *state = (PartitionFilterState *) node;

	/* It's convenient to store PlanState in 'custom_ps' */
	node->custom_ps = list_make1(ExecInitNode(state->subplan, estate, eflags));

	/* Init ResultRelInfo cache */
	init_result_parts_storage(&state->result_parts, estate,
							  state->on_conflict_action != ONCONFLICT_NONE,
							  ResultPartsStorageStandard, prepare_rri_fdw_for_insert, NULL);

	state->warning_triggered = false;
}

TupleTableSlot *
partition_filter_exec(CustomScanState *node)
{
	PartitionFilterState   *state = (PartitionFilterState *) node;

	ExprContext			   *econtext = node->ss.ps.ps_ExprContext;
	EState				   *estate = node->ss.ps.state;
	PlanState			   *child_ps = (PlanState *) linitial(node->custom_ps);
	TupleTableSlot		   *slot;

	slot = ExecProcNode(child_ps);

	/* Save original ResultRelInfo */
	if (!state->result_parts.saved_rel_info)
		state->result_parts.saved_rel_info = estate->es_result_relation_info;

	if (!TupIsNull(slot))
	{
		MemoryContext			old_cxt;
		const PartRelationInfo *prel;
		ResultRelInfoHolder	   *rri_holder;
		bool					isnull;
		Datum					value;

		/* Fetch PartRelationInfo for this partitioned relation */
		prel = get_pathman_relation_info(state->partitioned_table);
		if (!prel)
		{
			if (!state->warning_triggered)
				elog(WARNING, "Relation \"%s\" is not partitioned, "
							  "PartitionFilter will behave as a normal INSERT",
					 get_rel_name_or_relid(state->partitioned_table));

			return slot;
		}

		/* Extract partitioned column's value (also check types) */
		Assert(slot->tts_tupleDescriptor->
					attrs[prel->attnum - 1]->atttypid == prel->atttype);
		value = slot_getattr(slot, prel->attnum, &isnull);
		if (isnull)
			elog(ERROR, ERR_PART_ATTR_NULL);

		/* Switch to per-tuple context */
		old_cxt = MemoryContextSwitchTo(GetPerTupleMemoryContext(estate));

		/* Search for a matching partition */
		rri_holder = select_partition_for_insert(prel,
												 &state->result_parts,
												 value, estate, true);
		estate->es_result_relation_info = rri_holder->result_rel_info;

		/* Switch back and clean up per-tuple context */
		MemoryContextSwitchTo(old_cxt);
		ResetExprContext(econtext);

		return slot;
	}

	return NULL;
}

void
partition_filter_end(CustomScanState *node)
{
	PartitionFilterState   *state = (PartitionFilterState *) node;

	/* Executor will close rels via estate->es_result_relations */
	fini_result_parts_storage(&state->result_parts, false);

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
 * Smart wrapper for scan_result_parts_storage().
 */
ResultRelInfoHolder *
select_partition_for_insert(const PartRelationInfo *prel,
							ResultPartsStorage *parts_storage,
							Datum value, EState *estate,
							bool spawn_partitions)
{
	MemoryContext			old_cxt;
	ExprContext			   *econtext;
	ResultRelInfoHolder	   *rri_holder;
	Oid						selected_partid = InvalidOid;
	Oid					   *parts;
	int						nparts;

	econtext = GetPerTupleExprContext(estate);

	/* Search for matching partitions */
	parts = find_partitions_for_value(value, prel, econtext, &nparts);

	if (nparts > 1)
		elog(ERROR, ERR_PART_ATTR_MULTIPLE);
	else if (nparts == 0)
	{
		/*
		 * If auto partition propagation is enabled then try to create
		 * new partitions for the key
		 */
		if (prel->auto_partition && IsAutoPartitionEnabled() && spawn_partitions)
		{
			selected_partid = create_partitions(PrelParentRelid(prel),
												value, prel->atttype);

			/* get_pathman_relation_info() will refresh this entry */
			invalidate_pathman_relation_info(PrelParentRelid(prel), NULL);
		}
		else
			elog(ERROR, ERR_PART_ATTR_NO_PART,
				 datum_to_cstring(value, prel->atttype));
	}
	else selected_partid = parts[0];

	/* Replace parent table with a suitable partition */
	old_cxt = MemoryContextSwitchTo(estate->es_query_cxt);
	rri_holder = scan_result_parts_storage(selected_partid, parts_storage);
	MemoryContextSwitchTo(old_cxt);

	/* Could not find suitable partition */
	if (rri_holder == NULL)
		elog(ERROR, ERR_PART_ATTR_NO_PART,
			 datum_to_cstring(value, prel->atttype));

	return rri_holder;
}

/*
 * Callback to be executed on FDW partitions.
 */
static void
prepare_rri_fdw_for_insert(EState *estate,
						   ResultRelInfoHolder *rri_holder,
						   void *arg)
{
	ResultRelInfo  *rri = rri_holder->result_rel_info;
	FdwRoutine	   *fdw_routine = rri->ri_FdwRoutine;
	Oid				partid;

	/* Nothing to do if not FDW */
	if (fdw_routine == NULL)
		return;

	partid = RelationGetRelid(rri->ri_RelationDesc);

	/* Perform some checks according to 'pg_pathman_insert_into_fdw' */
	switch (pg_pathman_insert_into_fdw)
	{
		case PF_FDW_INSERT_DISABLED:
			elog(ERROR, "INSERTs into FDW partitions are disabled");
			break;

		case PF_FDW_INSERT_POSTGRES:
			{
				ForeignDataWrapper *fdw;
				ForeignServer	   *fserver;

				/* Check if it's PostgreSQL FDW */
				fserver = GetForeignServer(GetForeignTable(partid)->serverid);
				fdw = GetForeignDataWrapper(fserver->fdwid);
				if (strcmp("postgres_fdw", fdw->fdwname) != 0)
					elog(ERROR, "FDWs other than postgres_fdw are restricted");
			}
			break;

		case PF_FDW_INSERT_ANY_FDW:
			{
				ForeignDataWrapper *fdw;
				ForeignServer	   *fserver;

				fserver = GetForeignServer(GetForeignTable(partid)->serverid);
				fdw = GetForeignDataWrapper(fserver->fdwid);
				if (strcmp("postgres_fdw", fdw->fdwname) != 0)
					elog(WARNING, "unrestricted FDW mode may lead to \"%s\" crashes",
						 fdw->fdwname);
			}
			break; /* do nothing */

		default:
			elog(ERROR, "Mode is not implemented yet");
			break;
	}

	if (fdw_routine->PlanForeignModify)
	{
		RangeTblEntry	   *rte;
		ModifyTableState	mtstate;
		List			   *fdw_private;
		Query				query;
		PlannedStmt		   *plan;
		TupleDesc			tupdesc;
		int					i,
							target_attr;

		/* Fetch RangeTblEntry for partition */
		rte = rt_fetch(rri->ri_RangeTableIndex, estate->es_range_table);

		/* Fetch tuple descriptor */
		tupdesc = RelationGetDescr(rri->ri_RelationDesc);

		/* Create fake Query node */
		memset((void *) &query, 0, sizeof(Query));
		NodeSetTag(&query, T_Query);

		query.commandType		= CMD_INSERT;
		query.querySource		= QSRC_ORIGINAL;
		query.resultRelation	= 1;
		query.rtable			= list_make1(copyObject(rte));
		query.jointree			= makeNode(FromExpr);

		query.targetList		= NIL;
		query.returningList		= NIL;

		/* Generate 'query.targetList' using 'tupdesc' */
		target_attr = 1;
		for (i = 0; i < tupdesc->natts; i++)
		{
			Form_pg_attribute	attr;
			TargetEntry		   *te;
			Param			   *param;

			attr = tupdesc->attrs[i];

			if (attr->attisdropped)
				continue;

			param = makeNode(Param);
			param->paramkind	= PARAM_EXTERN;
			param->paramid		= target_attr;
			param->paramtype	= attr->atttypid;
			param->paramtypmod	= attr->atttypmod;
			param->paramcollid	= attr->attcollation;
			param->location		= -1;

			te = makeTargetEntry((Expr *) param, target_attr,
								 pstrdup(NameStr(attr->attname)),
								 false);

			query.targetList = lappend(query.targetList, te);

			target_attr++;
		}

		/* Create fake ModifyTableState */
		memset((void *) &mtstate, 0, sizeof(ModifyTableState));
		NodeSetTag(&mtstate, T_ModifyTableState);
		mtstate.ps.state = estate;
		mtstate.operation = CMD_INSERT;
		mtstate.resultRelInfo = rri;
		mtstate.mt_onconflict = ONCONFLICT_NONE;

		/* Plan fake query in for FDW access to be planned as well */
		elog(DEBUG1, "FDW(%u): plan fake query for fdw_private", partid);
		plan = standard_planner(&query, 0, NULL);

		/* Extract fdw_private from useless plan */
		elog(DEBUG1, "FDW(%u): extract fdw_private", partid);
		fdw_private = (List *)
				linitial(((ModifyTable *) plan->planTree)->fdwPrivLists);

		/* call BeginForeignModify on 'rri' */
		elog(DEBUG1, "FDW(%u): call BeginForeignModify on a fake INSERT node", partid);
		fdw_routine->BeginForeignModify(&mtstate, rri, fdw_private, 0, 0);

		/* Report success */
		elog(DEBUG1, "FDW(%u): success", partid);
	}
}

/*
 * Used by fetch_estate_mod_data() to find estate_mod_data.
 */
static void
pf_memcxt_callback(void *arg) { elog(DEBUG1, "EState is destroyed"); }

/*
 * Fetch (or create) a estate_mod_data structure we've hidden inside es_query_cxt.
 */
static estate_mod_data *
fetch_estate_mod_data(EState *estate)
{
	MemoryContext			estate_mcxt = estate->es_query_cxt;
	estate_mod_data		   *emd_struct;
	MemoryContextCallback  *cb = estate_mcxt->reset_cbs;

	/* Go through callback list */
	while (cb != NULL)
	{
		/* This is the dummy callback we're looking for! */
		if (cb->func == pf_memcxt_callback)
			return (estate_mod_data *) cb->arg;

		cb = estate_mcxt->reset_cbs->next;
	}

	/* Have to create a new one */
	emd_struct = MemoryContextAlloc(estate_mcxt, sizeof(estate_mod_data));
	emd_struct->estate_not_modified = true;
	emd_struct->estate_alloc_result_rels = estate->es_num_result_relations;

	cb = MemoryContextAlloc(estate_mcxt, sizeof(MemoryContextCallback));
	cb->func = pf_memcxt_callback;
	cb->arg = emd_struct;

	MemoryContextRegisterResetCallback(estate_mcxt, cb);

	return emd_struct;
}

/*
 * Append RangeTblEntry 'rte' to estate->es_range_table.
 */
static Index
append_rte_to_estate(EState *estate, RangeTblEntry *rte)
{
	estate_mod_data	   *emd_struct = fetch_estate_mod_data(estate);

	/* Copy estate->es_range_table if it's first time expansion */
	if (emd_struct->estate_not_modified)
		estate->es_range_table = list_copy(estate->es_range_table);

	estate->es_range_table = lappend(estate->es_range_table, rte);

	/* Update estate_mod_data */
	emd_struct->estate_not_modified = false;

	return list_length(estate->es_range_table);
}

/*
 * Append ResultRelInfo 'rri' to estate->es_result_relations.
 */
static int
append_rri_to_estate(EState *estate, ResultRelInfo *rri)
{
	estate_mod_data	   *emd_struct = fetch_estate_mod_data(estate);
	int					result_rels_allocated = emd_struct->estate_alloc_result_rels;

	/* Reallocate estate->es_result_relations if needed */
	if (result_rels_allocated <= estate->es_num_result_relations)
	{
		ResultRelInfo *rri_array = estate->es_result_relations;

		result_rels_allocated = result_rels_allocated * ALLOC_EXP + 1;
		estate->es_result_relations = palloc(result_rels_allocated *
												sizeof(ResultRelInfo));
		memcpy(estate->es_result_relations,
			   rri_array,
			   estate->es_num_result_relations * sizeof(ResultRelInfo));
	}

	/*
	 * Append ResultRelInfo to 'es_result_relations' array.
	 * NOTE: this is probably safe since ResultRelInfo
	 * contains nothing but pointers to various structs.
	 */
	estate->es_result_relations[estate->es_num_result_relations] = *rri;

	/* Update estate_mod_data */
	emd_struct->estate_alloc_result_rels = result_rels_allocated;
	emd_struct->estate_not_modified = false;

	return estate->es_num_result_relations++;
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

/*
 * Add partition filters to ModifyTable node's children.
 *
 * 'context' should point to the PlannedStmt->rtable.
 */
static void
partition_filter_visitor(Plan *plan, void *context)
{
	List		   *rtable = (List *) context;
	ModifyTable	   *modify_table = (ModifyTable *) plan;
	ListCell	   *lc1,
				   *lc2;

	/* Skip if not ModifyTable with 'INSERT' command */
	if (!IsA(modify_table, ModifyTable) || modify_table->operation != CMD_INSERT)
		return;

	Assert(rtable && IsA(rtable, List));

	forboth (lc1, modify_table->plans, lc2, modify_table->resultRelations)
	{
		Index					rindex = lfirst_int(lc2);
		Oid						relid = getrelid(rindex, rtable);
		const PartRelationInfo *prel = get_pathman_relation_info(relid);

		/* Check that table is partitioned */
		if (prel)
			lfirst(lc1) = make_partition_filter((Plan *) lfirst(lc1),
												relid,
												modify_table->onConflictAction);
	}
}
