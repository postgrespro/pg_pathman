/*-------------------------------------------------------------------------
 *
 * partition_creation.c
 *		Various functions for partition creation.
 *
 * Copyright (c) 2016, Postgres Professional
 *
 *-------------------------------------------------------------------------
 */

#include "init.h"
#include "partition_creation.h"
#include "partition_filter.h"
#include "pathman.h"
#include "pathman_workers.h"
#include "xact_handling.h"

#include "access/htup_details.h"
#include "access/reloptions.h"
#include "access/xact.h"
#include "catalog/heap.h"
#include "catalog/pg_type.h"
#include "catalog/toasting.h"
#include "commands/event_trigger.h"
#include "commands/sequence.h"
#include "commands/tablecmds.h"
#include "commands/tablespace.h"
#include "miscadmin.h"
#include "parser/parse_func.h"
#include "parser/parse_relation.h"
#include "parser/parse_utilcmd.h"
#include "tcop/utility.h"
#include "utils/builtins.h"
#include "utils/datum.h"
#include "utils/jsonb.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"


static Datum extract_binary_interval_from_text(Datum interval_text,
											   Oid part_atttype,
											   Oid *interval_type);

static void extract_op_func_and_ret_type(char *opname, Oid type1, Oid type2,
										 Oid *move_bound_op_func,
										 Oid *move_bound_op_ret_type);

static Oid spawn_partitions_val(Oid parent_relid,
								Datum range_bound_min,
								Datum range_bound_max,
								Oid range_bound_type,
								Datum interval_binary,
								Oid interval_type,
								Datum value,
								Oid value_type);

static Oid create_single_partition_internal(Oid parent_relid,
											RangeVar *partition_rv,
											char *tablespace,
											char **partitioned_column);

static char *choose_partition_name(Oid parent_relid, Oid parent_nsp);

static ObjectAddress create_table_using_stmt(CreateStmt *create_stmt,
											 Oid relowner);

static void copy_foreign_keys(Oid parent_relid, Oid partition_oid);


/*
 * ---------------------------------------
 *  Public interface (partition creation)
 * ---------------------------------------
 */

/* Create one RANGE partition [start_value, end_value) */
Oid
create_single_range_partition_internal(Oid parent_relid,
									   const Bound *start_value,
									   const Bound *end_value,
									   Oid value_type,
									   RangeVar *partition_rv,
									   char *tablespace)
{
	Oid						partition_relid;
	Relation				child_relation;
	Constraint			   *check_constr;
	char				   *partitioned_column;
	init_callback_params	callback_params;

	/* Create a partition & get 'partitioned_column' */
	partition_relid = create_single_partition_internal(parent_relid,
													   partition_rv,
													   tablespace,
													   &partitioned_column);

	/* Build check constraint for RANGE partition */
	check_constr = build_range_check_constraint(partition_relid,
												partitioned_column,
												start_value,
												end_value,
												value_type);

	/* Open the relation and add new check constraint & fkeys */
	child_relation = heap_open(partition_relid, AccessExclusiveLock);
	AddRelationNewConstraints(child_relation, NIL,
							  list_make1(check_constr),
							  false, true, true);
	heap_close(child_relation, NoLock);

	CommandCounterIncrement();

	/* Finally invoke 'init_callback' */
	MakeInitCallbackRangeParams(&callback_params, InvalidOid,
								parent_relid, partition_relid,
								*start_value, *end_value, value_type);
	invoke_part_callback(&callback_params);

	CommandCounterIncrement();

	/* Return the Oid */
	return partition_relid;
}

/*
 * Create RANGE partitions (if needed) using either BGW or current backend.
 *
 * Returns Oid of the partition to store 'value'.
 */
Oid
create_partitions_for_value(Oid relid, Datum value, Oid value_type)
{
	TransactionId	rel_xmin;
	Oid				last_partition = InvalidOid;

	/* Check that table is partitioned and fetch xmin */
	if (pathman_config_contains_relation(relid, NULL, NULL, &rel_xmin))
	{
		bool part_in_prev_xact =
					TransactionIdPrecedes(rel_xmin, GetCurrentTransactionId()) ||
					TransactionIdEquals(rel_xmin, FrozenTransactionId);

		/*
		 * If table has been partitioned in some previous xact AND
		 * we don't hold any conflicting locks, run BGWorker.
		 */
		if (part_in_prev_xact && !xact_bgw_conflicting_lock_exists(relid))
		{
			elog(DEBUG2, "create_partitions(): chose BGWorker [%u]", MyProcPid);
			last_partition = create_partitions_for_value_bg_worker(relid,
																   value,
																   value_type);
		}
		/* Else it'd be better for the current backend to create partitions */
		else
		{
			elog(DEBUG2, "create_partitions(): chose backend [%u]", MyProcPid);
			last_partition = create_partitions_for_value_internal(relid,
																  value,
																  value_type);
		}
	}
	else
		elog(ERROR, "relation \"%s\" is not partitioned by pg_pathman",
			 get_rel_name_or_relid(relid));

	/* Check that 'last_partition' is valid */
	if (last_partition == InvalidOid)
		elog(ERROR, "could not create new partitions for relation \"%s\"",
			 get_rel_name_or_relid(relid));

	return last_partition;
}


/*
 * --------------------
 *  Partition creation
 * --------------------
 */

/*
 * Create partitions (if needed) and return Oid of the partition to store 'value'.
 *
 * NB: This function should not be called directly,
 * use create_partitions_for_value() instead.
 */
Oid
create_partitions_for_value_internal(Oid relid, Datum value, Oid value_type)
{
	MemoryContext	old_mcxt = CurrentMemoryContext;
	Oid				partid = InvalidOid; /* last created partition (or InvalidOid) */

	PG_TRY();
	{
		const PartRelationInfo *prel;
		LockAcquireResult		lock_result; /* could we lock the parent? */
		Datum					values[Natts_pathman_config];
		bool					isnull[Natts_pathman_config];

		/* Get both PartRelationInfo & PATHMAN_CONFIG contents for this relation */
		if (pathman_config_contains_relation(relid, values, isnull, NULL))
		{
			Oid			base_bound_type;		/* base type of prel->atttype */
			Oid			base_value_type;	/* base type of value_type */

			/* Fetch PartRelationInfo by 'relid' */
			prel = get_pathman_relation_info_after_lock(relid, true, &lock_result);
			shout_if_prel_is_invalid(relid, prel, PT_RANGE);

			/* Fetch base types of prel->atttype & value_type */
			base_bound_type = getBaseType(prel->atttype);
			base_value_type = getBaseType(value_type);

			/* Search for a suitable partition if we didn't hold it */
			Assert(lock_result != LOCKACQUIRE_NOT_AVAIL);
			if (lock_result == LOCKACQUIRE_OK)
			{
				Oid	   *parts;
				int		nparts;

				/* Search for matching partitions */
				parts = find_partitions_for_value(value, value_type, prel, &nparts);

				/* Shout if there's more than one */
				if (nparts > 1)
					elog(ERROR, ERR_PART_ATTR_MULTIPLE);

				/* It seems that we got a partition! */
				else if (nparts == 1)
				{
					/* Unlock the parent (we're not going to spawn) */
					xact_unlock_partitioned_rel(relid);

					/* Simply return the suitable partition */
					partid = parts[0];
				}

				/* Don't forget to free */
				pfree(parts);
			}

			/* Else spawn a new one (we hold a lock on the parent) */
			if (partid == InvalidOid)
			{
				RangeEntry *ranges = PrelGetRangesArray(prel);
				Datum		bound_min,			/* absolute MIN */
							bound_max;			/* absolute MAX */
				// Infinitable	bound_min,		/* lower bound of all partitions */
				// 			bound_max;		/* upper bound of all partitions */
				// Infinitable	start,
				// 			end; 

				Oid			interval_type = InvalidOid;
				Datum		interval_binary, /* assigned 'width' of one partition */
							interval_text;

				// bound_min = ranges[0].min;
				// bound_max = ranges[PrelLastChild(prel)].max;

				// start.value = !IsInfinite(&bound_min) ?
				// 	datumCopy(InfinitableGetValue(&bound_min),
				// 			  prel->attbyval,
				// 			  prel->attlen) :
				// 	(Datum) 0;
				// start.is_infinite = IsInfinite(&bound_min);

				// end.value = !IsInfinite(&bound_max) ?
				// 	datumCopy(InfinitableGetValue(&bound_max),
				// 			  prel->attbyval,
				// 			  prel->attlen) :
				// 	(Datum) 0;
				// end.is_infinite = IsInfinite(&bound_max);

				/* Read max & min range values from PartRelationInfo */
				/* TODO */
				// bound_min = PrelGetRangesArray(prel)[0].min;
				// bound_max = PrelGetRangesArray(prel)[PrelLastChild(prel)].max;
				bound_min = BoundGetValue(&ranges[0].min);
				bound_max = BoundGetValue(&ranges[PrelLastChild(prel)].max);

				/* Copy datums on order to protect them from cache invalidation */
				bound_min = datumCopy(bound_min, prel->attbyval, prel->attlen);
				bound_max = datumCopy(bound_max, prel->attbyval, prel->attlen);

				/* Retrieve interval as TEXT from tuple */
				interval_text = values[Anum_pathman_config_range_interval - 1];

				/* Convert interval to binary representation */
				interval_binary = extract_binary_interval_from_text(interval_text,
																	base_bound_type,
																	&interval_type);

				/* At last, spawn partitions to store the value */
				partid = spawn_partitions_val(PrelParentRelid(prel),
											  bound_min, bound_max, base_bound_type,
											  interval_binary, interval_type,
											  value, base_value_type);
			}
		}
		else
			elog(ERROR, "pg_pathman's config does not contain relation \"%s\"",
				 get_rel_name_or_relid(relid));
	}
	PG_CATCH();
	{
		ErrorData *edata;

		/* Switch to the original context & copy edata */
		MemoryContextSwitchTo(old_mcxt);
		edata = CopyErrorData();
		FlushErrorState();

		elog(LOG, "create_partitions_internal(): %s [%u]",
			 edata->message, MyProcPid);

		FreeErrorData(edata);

		/* Reset 'partid' in case of error */
		partid = InvalidOid;
	}
	PG_END_TRY();

	return partid;
}

/*
 * Convert interval from TEXT to binary form using partitioned column's type.
 */
static Datum
extract_binary_interval_from_text(Datum interval_text,	/* interval as TEXT */
								  Oid part_atttype,		/* partitioned column's type */
								  Oid *interval_type)	/* returned value */
{
	Datum		interval_binary;
	const char *interval_cstring;

	interval_cstring = TextDatumGetCString(interval_text);

	/* If 'part_atttype' is a *date type*, cast 'range_interval' to INTERVAL */
	if (is_date_type_internal(part_atttype))
	{
		int32	interval_typmod = PATHMAN_CONFIG_interval_typmod;

		/* Convert interval from CSTRING to internal form */
		interval_binary = DirectFunctionCall3(interval_in,
											  CStringGetDatum(interval_cstring),
											  ObjectIdGetDatum(InvalidOid),
											  Int32GetDatum(interval_typmod));
		if (interval_type)
			*interval_type = INTERVALOID;
	}
	/* Otherwise cast it to the partitioned column's type */
	else
	{
		HeapTuple	htup;
		Oid			typein_proc = InvalidOid;

		htup = SearchSysCache1(TYPEOID, ObjectIdGetDatum(part_atttype));
		if (HeapTupleIsValid(htup))
		{
			typein_proc = ((Form_pg_type) GETSTRUCT(htup))->typinput;
			ReleaseSysCache(htup);
		}
		else
			elog(ERROR, "Cannot find input function for type %u", part_atttype);

		/*
		 * Convert interval from CSTRING to 'prel->atttype'.
		 *
		 * Note: We pass 3 arguments in case
		 * 'typein_proc' also takes Oid & typmod.
		 */
		interval_binary = OidFunctionCall3(typein_proc,
										   CStringGetDatum(interval_cstring),
										   ObjectIdGetDatum(part_atttype),
										   Int32GetDatum(-1));
		if (interval_type)
			*interval_type = part_atttype;
	}

	return interval_binary;
}

/*
 * Fetch binary operator by name and return it's function and ret type.
 */
static void
extract_op_func_and_ret_type(char *opname, Oid type1, Oid type2,
							 Oid *move_bound_op_func,		/* returned value #1 */
							 Oid *move_bound_op_ret_type)	/* returned value #2 */
{
	Operator op;

	/* Get "move bound operator" descriptor */
	op = get_binary_operator(opname, type1, type2);
	if (!op)
		elog(ERROR, "missing %s operator for types %s and %s",
			 opname, format_type_be(type1), format_type_be(type2));

	*move_bound_op_func = oprfuncid(op);
	*move_bound_op_ret_type = get_operator_ret_type(op);

	/* Don't forget to release system cache */
	ReleaseSysCache(op);
}

/*
 * Append\prepend partitions if there's no partition to store 'value'.
 *
 * Used by create_partitions_for_value_internal().
 *
 * NB: 'value' type is not needed since we've already taken
 * it into account while searching for the 'cmp_proc'.
 */
static Oid
spawn_partitions_val(Oid parent_relid,			/* parent's Oid */
					 Datum range_bound_min,		/* parent's MIN boundary */
					 Datum range_bound_max,		/* parent's MAX boundary */
					 Oid range_bound_type,		/* type of boundary's value */
					 Datum interval_binary,		/* interval in binary form */
					 Oid interval_type,			/* INTERVALOID or prel->atttype */
					 Datum value,				/* value to be INSERTed */
					 Oid value_type)			/* type of value */
{
	bool		should_append;				/* append or prepend? */

	Oid			move_bound_op_func,			/* operator's function */
				move_bound_op_ret_type;		/* operator's ret type */

	FmgrInfo	cmp_value_bound_finfo,		/* exec 'value (>=|<) bound' */
				move_bound_finfo;			/* exec 'bound + interval' */

	Datum		cur_leading_bound,			/* boundaries of a new partition */
				cur_following_bound;

	Oid			last_partition = InvalidOid;


	fill_type_cmp_fmgr_info(&cmp_value_bound_finfo, value_type, range_bound_type);

	/* value >= MAX_BOUNDARY */
	if (check_ge(&cmp_value_bound_finfo, value, range_bound_max))
	{
		should_append = true;
		cur_leading_bound = range_bound_max;
	}

	/* value < MIN_BOUNDARY */
	else if (check_lt(&cmp_value_bound_finfo, value, range_bound_min))
	{
		should_append = false;
		cur_leading_bound = range_bound_min;
	}

	/* There's a gap, halt and emit ERROR */
	else elog(ERROR, "cannot spawn a partition inside a gap");

	/* Fetch operator's underlying function and ret type */
	extract_op_func_and_ret_type(should_append ? "+" : "-",
								 range_bound_type,
								 interval_type,
								 &move_bound_op_func,
								 &move_bound_op_ret_type);

	/* Perform casts if types don't match (e.g. date + interval = timestamp) */
	if (move_bound_op_ret_type != range_bound_type)
	{
		/* Cast 'cur_leading_bound' to 'move_bound_op_ret_type' */
		cur_leading_bound = perform_type_cast(cur_leading_bound,
											  range_bound_type,
											  move_bound_op_ret_type,
											  NULL); /* might emit ERROR */

		/* Update 'range_bound_type' */
		range_bound_type = move_bound_op_ret_type;

		/* Fetch new comparison function */
		fill_type_cmp_fmgr_info(&cmp_value_bound_finfo,
								value_type,
								range_bound_type);

		/* Since type has changed, fetch another operator */
		extract_op_func_and_ret_type(should_append ? "+" : "-",
									 range_bound_type,
									 interval_type,
									 &move_bound_op_func,
									 &move_bound_op_ret_type);

		/* What, again? Don't want to deal with this nightmare */
		if (move_bound_op_ret_type != range_bound_type)
			elog(ERROR, "error in spawn_partitions_val()");
	}

	/* Get operator's underlying function */
	fmgr_info(move_bound_op_func, &move_bound_finfo);

	/* Execute comparison function cmp(value, cur_leading_bound) */
	while (should_append ?
				check_ge(&cmp_value_bound_finfo, value, cur_leading_bound) :
				check_lt(&cmp_value_bound_finfo, value, cur_leading_bound))
	{
		Datum args[2];
		Bound bounds[2];

		/* Assign the 'following' boundary to current 'leading' value */
		cur_following_bound = cur_leading_bound;

		/* Move leading bound by interval (exec 'leading (+|-) INTERVAL') */
		cur_leading_bound = FunctionCall2(&move_bound_finfo,
										  cur_leading_bound,
										  interval_binary);

		args[0] = should_append ? cur_following_bound : cur_leading_bound;
		args[1] = should_append ? cur_leading_bound : cur_following_bound;

		MakeBound(&bounds[0], args[0], FINITE);
		MakeBound(&bounds[1], args[1], FINITE);

		last_partition = create_single_range_partition_internal(parent_relid,
																&bounds[0], &bounds[1],
																range_bound_type,
																NULL, NULL);

#ifdef USE_ASSERT_CHECKING
		elog(DEBUG2, "%s partition with following='%s' & leading='%s' [%u]",
			 (should_append ? "Appending" : "Prepending"),
			 DebugPrintDatum(cur_following_bound, range_bound_type),
			 DebugPrintDatum(cur_leading_bound, range_bound_type),
			 MyProcPid);
#endif
	}

	return last_partition;
}

/* Choose a good name for a partition */
static char *
choose_partition_name(Oid parent_relid, Oid parent_nsp)
{
	Datum	part_num;
	Oid		part_seq_relid;

	part_seq_relid = get_relname_relid(build_sequence_name_internal(parent_relid),
									   parent_nsp);
	part_num = DirectFunctionCall1(nextval_oid, ObjectIdGetDatum(part_seq_relid));

	return psprintf("%s_%u", get_rel_name(parent_relid), DatumGetInt32(part_num));
}

/* Create a partition-like table (no constraints yet) */
static Oid
create_single_partition_internal(Oid parent_relid,
								 RangeVar *partition_rv,
								 char *tablespace,
								 char **partitioned_column) /* to be set */
{
	/* Value to be returned */
	Oid					partition_relid = InvalidOid; /* safety */

	/* Parent's namespace and name */
	Oid					parent_nsp;
	char			   *parent_name,
					   *parent_nsp_name;

	/* Values extracted from PATHMAN_CONFIG */
	Datum				config_values[Natts_pathman_config];
	bool				config_nulls[Natts_pathman_config];

	/* Elements of the "CREATE TABLE" query tree */
	RangeVar		   *parent_rv;
	TableLikeClause		like_clause;
	CreateStmt			create_stmt;
	List			   *create_stmts;
	ListCell		   *lc;


	/* Lock parent and check if it exists */
	LockRelationOid(parent_relid, ShareUpdateExclusiveLock);
	if (!SearchSysCacheExists1(RELOID, ObjectIdGetDatum(parent_relid)))
		elog(ERROR, "relation %u does not exist", parent_relid);

	/* Check that table is registered in PATHMAN_CONFIG */
	if (!pathman_config_contains_relation(parent_relid,
										  config_values, config_nulls, NULL))
		elog(ERROR, "table \"%s\" is not partitioned",
			 get_rel_name_or_relid(parent_relid));

	/* Cache parent's namespace and name */
	parent_name = get_rel_name(parent_relid);
	parent_nsp = get_rel_namespace(parent_relid);
	parent_nsp_name = get_namespace_name(parent_nsp);

	/* Fetch partitioned column's name */
	if (partitioned_column)
	{
		Datum partitioned_column_datum;

		partitioned_column_datum = config_values[Anum_pathman_config_attname - 1];
		*partitioned_column = TextDatumGetCString(partitioned_column_datum);
	}

	/* Make up parent's RangeVar */
	parent_rv = makeRangeVar(parent_nsp_name, parent_name, -1);

	/* Generate a name if asked to */
	if (!partition_rv)
	{
		char *part_name;

		/* Make up a name for the partition */
		part_name = choose_partition_name(parent_relid, parent_nsp);

		/* Make RangeVar for the partition */
		partition_rv = makeRangeVar(parent_nsp_name, part_name, -1);
	}

	/* If no 'tablespace' is provided, get parent's tablespace */
	if (!tablespace)
		tablespace = get_tablespace_name(get_rel_tablespace(parent_relid));

	/* Initialize TableLikeClause structure */
	NodeSetTag(&like_clause, T_TableLikeClause);
	like_clause.relation		= copyObject(parent_rv);
	like_clause.options			= CREATE_TABLE_LIKE_DEFAULTS |
								  CREATE_TABLE_LIKE_CONSTRAINTS |
								  CREATE_TABLE_LIKE_INDEXES |
								  CREATE_TABLE_LIKE_STORAGE;

	/* Initialize CreateStmt structure */
	NodeSetTag(&create_stmt, T_CreateStmt);
	create_stmt.relation		= copyObject(partition_rv);
	create_stmt.tableElts		= list_make1(copyObject(&like_clause));
	create_stmt.inhRelations	= list_make1(copyObject(parent_rv));
	create_stmt.ofTypename		= NULL;
	create_stmt.constraints		= NIL;
	create_stmt.options			= NIL;
	create_stmt.oncommit		= ONCOMMIT_NOOP;
	create_stmt.tablespacename	= tablespace;
	create_stmt.if_not_exists	= false;

	/* Generate columns using the parent table */
	create_stmts = transformCreateStmt(&create_stmt, NULL);

	/* Create the partition and all required relations */
	foreach (lc, create_stmts)
	{
		Node *cur_stmt;

		/* Fetch current CreateStmt */
		cur_stmt = (Node *) lfirst(lc);

		if (IsA(cur_stmt, CreateStmt))
		{
			Oid child_relowner;

			/* Partition should have the same owner as the parent */
			child_relowner = get_rel_owner(parent_relid);

			/* Create a partition and save its Oid */
			partition_relid = create_table_using_stmt((CreateStmt *) cur_stmt,
													  child_relowner).objectId;

			/* Copy FOREIGN KEYS of the parent table */
			copy_foreign_keys(parent_relid, partition_relid);
		}
		else if (IsA(cur_stmt, CreateForeignTableStmt))
		{
			elog(ERROR, "FDW partition creation is not implemented yet");
		}
		else
		{
			/*
			 * Recurse for anything else.  Note the recursive
			 * call will stash the objects so created into our
			 * event trigger context.
			 */
			ProcessUtility(cur_stmt,
						   "we have to provide a query string",
						   PROCESS_UTILITY_SUBCOMMAND,
						   NULL,
						   None_Receiver,
						   NULL);
		}

		/* Update config one more time */
		CommandCounterIncrement();
	}

	return partition_relid;
}

/* Create a new table using cooked CreateStmt */
static ObjectAddress
create_table_using_stmt(CreateStmt *create_stmt, Oid relowner)
{
	ObjectAddress	table_addr;
	Datum			toast_options;
	static char	   *validnsps[] = HEAP_RELOPT_NAMESPACES;
	int				guc_level;

	/* Create new GUC level... */
	guc_level = NewGUCNestLevel();

	/* ... and set client_min_messages = WARNING */
	(void) set_config_option("client_min_messages", "WARNING",
							 PGC_USERSET, PGC_S_SESSION,
							 GUC_ACTION_SAVE, true, 0, false);

	/* Create new partition owned by parent's posessor */
	table_addr = DefineRelation(create_stmt, RELKIND_RELATION, relowner, NULL);

	/* Save data about a simple DDL command that was just executed */
	EventTriggerCollectSimpleCommand(table_addr,
									 InvalidObjectAddress,
									 (Node *) create_stmt);

	/*
	 * Let NewRelationCreateToastTable decide if this
	 * one needs a secondary relation too.
	 */
	CommandCounterIncrement();

	/* Parse and validate reloptions for the toast table */
	toast_options = transformRelOptions((Datum) 0, create_stmt->options,
										"toast", validnsps, true, false);

	/* Parse options for a new toast table */
	(void) heap_reloptions(RELKIND_TOASTVALUE, toast_options, true);

	/* Now create the toast table if needed */
	NewRelationCreateToastTable(table_addr.objectId, toast_options);

	/* Restore original GUC values */
	AtEOXact_GUC(true, guc_level);

	/* Return the address */
	return table_addr;
}

/* Copy foreign keys of parent table */
static void
copy_foreign_keys(Oid parent_relid, Oid partition_oid)
{
	Oid						copy_fkeys_proc_args[] = { REGCLASSOID, REGCLASSOID };
	List				   *copy_fkeys_proc_name;
	FmgrInfo				copy_fkeys_proc_flinfo;
	FunctionCallInfoData	copy_fkeys_proc_fcinfo;
	char					*pathman_schema;

	/* Fetch pg_pathman's schema */
	pathman_schema = get_namespace_name(get_pathman_schema());

	/* Build function's name */
	copy_fkeys_proc_name = list_make2(makeString(pathman_schema),
									  makeString(CppAsString(copy_foreign_keys)));

	/* Lookup function's Oid and get FmgrInfo */
	fmgr_info(LookupFuncName(copy_fkeys_proc_name, 2,
							 copy_fkeys_proc_args, false),
			  &copy_fkeys_proc_flinfo);

	InitFunctionCallInfoData(copy_fkeys_proc_fcinfo, &copy_fkeys_proc_flinfo,
							 2, InvalidOid, NULL, NULL);
	copy_fkeys_proc_fcinfo.arg[0] = ObjectIdGetDatum(parent_relid);
	copy_fkeys_proc_fcinfo.argnull[0] = false;
	copy_fkeys_proc_fcinfo.arg[1] = ObjectIdGetDatum(partition_oid);
	copy_fkeys_proc_fcinfo.argnull[1] = false;

	/* Invoke the callback */
	FunctionCallInvoke(&copy_fkeys_proc_fcinfo);
}


/*
 * -----------------------------
 *  Check constraint generation
 * -----------------------------
 */

/* Build RANGE check constraint expression tree */
Node *
build_raw_range_check_tree(char *attname,
						   const Bound *start_value,
						   const Bound *end_value,
						   Oid value_type)
{
	BoolExpr   *and_oper	= makeNode(BoolExpr);
	A_Expr	   *left_arg	= makeNode(A_Expr),
			   *right_arg	= makeNode(A_Expr);
	A_Const	   *left_const	= makeNode(A_Const),
			   *right_const	= makeNode(A_Const);
	ColumnRef  *col_ref		= makeNode(ColumnRef);

	/* Partitioned column */
	col_ref->fields = list_make1(makeString(attname));
	col_ref->location = -1;

	and_oper->boolop = AND_EXPR;
	and_oper->args = NIL;
	and_oper->location = -1;

	/* Left comparison (VAR >= start_value) */
	if (!IsInfinite(start_value))
	{
		/* Left boundary */
		left_const->val = *makeString(
			datum_to_cstring(BoundGetValue(start_value), value_type));
		left_const->location = -1;

		left_arg->name		= list_make1(makeString(">="));
		left_arg->kind		= AEXPR_OP;
		left_arg->lexpr		= (Node *) col_ref;
		left_arg->rexpr		= (Node *) left_const;
		left_arg->location	= -1;
		and_oper->args = lappend(and_oper->args, left_arg);
	}

	/* Right comparision (VAR < end_value) */
	if (!IsInfinite(end_value))
	{
		/* Right boundary */
		right_const->val = *makeString(
			datum_to_cstring(BoundGetValue(end_value), value_type));
		right_const->location = -1;

		right_arg->name		= list_make1(makeString("<"));
		right_arg->kind		= AEXPR_OP;
		right_arg->lexpr	= (Node *) col_ref;
		right_arg->rexpr	= (Node *) right_const;
		right_arg->location	= -1;
		and_oper->args = lappend(and_oper->args, right_arg);
	}

	if (and_oper->args == NIL)
		elog(ERROR, "Cannot create infinite range constraint");

	return (Node *) and_oper;
}

/* Build complete RANGE check constraint */
Constraint *
build_range_check_constraint(Oid child_relid,
							 char *attname,
							 const Bound *start_value,
							 const Bound *end_value,
							 Oid value_type)
{
	Constraint	   *range_constr;
	char		   *range_constr_name;
	AttrNumber		attnum;

	/* Build a correct name for this constraint */
	attnum = get_attnum(child_relid, attname);
	range_constr_name = build_check_constraint_name_internal(child_relid, attnum);

	/* Initialize basic properties of a CHECK constraint */
	range_constr = makeNode(Constraint);
	range_constr->conname			= range_constr_name;
	range_constr->deferrable		= false;
	range_constr->initdeferred		= false;
	range_constr->location			= -1;
	range_constr->contype			= CONSTR_CHECK;
	range_constr->is_no_inherit		= true;

	/* Validate existing data using this constraint */
	range_constr->skip_validation	= false;
	range_constr->initially_valid	= true;

	/* Finally we should build an expression tree */
	range_constr->raw_expr = build_raw_range_check_tree(attname,
														start_value,
														end_value,
														value_type);
	/* Everything seems to be fine */
	return range_constr;
}

/* Check if range overlaps with any partitions */
bool
check_range_available(Oid parent_relid,
					  const Bound *start,
					  const Bound *end,
					  Oid value_type,
					  bool raise_error)
{
	const PartRelationInfo	   *prel;
	RangeEntry				   *ranges;
	FmgrInfo					cmp_func;
	uint32						i;

	/* Try fetching the PartRelationInfo structure */
	prel = get_pathman_relation_info(parent_relid);

	/* If there's no prel, return TRUE (overlap is not possible) */
	if (!prel) return true;

	/* Emit an error if it is not partitioned by RANGE */
	shout_if_prel_is_invalid(parent_relid, prel, PT_RANGE);

	/* Fetch comparison function */
	fill_type_cmp_fmgr_info(&cmp_func,
							getBaseType(value_type),
							getBaseType(prel->atttype));

	ranges = PrelGetRangesArray(prel);
	for (i = 0; i < PrelChildrenCount(prel); i++)
	{
		int c1, c2;

		/*
		 * If the range we're checking starts with minus infinity or current
		 * range ends in plus infinity then the left boundary of the first
		 * range is on the left. Otherwise compare specific values
		 */
		// c1 = (IsInfinite(start) || IsInfinite(&ranges[i].max)) ?
		// 	-1 :
		// 	FunctionCall2(&cmp_func, 
		// 				  BoundGetValue(start),
		// 				  BoundGetValue(&ranges[i].max));
		/*
		 * Similary check that right boundary of the range we're checking is on
		 * the right of the beginning of the current one
		 */
		// c2 = (IsInfinite(end) || IsInfinite(&ranges[i].min)) ?
		// 	1 :
		// 	FunctionCall2(&cmp_func,
		// 				  BoundGetValue(end),
		// 				  BoundGetValue(&ranges[i].min));

		c1 = cmp_bounds(&cmp_func, start, &ranges[i].max);
		c2 = cmp_bounds(&cmp_func, end, &ranges[i].min);

		/* There's someone! */
		if (c1 < 0 && c2 > 0)
		{
			if (raise_error)
				elog(ERROR, "specified range [%s, %s) overlaps "
							"with existing partitions",
					 !IsInfinite(start) ? datum_to_cstring(BoundGetValue(start), value_type) : "NULL",
					 !IsInfinite(end) ? datum_to_cstring(BoundGetValue(end), value_type) : "NULL");
			else
				return false;
		}
	}

	return true;
}


/*
 * ---------------------
 *  Callback invocation
 * ---------------------
 */

/* Invoke 'init_callback' for a partition */
static void
invoke_init_callback_internal(init_callback_params *cb_params)
{
#define JSB_INIT_VAL(value, val_type, val_cstring) \
	do { \
		(value)->type = jbvString; \
		(value)->val.string.len = strlen(val_cstring); \
		(value)->val.string.val = val_cstring; \
		pushJsonbValue(&jsonb_state, val_type, (value)); \
	} while (0)

#define JSB_INIT_NULL_VAL(value, val_type)	\
	do {	\
		(value)->type = jbvNull;	\
		pushJsonbValue(&jsonb_state, val_type, (value)); \
	} while (0)

	Oid						parent_oid = cb_params->parent_relid;
	Oid						partition_oid = cb_params->partition_relid;

	FmgrInfo				cb_flinfo;
	FunctionCallInfoData	cb_fcinfo;

	JsonbParseState		   *jsonb_state = NULL;
	JsonbValue			   *result,
							key,
							val;

	switch (cb_params->parttype)
	{
		case PT_HASH:
			{
				pushJsonbValue(&jsonb_state, WJB_BEGIN_OBJECT, NULL);

				JSB_INIT_VAL(&key, WJB_KEY, "parent");
				JSB_INIT_VAL(&val, WJB_VALUE, get_rel_name_or_relid(parent_oid));
				JSB_INIT_VAL(&key, WJB_KEY, "partition");
				JSB_INIT_VAL(&val, WJB_VALUE, get_rel_name_or_relid(partition_oid));
				JSB_INIT_VAL(&key, WJB_KEY, "parttype");
				JSB_INIT_VAL(&val, WJB_VALUE, PartTypeToCString(PT_HASH));

				result = pushJsonbValue(&jsonb_state, WJB_END_OBJECT, NULL);
			}
			break;

		case PT_RANGE:
			{
				char   *start_value,
					   *end_value;
				Bound	sv_datum	= cb_params->params.range_params.start_value,
						ev_datum	= cb_params->params.range_params.end_value;
				Oid		type		= cb_params->params.range_params.value_type;

				/* Convert min & max to CSTRING */
				// start_value = datum_to_cstring(sv_datum, type);
				// end_value = datum_to_cstring(ev_datum, type);

				pushJsonbValue(&jsonb_state, WJB_BEGIN_OBJECT, NULL);

				JSB_INIT_VAL(&key, WJB_KEY, "parent");
				JSB_INIT_VAL(&val, WJB_VALUE, get_rel_name_or_relid(parent_oid));
				JSB_INIT_VAL(&key, WJB_KEY, "partition");
				JSB_INIT_VAL(&val, WJB_VALUE, get_rel_name_or_relid(partition_oid));
				JSB_INIT_VAL(&key, WJB_KEY, "parttype");
				JSB_INIT_VAL(&val, WJB_VALUE, PartTypeToCString(PT_RANGE));

				/* Lower bound */
				JSB_INIT_VAL(&key, WJB_KEY, "range_min");
				if (!IsInfinite(&sv_datum))
				{
					start_value = datum_to_cstring(BoundGetValue(&sv_datum), type);
					JSB_INIT_VAL(&val, WJB_VALUE, start_value);
				}
				else
					JSB_INIT_NULL_VAL(&val, WJB_VALUE);

				/* Upper bound */
				JSB_INIT_VAL(&key, WJB_KEY, "range_max");
				if (!IsInfinite(&ev_datum))
				{
					end_value = datum_to_cstring(BoundGetValue(&ev_datum), type);
					JSB_INIT_VAL(&val, WJB_VALUE, end_value);
				}
				else
					JSB_INIT_NULL_VAL(&val, WJB_VALUE);
				// JSB_INIT_VAL(&val, WJB_VALUE, end_value);

				result = pushJsonbValue(&jsonb_state, WJB_END_OBJECT, NULL);
			}
			break;

		default:
			elog(ERROR, "Unknown partitioning type %u", cb_params->parttype);
			break;
	}

	/* Fetch & cache callback's Oid if needed */
	if (!cb_params->callback_is_cached)
	{
		Datum	param_values[Natts_pathman_config_params];
		bool	param_isnull[Natts_pathman_config_params];

		/* Search for init_callback entry in PATHMAN_CONFIG_PARAMS */
		if (read_pathman_params(parent_oid, param_values, param_isnull))
		{
			Datum		init_cb_datum; /* Oid of init_callback */
			AttrNumber	init_cb_attno = Anum_pathman_config_params_init_callback;

			/* Extract Datum storing callback's Oid */
			init_cb_datum = param_values[init_cb_attno - 1];

			/* Cache init_callback's Oid */
			cb_params->callback = DatumGetObjectId(init_cb_datum);
		}
	}

	/* No callback is set, exit */
	if (!OidIsValid(cb_params->callback))
		return;

	/* Validate the callback's signature */
	validate_on_part_init_cb(cb_params->callback, true);

	fmgr_info(cb_params->callback, &cb_flinfo);

	InitFunctionCallInfoData(cb_fcinfo, &cb_flinfo, 1, InvalidOid, NULL, NULL);
	cb_fcinfo.arg[0] = PointerGetDatum(JsonbValueToJsonb(result));
	cb_fcinfo.argnull[0] = false;

	/* Invoke the callback */
	FunctionCallInvoke(&cb_fcinfo);
}

/* Invoke a callback of a specified type */
void
invoke_part_callback(init_callback_params *cb_params)
{
	switch (cb_params->cb_type)
	{
		case PT_INIT_CALLBACK:
			invoke_init_callback_internal(cb_params);
			break;

		default:
			elog(ERROR, "Unknown callback type: %u", cb_params->cb_type);
	}
}
