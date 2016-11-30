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
#include "access/sysattr.h"
#include "access/xact.h"
#include "catalog/heap.h"
#include "catalog/pg_authid.h"
#include "catalog/pg_proc.h"
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
#include "utils/fmgroids.h"
#include "utils/jsonb.h"
#include "utils/snapmgr.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"
#include "utils/typcache.h"


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

static void create_single_partition_common(Oid partition_relid,
										   Constraint *check_constraint,
										   init_callback_params *callback_params);

static Oid create_single_partition_internal(Oid parent_relid,
											RangeVar *partition_rv,
											char *tablespace,
											char **partitioned_column);

static char *choose_range_partition_name(Oid parent_relid, Oid parent_nsp);
static char *choose_hash_partition_name(Oid parent_relid, uint32 part_idx);

static ObjectAddress create_table_using_stmt(CreateStmt *create_stmt,
											 Oid relowner);

static void copy_foreign_keys(Oid parent_relid, Oid partition_oid);
static void copy_acl_privileges(Oid parent_relid, Oid partition_relid);

static Constraint *make_constraint_common(char *name, Node *raw_expr);

static Value make_string_value_struct(char *str);
static Value make_int_value_struct(int int_val);


/*
 * ---------------------------------------
 *  Public interface (partition creation)
 * ---------------------------------------
 */

/* Create one RANGE partition [start_value, end_value) */
Oid
create_single_range_partition_internal(Oid parent_relid,
									   Datum start_value,
									   Datum end_value,
									   Oid value_type,
									   RangeVar *partition_rv,
									   char *tablespace)
{
	Oid						partition_relid;
	Constraint			   *check_constr;
	char				   *partitioned_column;
	init_callback_params	callback_params;

	/* Generate a name if asked to */
	if (!partition_rv)
	{
		Oid		parent_nsp = get_rel_namespace(parent_relid);
		char   *parent_nsp_name = get_namespace_name(parent_nsp);
		char   *partition_name;

		partition_name = choose_range_partition_name(parent_relid, parent_nsp);

		partition_rv = makeRangeVar(parent_nsp_name, partition_name, -1);
	}

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

	/* Cook args for init_callback */
	MakeInitCallbackRangeParams(&callback_params,
								DEFAULT_INIT_CALLBACK,
								parent_relid, partition_relid,
								start_value, end_value, value_type);

	/* Add constraint & execute init_callback */
	create_single_partition_common(partition_relid,
								   check_constr,
								   &callback_params);

	/* Return the Oid */
	return partition_relid;
}

/* Create one HASH partition */
Oid
create_single_hash_partition_internal(Oid parent_relid,
									  uint32 part_idx,
									  uint32 part_count,
									  Oid value_type,
									  RangeVar *partition_rv,
									  char *tablespace)
{
	Oid						partition_relid;
	Constraint			   *check_constr;
	char				   *partitioned_column;
	init_callback_params	callback_params;

	/* Generate a name if asked to */
	if (!partition_rv)
	{
		Oid		parent_nsp = get_rel_namespace(parent_relid);
		char   *parent_nsp_name = get_namespace_name(parent_nsp);
		char   *partition_name;

		partition_name = choose_hash_partition_name(parent_relid, part_idx);

		partition_rv = makeRangeVar(parent_nsp_name, partition_name, -1);
	}

	/* Create a partition & get 'partitioned_column' */
	partition_relid = create_single_partition_internal(parent_relid,
													   partition_rv,
													   tablespace,
													   &partitioned_column);

	/* Build check constraint for HASH partition */
	check_constr = build_hash_check_constraint(partition_relid,
											   partitioned_column,
											   part_idx,
											   part_count,
											   value_type);

	/* Cook args for init_callback */
	MakeInitCallbackHashParams(&callback_params,
							   DEFAULT_INIT_CALLBACK,
							   parent_relid, partition_relid);

	/* Add constraint & execute init_callback */
	create_single_partition_common(partition_relid,
								   check_constr,
								   &callback_params);

	/* Return the Oid */
	return partition_relid;
}

/* Add constraint & execute init_callback */
void
create_single_partition_common(Oid partition_relid,
							   Constraint *check_constraint,
							   init_callback_params *callback_params)
{
	Relation child_relation;

	/* Open the relation and add new check constraint & fkeys */
	child_relation = heap_open(partition_relid, AccessExclusiveLock);
	AddRelationNewConstraints(child_relation, NIL,
							  list_make1(check_constraint),
							  false, true, true);
	heap_close(child_relation, NoLock);

	/* Make constraint visible */
	CommandCounterIncrement();

	/* Finally invoke 'init_callback' */
	invoke_part_callback(callback_params);

	/* Make possible changes visible */
	CommandCounterIncrement();
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
		/* Was table partitioned in some previous transaction? */
		bool	part_in_prev_xact =
					TransactionIdPrecedes(rel_xmin, GetCurrentTransactionId()) ||
					TransactionIdEquals(rel_xmin, FrozenTransactionId);

		/* Take default values */
		bool	spawn_using_bgw	= DEFAULT_SPAWN_USING_BGW,
				enable_auto		= DEFAULT_AUTO;

		/* Values to be extracted from PATHMAN_CONFIG_PARAMS */
		Datum	values[Natts_pathman_config_params];
		bool	isnull[Natts_pathman_config_params];

		/* Try fetching options from PATHMAN_CONFIG_PARAMS */
		if (read_pathman_params(relid, values, isnull))
		{
			enable_auto = values[Anum_pathman_config_params_auto - 1];
			spawn_using_bgw = values[Anum_pathman_config_params_spawn_using_bgw - 1];
		}

		/* Emit ERROR if automatic partition creation is disabled */
		if (!enable_auto || !IsAutoPartitionEnabled())
			elog(ERROR, ERR_PART_ATTR_NO_PART, datum_to_cstring(value, value_type));

		/*
		 * If table has been partitioned in some previous xact AND
		 * we don't hold any conflicting locks, run BGWorker.
		 */
		if (spawn_using_bgw && part_in_prev_xact &&
			!xact_bgw_conflicting_lock_exists(relid))
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
				Datum		bound_min,			/* absolute MIN */
							bound_max;			/* absolute MAX */

				Oid			interval_type = InvalidOid;
				Datum		interval_binary, /* assigned 'width' of one partition */
							interval_text;

				/* Read max & min range values from PartRelationInfo */
				bound_min = PrelGetRangesArray(prel)[0].min;
				bound_max = PrelGetRangesArray(prel)[PrelLastChild(prel)].max;

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
			elog(ERROR, "error in function " CppAsString(spawn_partitions_val));
	}

	/* Get operator's underlying function */
	fmgr_info(move_bound_op_func, &move_bound_finfo);

	/* Execute comparison function cmp(value, cur_leading_bound) */
	while (should_append ?
				check_ge(&cmp_value_bound_finfo, value, cur_leading_bound) :
				check_lt(&cmp_value_bound_finfo, value, cur_leading_bound))
	{
		Datum args[2];

		/* Assign the 'following' boundary to current 'leading' value */
		cur_following_bound = cur_leading_bound;

		/* Move leading bound by interval (exec 'leading (+|-) INTERVAL') */
		cur_leading_bound = FunctionCall2(&move_bound_finfo,
										  cur_leading_bound,
										  interval_binary);

		args[0] = should_append ? cur_following_bound : cur_leading_bound;
		args[1] = should_append ? cur_leading_bound : cur_following_bound;

		last_partition = create_single_range_partition_internal(parent_relid,
																args[0], args[1],
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

/* Choose a good name for a RANGE partition */
static char *
choose_range_partition_name(Oid parent_relid, Oid parent_nsp)
{
	Datum	part_num;
	Oid		part_seq_relid;
	Oid		save_userid;
	int		save_sec_context;
	bool	need_priv_escalation = !superuser(); /* we might be a SU */

	part_seq_relid = get_relname_relid(build_sequence_name_internal(parent_relid),
									   parent_nsp);

	/* Do we have to escalate privileges? */
	if (need_priv_escalation)
	{
		/* Get current user's Oid and security context */
		GetUserIdAndSecContext(&save_userid, &save_sec_context);

		/* Become superuser in order to bypass sequence ACL checks */
		SetUserIdAndSecContext(BOOTSTRAP_SUPERUSERID,
							   save_sec_context | SECURITY_LOCAL_USERID_CHANGE);
	}

	/* Get next integer for partition name */
	part_num = DirectFunctionCall1(nextval_oid, ObjectIdGetDatum(part_seq_relid));

	/* Restore user's privileges */
	if (need_priv_escalation)
		SetUserIdAndSecContext(save_userid, save_sec_context);

	return psprintf("%s_%u", get_rel_name(parent_relid), DatumGetInt32(part_num));
}

/* Choose a good name for a HASH partition */
static char *
choose_hash_partition_name(Oid parent_relid, uint32 part_idx)
{
	return psprintf("%s_%u", get_rel_name(parent_relid), part_idx);
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

	/* Current user and security context */
	Oid					save_userid;
	int					save_sec_context;
	bool				need_priv_escalation = !superuser(); /* we might be a SU */

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

	Assert(partition_rv);

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

	/* Do we have to escalate privileges? */
	if (need_priv_escalation)
	{
		/* Get current user's Oid and security context */
		GetUserIdAndSecContext(&save_userid, &save_sec_context);

		/* Check that user's allowed to spawn partitions */
		if (ACLCHECK_OK != pg_class_aclcheck(parent_relid, save_userid,
											 ACL_SPAWN_PARTITIONS))
			ereport(ERROR,
					(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
					 errmsg("permission denied for parent relation \"%s\"",
							get_rel_name_or_relid(parent_relid)),
					 errdetail("user is not allowed to create new partitions"),
					 errhint("consider granting INSERT privilege")));

		/* Become superuser in order to bypass various ACL checks */
		SetUserIdAndSecContext(BOOTSTRAP_SUPERUSERID,
							   save_sec_context | SECURITY_LOCAL_USERID_CHANGE);
	}

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

			/* Make changes visible */
			CommandCounterIncrement();

			/* Copy ACL privileges of the parent table */
			copy_acl_privileges(parent_relid, partition_relid);
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

	/* Restore user's privileges */
	if (need_priv_escalation)
		SetUserIdAndSecContext(save_userid, save_sec_context);

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

/* Copy ACL privileges of parent table */
static void
copy_acl_privileges(Oid parent_relid, Oid partition_relid)
{
	Relation		pg_class_rel,
					pg_attribute_rel;

	TupleDesc		pg_class_desc,
					pg_attribute_desc;

	HeapTuple		htup;
	ScanKeyData		skey[2];
	SysScanDesc		scan;

	Datum			acl_datum;
	bool			acl_null;

	Snapshot		snapshot;

	pg_class_rel = heap_open(RelationRelationId, RowExclusiveLock);
	pg_attribute_rel = heap_open(AttributeRelationId, RowExclusiveLock);

	/* Get most recent snapshot */
	snapshot = RegisterSnapshot(GetLatestSnapshot());

	pg_class_desc = RelationGetDescr(pg_class_rel);
	pg_attribute_desc = RelationGetDescr(pg_attribute_rel);

	htup = SearchSysCache1(RELOID, ObjectIdGetDatum(parent_relid));
	if (!HeapTupleIsValid(htup))
		elog(ERROR, "cache lookup failed for relation %u", parent_relid);

	/* Get parent's ACL */
	acl_datum = heap_getattr(htup, Anum_pg_class_relacl, pg_class_desc, &acl_null);

	/* Copy datum if it's not NULL */
	if (!acl_null)
	{
		Form_pg_attribute acl_column;

		acl_column = pg_class_desc->attrs[Anum_pg_class_relacl - 1];

		acl_datum = datumCopy(acl_datum, acl_column->attbyval, acl_column->attlen);
	}

	/* Release 'htup' */
	ReleaseSysCache(htup);

	/* Search for 'partition_relid' */
	ScanKeyInit(&skey[0],
				ObjectIdAttributeNumber,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(partition_relid));

	scan = systable_beginscan(pg_class_rel, ClassOidIndexId,
							  true, snapshot, 1, skey);

	/* There should be exactly one tuple (our child) */
	if (HeapTupleIsValid(htup = systable_getnext(scan)))
	{
		ItemPointerData		iptr;
		Datum				values[Natts_pg_class] = { (Datum) 0 };
		bool				nulls[Natts_pg_class] = { false };
		bool				replaces[Natts_pg_class] = { false };

		/* Copy ItemPointer of this tuple */
		iptr = htup->t_self;

		values[Anum_pg_class_relacl - 1] = acl_datum;	/* ACL array */
		nulls[Anum_pg_class_relacl - 1] = acl_null;		/* do we have ACL? */
		replaces[Anum_pg_class_relacl - 1] = true;

		/* Build new tuple with parent's ACL */
		htup = heap_modify_tuple(htup, pg_class_desc, values, nulls, replaces);

		/* Update child's tuple */
		simple_heap_update(pg_class_rel, &iptr, htup);

		/* Don't forget to update indexes */
		CatalogUpdateIndexes(pg_class_rel, htup);
	}

	systable_endscan(scan);


	/* Search for 'parent_relid's columns */
	ScanKeyInit(&skey[0],
				Anum_pg_attribute_attrelid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(parent_relid));

	/* Consider only user-defined columns (>0) */
	ScanKeyInit(&skey[1],
				Anum_pg_attribute_attnum,
				BTEqualStrategyNumber, F_INT2GT,
				Int16GetDatum(InvalidAttrNumber));

	scan = systable_beginscan(pg_attribute_rel,
							  AttributeRelidNumIndexId,
							  true, snapshot, 2, skey);

	/* Go through the list of parent's columns */
	while (HeapTupleIsValid(htup = systable_getnext(scan)))
	{
		ScanKeyData		subskey[2];
		SysScanDesc		subscan;
		HeapTuple		subhtup;

		AttrNumber		cur_attnum;
		bool			cur_attnum_null;

		/* Get parent column's ACL */
		acl_datum = heap_getattr(htup, Anum_pg_attribute_attacl,
								 pg_attribute_desc, &acl_null);

		/* Copy datum if it's not NULL */
		if (!acl_null)
		{
			Form_pg_attribute acl_column;

			acl_column = pg_attribute_desc->attrs[Anum_pg_attribute_attacl - 1];

			acl_datum = datumCopy(acl_datum,
								  acl_column->attbyval,
								  acl_column->attlen);
		}

		/* Fetch number of current column */
		cur_attnum = DatumGetInt16(heap_getattr(htup, Anum_pg_attribute_attnum,
												pg_attribute_desc, &cur_attnum_null));
		Assert(cur_attnum_null == false); /* must not be NULL! */

		/* Search for 'partition_relid' */
		ScanKeyInit(&subskey[0],
					Anum_pg_attribute_attrelid,
					BTEqualStrategyNumber, F_OIDEQ,
					ObjectIdGetDatum(partition_relid));

		/* Search for 'partition_relid's columns */
		ScanKeyInit(&subskey[1],
					Anum_pg_attribute_attnum,
					BTEqualStrategyNumber, F_INT2EQ,
					Int16GetDatum(cur_attnum));

		subscan = systable_beginscan(pg_attribute_rel,
									 AttributeRelidNumIndexId,
									 true, snapshot, 2, subskey);

		/* There should be exactly one tuple (our child's column) */
		if (HeapTupleIsValid(subhtup = systable_getnext(subscan)))
		{
			ItemPointerData		iptr;
			Datum				values[Natts_pg_attribute] = { (Datum) 0 };
			bool				nulls[Natts_pg_attribute] = { false };
			bool				replaces[Natts_pg_attribute] = { false };

			/* Copy ItemPointer of this tuple */
			iptr = subhtup->t_self;

			values[Anum_pg_attribute_attacl - 1] = acl_datum;	/* ACL array */
			nulls[Anum_pg_attribute_attacl - 1] = acl_null;		/* do we have ACL? */
			replaces[Anum_pg_attribute_attacl - 1] = true;

			/* Build new tuple with parent's ACL */
			subhtup = heap_modify_tuple(subhtup, pg_attribute_desc,
										values, nulls, replaces);

			/* Update child's tuple */
			simple_heap_update(pg_attribute_rel, &iptr, subhtup);

			/* Don't forget to update indexes */
			CatalogUpdateIndexes(pg_attribute_rel, subhtup);
		}

		systable_endscan(subscan);
	}

	systable_endscan(scan);

	/* Don't forget to free snapshot */
	UnregisterSnapshot(snapshot);

	heap_close(pg_class_rel, RowExclusiveLock);
	heap_close(pg_attribute_rel, RowExclusiveLock);
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
						   Datum start_value,
						   Datum end_value,
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

	/* Left boundary */
	left_const->val = make_string_value_struct(datum_to_cstring(start_value,
																value_type));
	left_const->location = -1;

	/* Right boundary */
	right_const->val = make_string_value_struct(datum_to_cstring(end_value,
																 value_type));
	right_const->location = -1;

	/* Left comparison (VAR >= start_value) */
	left_arg->name		= list_make1(makeString(">="));
	left_arg->kind		= AEXPR_OP;
	left_arg->lexpr		= (Node *) col_ref;
	left_arg->rexpr		= (Node *) left_const;
	left_arg->location	= -1;

	/* Right comparision (VAR < end_value) */
	right_arg->name		= list_make1(makeString("<"));
	right_arg->kind		= AEXPR_OP;
	right_arg->lexpr	= (Node *) col_ref;
	right_arg->rexpr	= (Node *) right_const;
	right_arg->location	= -1;

	and_oper->boolop = AND_EXPR;
	and_oper->args = list_make2(left_arg, right_arg);
	and_oper->location = -1;

	return (Node *) and_oper;
}

/* Build complete RANGE check constraint */
Constraint *
build_range_check_constraint(Oid child_relid,
							 char *attname,
							 Datum start_value,
							 Datum end_value,
							 Oid value_type)
{
	Constraint	   *hash_constr;
	char		   *range_constr_name;
	AttrNumber		attnum;

	/* Build a correct name for this constraint */
	attnum = get_attnum(child_relid, attname);
	range_constr_name = build_check_constraint_name_relid_internal(child_relid,
																   attnum);

	/* Initialize basic properties of a CHECK constraint */
	hash_constr = make_constraint_common(range_constr_name,
										 build_raw_range_check_tree(attname,
																	start_value,
																	end_value,
																	value_type));
	/* Everything seems to be fine */
	return hash_constr;
}

/* Check if range overlaps with any partitions */
bool
check_range_available(Oid parent_relid,
					  Datum start_value,
					  Datum end_value,
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
		int		c1 = FunctionCall2(&cmp_func, start_value, ranges[i].max),
				c2 = FunctionCall2(&cmp_func, end_value, ranges[i].min);

		/* There's someone! */
		if (c1 < 0 && c2 > 0)
		{
			if (raise_error)
				elog(ERROR, "specified range [%s, %s) overlaps "
							"with existing partitions",
					 datum_to_cstring(start_value, value_type),
					 datum_to_cstring(end_value, value_type));
			else
				return false;
		}
	}

	return true;
}

/* Build HASH check constraint expression tree */
Node *
build_raw_hash_check_tree(char *attname,
						  uint32 part_idx,
						  uint32 part_count,
						  Oid value_type)
{
	A_Expr		   *eq_oper			= makeNode(A_Expr);
	FuncCall	   *part_idx_call	= makeNode(FuncCall),
				   *hash_call		= makeNode(FuncCall);
	ColumnRef	   *hashed_column	= makeNode(ColumnRef);
	A_Const		   *part_idx_c		= makeNode(A_Const),
				   *part_count_c	= makeNode(A_Const);

	List		   *get_hash_part_idx_proc;

	Oid				hash_proc;
	TypeCacheEntry *tce;

	tce = lookup_type_cache(value_type, TYPECACHE_HASH_PROC);
	hash_proc = tce->hash_proc;

	/* Partitioned column */
	hashed_column->fields = list_make1(makeString(attname));
	hashed_column->location = -1;

	/* Total amount of partitions */
	part_count_c->val = make_int_value_struct(part_count);
	part_count_c->location = -1;

	/* Index of this partition (hash % total amount) */
	part_idx_c->val = make_int_value_struct(part_idx);
	part_idx_c->location = -1;

	/* Call hash_proc() */
	hash_call->funcname			= list_make1(makeString(get_func_name(hash_proc)));
	hash_call->args				= list_make1(hashed_column);
	hash_call->agg_order		= NIL;
	hash_call->agg_filter		= NULL;
	hash_call->agg_within_group	= false;
	hash_call->agg_star			= false;
	hash_call->agg_distinct		= false;
	hash_call->func_variadic	= false;
	hash_call->over				= NULL;
	hash_call->location			= -1;

	/* Build schema-qualified name of function get_hash_part_idx() */
	get_hash_part_idx_proc =
			list_make2(makeString(get_namespace_name(get_pathman_schema())),
					   makeString("get_hash_part_idx"));

	/* Call get_hash_part_idx() */
	part_idx_call->funcname			= get_hash_part_idx_proc;
	part_idx_call->args				= list_make2(hash_call, part_count_c);
	part_idx_call->agg_order		= NIL;
	part_idx_call->agg_filter		= NULL;
	part_idx_call->agg_within_group	= false;
	part_idx_call->agg_star			= false;
	part_idx_call->agg_distinct		= false;
	part_idx_call->func_variadic	= false;
	part_idx_call->over				= NULL;
	part_idx_call->location			= -1;

	/* Construct equality operator */
	eq_oper->kind = AEXPR_OP;
	eq_oper->name = list_make1(makeString("="));
	eq_oper->lexpr = (Node *) part_idx_call;
	eq_oper->rexpr = (Node *) part_idx_c;
	eq_oper->location = -1;

	return (Node *) eq_oper;
}

/* Build complete HASH check constraint */
Constraint *
build_hash_check_constraint(Oid child_relid,
							char *attname,
							uint32 part_idx,
							uint32 part_count,
							Oid value_type)
{
	Constraint	   *hash_constr;
	char		   *hash_constr_name;
	AttrNumber		attnum;

	/* Build a correct name for this constraint */
	attnum = get_attnum(child_relid, attname);
	hash_constr_name = build_check_constraint_name_relid_internal(child_relid,
																  attnum);

	/* Initialize basic properties of a CHECK constraint */
	hash_constr = make_constraint_common(hash_constr_name,
										 build_raw_hash_check_tree(attname,
																   part_idx,
																   part_count,
																   value_type));
	/* Everything seems to be fine */
	return hash_constr;
}

static Constraint *
make_constraint_common(char *name, Node *raw_expr)
{
	Constraint *constraint;

	/* Initialize basic properties of a CHECK constraint */
	constraint = makeNode(Constraint);
	constraint->conname			= name;
	constraint->deferrable		= false;
	constraint->initdeferred	= false;
	constraint->location		= -1;
	constraint->contype			= CONSTR_CHECK;
	constraint->is_no_inherit	= false;

	/* Validate existing data using this constraint */
	constraint->skip_validation	= false;
	constraint->initially_valid	= true;

	/* Finally we should build an expression tree */
	constraint->raw_expr		= raw_expr;

	return constraint;
}

static Value
make_string_value_struct(char *str)
{
	Value val;

	val.type = T_String;
	val.val.str = str;

	return val;
}

static Value
make_int_value_struct(int int_val)
{
	Value val;

	val.type = T_Integer;
	val.val.ival = int_val;

	return val;
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

	Oid						parent_oid = cb_params->parent_relid;
	Oid						partition_oid = cb_params->partition_relid;

	FmgrInfo				cb_flinfo;
	FunctionCallInfoData	cb_fcinfo;

	JsonbParseState		   *jsonb_state = NULL;
	JsonbValue			   *result,
							key,
							val;

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
			cb_params->callback_is_cached = true;
		}
	}

	/* No callback is set, exit */
	if (!OidIsValid(cb_params->callback))
		return;

	/* Validate the callback's signature */
	validate_part_callback(cb_params->callback, true);

	/* Generate JSONB we're going to pass to callback */
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
				Datum	sv_datum	= cb_params->params.range_params.start_value,
						ev_datum	= cb_params->params.range_params.end_value;
				Oid		type		= cb_params->params.range_params.value_type;

				/* Convert min & max to CSTRING */
				start_value = datum_to_cstring(sv_datum, type);
				end_value = datum_to_cstring(ev_datum, type);

				pushJsonbValue(&jsonb_state, WJB_BEGIN_OBJECT, NULL);

				JSB_INIT_VAL(&key, WJB_KEY, "parent");
				JSB_INIT_VAL(&val, WJB_VALUE, get_rel_name_or_relid(parent_oid));
				JSB_INIT_VAL(&key, WJB_KEY, "partition");
				JSB_INIT_VAL(&val, WJB_VALUE, get_rel_name_or_relid(partition_oid));
				JSB_INIT_VAL(&key, WJB_KEY, "parttype");
				JSB_INIT_VAL(&val, WJB_VALUE, PartTypeToCString(PT_RANGE));
				JSB_INIT_VAL(&key, WJB_KEY, "range_min");
				JSB_INIT_VAL(&val, WJB_VALUE, start_value);
				JSB_INIT_VAL(&key, WJB_KEY, "range_max");
				JSB_INIT_VAL(&val, WJB_VALUE, end_value);

				result = pushJsonbValue(&jsonb_state, WJB_END_OBJECT, NULL);
			}
			break;

		default:
			elog(ERROR, "Unknown partitioning type %u", cb_params->parttype);
			break;
	}

	/* Fetch function call data */
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

/*
 * Checks that callback function meets specific requirements.
 * It must have the only JSONB argument and BOOL return type.
 */
bool
validate_part_callback(Oid procid, bool emit_error)
{
	HeapTuple		tp;
	Form_pg_proc	functup;
	bool			is_ok = true;

	if (procid == DEFAULT_INIT_CALLBACK)
		return true;

	tp = SearchSysCache1(PROCOID, ObjectIdGetDatum(procid));
	if (!HeapTupleIsValid(tp))
		elog(ERROR, "cache lookup failed for function %u", procid);

	functup = (Form_pg_proc) GETSTRUCT(tp);

	if (functup->pronargs != 1 ||
		functup->proargtypes.values[0] != JSONBOID ||
		functup->prorettype != VOIDOID)
		is_ok = false;

	ReleaseSysCache(tp);

	if (emit_error && !is_ok)
		elog(ERROR,
			 "Callback function must have the following signature: "
			 "callback(arg JSONB) RETURNS VOID");

	return is_ok;
}
