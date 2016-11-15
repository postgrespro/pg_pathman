#include "pathman.h"
#include "init.h"
#include "partition_creation.h"

#include "access/reloptions.h"
#include "access/xact.h"
#include "catalog/heap.h"
#include "catalog/pg_type.h"
#include "catalog/toasting.h"
#include "commands/event_trigger.h"
#include "commands/sequence.h"
#include "commands/tablecmds.h"
#include "parser/parse_func.h"
#include "parser/parse_relation.h"
#include "parser/parse_utilcmd.h"
#include "tcop/utility.h"
#include "utils/builtins.h"
#include "utils/jsonb.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"


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
									   Datum start_value,
									   Datum end_value,
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
								start_value, end_value, value_type);
	invoke_part_callback(&callback_params);

	/* Return the Oid */
	return partition_relid;
}


/*
 * --------------------
 *  Partition creation
 * --------------------
 */

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

	/* Update config one more time */
	CommandCounterIncrement();

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
	left_const->val = *makeString(datum_to_cstring(start_value, value_type));
	left_const->location = -1;

	/* Right boundary */
	right_const->val = *makeString(datum_to_cstring(end_value, value_type));
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
