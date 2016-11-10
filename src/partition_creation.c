#include "pathman.h"
#include "init.h"
#include "partition_creation.h"

#include "access/reloptions.h"
#include "access/xact.h"
#include "catalog/heap.h"
#include "catalog/toasting.h"
#include "commands/defrem.h"
#include "commands/event_trigger.h"
#include "commands/tablecmds.h"
#include "parser/parse_relation.h"
#include "parser/parse_utilcmd.h"
#include "tcop/utility.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"


static Oid create_single_partition(Oid parent_relid,
								   RangeVar *partition_rv,
								   char *tablespace,
								   char **partitioned_column);

static ObjectAddress create_table_using_stmt(CreateStmt *create_stmt,
											 Oid relowner);


/* Create one RANGE partition [start_value, end_value) */
Oid
create_single_range_partition_internal(Oid parent_relid,
									   Datum start_value,
									   Datum end_value,
									   Oid value_type,
									   RangeVar *partition_rv,
									   char *tablespace)
{
	Oid				partition;
	Relation		child_relation;
	Constraint	   *check_constr;
	char		   *partitioned_column;

	/* Create a partition & get 'partitioned_column' */
	partition = create_single_partition(parent_relid,
										partition_rv,
										tablespace,
										&partitioned_column); /* get it */

	/* Build check constraint for RANGE partition */
	check_constr = build_range_check_constraint(partition,
												partitioned_column,
												start_value,
												end_value,
												value_type);

	/* Open the relation and add new check constraint */
	child_relation = heap_open(partition, AccessExclusiveLock);
	AddRelationNewConstraints(child_relation, NIL,
							  list_make1(check_constr),
							  false, true, true);
	heap_close(child_relation, NoLock);

	/* Return the Oid */
	return partition;
}

/* Create a partition-like table (no constraints yet) */
static Oid
create_single_partition(Oid parent_relid,
						RangeVar *partition_rv,
						char *tablespace,
						char **partitioned_column) /* to be set */
{
	/* Value to be returned */
	Oid					child_relid = InvalidOid; /* safety */

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
		part_name = ChooseRelationName(parent_name, NULL, "part", parent_nsp);

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
			child_relid = create_table_using_stmt((CreateStmt *) cur_stmt,
												  child_relowner).objectId;
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
						   "have to provide query string",
						   PROCESS_UTILITY_SUBCOMMAND,
						   NULL,
						   None_Receiver,
						   NULL);
		}
	}

	return child_relid;
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

/* Invoke 'init_callback' for a partition */
void
invoke_init_callback(Oid parent_relid,
					 Oid child_relid,
					 PartType part_type,
					 Datum start_value,
					 Datum end_value,
					 Oid value_type)
{
	/* TODO: implement callback invocation machinery */
}
