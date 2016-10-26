#include "pathman.h"
#include "init.h"
#include "partition_creation.h"
#include "relation_info.h"

#include "access/reloptions.h"
#include "access/xact.h"
#include "catalog/heap.h"
#include "catalog/toasting.h"
#include "commands/defrem.h"
#include "commands/event_trigger.h"
#include "commands/tablecmds.h"
#include "nodes/makefuncs.h"
#include "parser/parse_expr.h"
#include "parser/parse_node.h"
#include "parser/parse_relation.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"


/* TODO: comment */
Oid
create_single_range_partition(Oid parent_relid,
							  Datum start_value,
							  Datum end_value,
							  Oid value_type,
							  RangeVar *partition_rv,
							  char *tablespace)
{
	CreateStmt			create_stmt;
	ObjectAddress		partition_addr;
	Oid					child_relid;
	Relation			child_relation;
	Datum				toast_options;
	TableLikeClause		like_clause;
	Constraint		   *check_constr;
	RangeVar		   *parent_rv;
	Oid					parent_nsp;
	char			   *parent_name,
					   *parent_nsp_name,
						partitioned_column;
	Datum				config_values[Natts_pathman_config];
	bool				config_nulls[Natts_pathman_config];
	static char		   *validnsps[] = HEAP_RELOPT_NAMESPACES;

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
	like_clause.options			= CREATE_TABLE_LIKE_ALL;

	/* Initialize CreateStmt structure */
	NodeSetTag(&create_stmt, T_CreateStmt);
	create_stmt.relation		= copyObject(partition_rv);
	create_stmt.tableElts		= list_make1(&like_clause);
	create_stmt.inhRelations	= list_make1(copyObject(parent_rv));
	create_stmt.ofTypename		= NULL;
	create_stmt.constraints		= list_make1(&check_constr);
	create_stmt.options			= NIL;
	create_stmt.oncommit		= ONCOMMIT_NOOP;
	create_stmt.tablespacename	= tablespace;
	create_stmt.if_not_exists	= false;

	/* Create new partition owned by parent's posessor */
	partition_addr = DefineRelation(&create_stmt, RELKIND_RELATION,
									get_rel_owner(parent_relid), NULL);

	/* Save data about a simple DDL command that was just executed */
	EventTriggerCollectSimpleCommand(partition_addr,
									 InvalidObjectAddress,
									 (Node *) &create_stmt);

	/* Save partition's Oid */
	child_relid = partition_addr.objectId;

	/*
	 * Let NewRelationCreateToastTable decide if this
	 * one needs a secondary relation too.
	 */
	CommandCounterIncrement();

	/* Parse and validate reloptions for the toast table */
	toast_options = transformRelOptions((Datum) 0, create_stmt.options,
										"toast", validnsps, true, false);

	/* Parse options for a new toast table */
	(void) heap_reloptions(RELKIND_TOASTVALUE, toast_options, true);

	/* Now create the toast table if needed */
	NewRelationCreateToastTable(child_relid, toast_options);

	/* Update config one more time */
	CommandCounterIncrement();

	/* Fetch partitioned column's name */
	partitioned_column = config_values[Anum_pathman_config_attname - 1];

	/* Build check constraint for RANGE partition */
	check_constr = build_range_check_constraint(partitioned_column,
												start_value,
												end_value,
												value_type);

	/* Open the relation and add new check constraint */
	child_relation = heap_openrv(partition_rv, AccessExclusiveLock);
	AddRelationNewConstraints(child_relation, NIL,
							  list_make1(check_constr),
							  false, true, true);
	heap_close(child_relation, NoLock);

	/* Invoke init_callback on partition */
	invoke_init_callback(parent_relid, child_relid, InvalidOid,
						 start_value, end_value, value_type);

	return child_relid;
}

Node *
raw_range_check_tree(char *attname,
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

Node *
good_range_check_tree(RangeVar *partition,
					  char *attname,
					  Datum start_value,
					  Datum end_value,
					  Oid value_type)
{
	ParseState		   *pstate = make_parsestate(NULL);
	RangeTblEntry	   *partition_rte;
	Node			   *expression,
					   *raw_expression;
	ParseNamespaceItem	pni;

	/* Required for transformExpr() */
	partition_rte = addRangeTableEntry(pstate, partition, NULL, false, false);

	memset((void *) &pni, 0, sizeof(ParseNamespaceItem));
	pni.p_rte = partition_rte;
	pni.p_rel_visible = true;
	pni.p_cols_visible = true;

	pstate->p_namespace = list_make1(&pni);
	pstate->p_rtable = list_make1(partition_rte);

	/* Transform raw check constraint expression into Constraint */
	raw_expression = raw_range_check_tree(attname, start_value, end_value, value_type);
	expression = transformExpr(pstate, raw_expression, EXPR_KIND_CHECK_CONSTRAINT);

	return (Node *) expression;
}

Constraint *
build_range_check_constraint(char *attname,
							 Datum start_value,
							 Datum end_value,
							 Oid value_type)
{
	Constraint *range_constr;

	range_constr = makeNode(Constraint);
	range_constr->conname = NULL;
	range_constr->deferrable = false;
	range_constr->initdeferred = false;
	range_constr->location = -1;
	range_constr->contype = CONSTR_CHECK;
	range_constr->is_no_inherit = true;

	range_constr->raw_expr = raw_range_check_tree(attname,
												  start_value,
												  end_value,
												  value_type);

	return range_constr;
}

/* TODO: comment */
void
invoke_init_callback(Oid parent_relid,
					 Oid child_relid,
					 Oid init_callback,
					 Datum start_value,
					 Datum end_value,
					 Oid value_type)
{

}
