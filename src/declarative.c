#include "pathman.h"
#include "declarative.h"
#include "utils.h"
#include "partition_creation.h"

#include "access/htup_details.h"
#include "catalog/namespace.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_type.h"
#include "fmgr.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "optimizer/planner.h"
#include "parser/parse_coerce.h"
#include "parser/parse_func.h"
#include "utils/builtins.h"
#include "utils/int8.h"
#include "utils/int8.h"
#include "utils/lsyscache.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"
#include "utils/varbit.h"

/*
 * Modifies query of declarative partitioning commands,
 * There is a little hack here, ATTACH PARTITION command
 * expects relation with REL_PARTITIONED_TABLE relkind.
 * To avoid this check we negate subtype, and then after the checks
 * we set it back (look `is_pathman_related_partitioning_cmd`)
 */
void
modify_declarative_partitioning_query(Query *query)
{
	if (query->commandType != CMD_UTILITY)
		return;

	if (IsA(query->utilityStmt, AlterTableStmt))
	{
		PartRelationInfo	*prel;
		ListCell   *lcmd;
		Oid			relid;

		AlterTableStmt *stmt = (AlterTableStmt *) query->utilityStmt;
		relid = RangeVarGetRelid(stmt->relation, NoLock, true);
		if ((prel = get_pathman_relation_info(relid)) != NULL)
		{
			close_pathman_relation_info(prel);

			foreach(lcmd, stmt->cmds)
			{
				AlterTableCmd *cmd = (AlterTableCmd *) lfirst(lcmd);
				switch (cmd->subtype)
				{
					case AT_AttachPartition:
					case AT_DetachPartition:
						cmd->subtype = -cmd->subtype;
						break;
					default:
						break;
				}
			}
		}
	}
}

/* is it one of declarative partitioning commands? */
bool
is_pathman_related_partitioning_cmd(Node *parsetree, Oid *parent_relid)
{
	PartRelationInfo	*prel;

	if (IsA(parsetree, AlterTableStmt))
	{
		ListCell	   *lc;
		AlterTableStmt *stmt = (AlterTableStmt *) parsetree;
		int				cnt = 0;

		*parent_relid = RangeVarGetRelid(stmt->relation, NoLock, stmt->missing_ok);

		if (stmt->missing_ok && *parent_relid == InvalidOid)
			return false;

		if ((prel = get_pathman_relation_info(*parent_relid)) == NULL)
			return false;

		close_pathman_relation_info(prel);

		/*
		 * Since cmds can contain multiple commmands but we can handle only
		 * two of them here, so we need to check that there are only commands
		 * we can handle. In case if cmds contain other commands we skip all
		 * commands in this statement.
		 */
		foreach(lc, stmt->cmds)
		{
			AlterTableCmd  *cmd = (AlterTableCmd *) lfirst(lc);
			switch (abs(cmd->subtype))
			{
				case AT_AttachPartition:
				case AT_DetachPartition:
					/*
					 * We need to fix all subtypes,
					 * possibly we're not going to handle this
					 */
					cmd->subtype = abs(cmd->subtype);
					continue;
				default:
					cnt++;
			}
		}

		return (cnt == 0);
	}
	else if (IsA(parsetree, CreateStmt))
	{
		/* inhRelations != NULL, partbound != NULL, tableElts == NULL */
		CreateStmt	*stmt = (CreateStmt *) parsetree;

		if (stmt->inhRelations && stmt->partbound != NULL)
		{
			RangeVar *rv = castNode(RangeVar, linitial(stmt->inhRelations));
			*parent_relid = RangeVarGetRelid(rv, NoLock, false);
			if ((prel = get_pathman_relation_info(*parent_relid)) == NULL)
				return false;

			close_pathman_relation_info(prel);
			if (stmt->tableElts != NIL)
				elog(ERROR, "pg_pathman doesn't support column definitions "
						"in declarative syntax yet");

			return true;

		}
	}
	return false;
}

static FuncExpr *
make_fn_expr(Oid funcOid, List *args)
{
	FuncExpr	   *fn_expr;
	HeapTuple		procTup;
	Form_pg_proc	procStruct;

	procTup = SearchSysCache1(PROCOID, ObjectIdGetDatum(funcOid));
	if (!HeapTupleIsValid(procTup))
		elog(ERROR, "cache lookup failed for function %u", funcOid);
	procStruct = (Form_pg_proc) GETSTRUCT(procTup);

	fn_expr = makeFuncExpr(funcOid, procStruct->prorettype, args,
					 InvalidOid, InvalidOid, COERCE_EXPLICIT_CALL);
	ReleaseSysCache(procTup);
	return fn_expr;
}

/*
 * Transform one constant in a partition bound spec
 */
static Const *
transform_bound_value(ParseState *pstate, A_Const *con,
						Oid colType, int32 colTypmod)
{
	Node	   *value;

	/* Make it into a Const */
	value = (Node *) make_const(pstate, &con->val, con->location);

	/* Coerce to correct type */
	value = coerce_to_target_type(pstate,
								  value, exprType(value),
								  colType,
								  colTypmod,
								  COERCION_ASSIGNMENT,
								  COERCE_IMPLICIT_CAST,
								  -1);

	if (value == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_DATATYPE_MISMATCH),
		errmsg("specified value cannot be cast to type %s",
			   format_type_be(colType)),
				 parser_errposition(pstate, con->location)));

	/* Simplify the expression, in case we had a coercion */
	if (!IsA(value, Const))
		value = (Node *) expression_planner((Expr *) value);

	/* Fail if we don't have a constant (i.e., non-immutable coercion) */
	if (!IsA(value, Const))
		ereport(ERROR,
				(errcode(ERRCODE_DATATYPE_MISMATCH),
		errmsg("specified value cannot be cast to type %s",
			   format_type_be(colType)),
				 errdetail("The cast requires a non-immutable conversion."),
				 errhint("Try putting the literal value in single quotes."),
				 parser_errposition(pstate, con->location)));

	return (Const *) value;
}

/* handle ALTER TABLE .. ATTACH PARTITION command */
void
handle_attach_partition(Oid parent_relid, AlterTableCmd *cmd)
{
	Oid		partition_relid,
			proc_args[] = { REGCLASSOID, REGCLASSOID,
							ANYELEMENTOID, ANYELEMENTOID };

	List				   *proc_name;
	FmgrInfo				proc_flinfo;
	FunctionCallInfoData	proc_fcinfo;
	char				   *pathman_schema;
	PartitionRangeDatum	   *ldatum,
						   *rdatum;
	Const				   *lval,
						   *rval;
	A_Const				   *con;
	List				   *fn_args;
	ParseState			   *pstate = make_parsestate(NULL);
	PartRelationInfo	   *prel;

	PartitionCmd		*pcmd	= (PartitionCmd *) cmd->def;

	/* in 10beta1, PartitionCmd->bound is (Node *) */
	PartitionBoundSpec	*bound = (PartitionBoundSpec *) pcmd->bound;

	Assert(cmd->subtype == AT_AttachPartition);

	if (bound->strategy != PARTITION_STRATEGY_RANGE)
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
		errmsg("pg_pathman only supports queries for range partitions")));

	if ((prel = get_pathman_relation_info(parent_relid)) == NULL)
		elog(ERROR, "relation is not partitioned");

	partition_relid = RangeVarGetRelid(pcmd->name, NoLock, false);

	/* Fetch pg_pathman's schema */
	pathman_schema = get_namespace_name(get_pathman_schema());
	if (pathman_schema == NULL)
		elog(ERROR, "pg_pathman schema not initialized");

	/* Build function's name */
	proc_name = list_make2(makeString(pathman_schema),
						   makeString(CppAsString(attach_range_partition)));

	if ((!list_length(bound->lowerdatums)) ||
		(!list_length(bound->upperdatums)))
		elog(ERROR, "provide start and end value for range partition");

	ldatum = (PartitionRangeDatum *) linitial(bound->lowerdatums);
	con = castNode(A_Const, ldatum->value);
	lval = transform_bound_value(pstate, con, prel->ev_type, prel->ev_typmod);

	rdatum = (PartitionRangeDatum *) linitial(bound->upperdatums);
	con = castNode(A_Const, rdatum->value);
	rval = transform_bound_value(pstate, con, prel->ev_type, prel->ev_typmod);
	close_pathman_relation_info(prel);

	/* Lookup function's Oid and get FmgrInfo */
	fmgr_info(LookupFuncName(proc_name, 4, proc_args, false), &proc_flinfo);

	InitFunctionCallInfoData(proc_fcinfo, &proc_flinfo,
							 4, InvalidOid, NULL, NULL);
	proc_fcinfo.arg[0] = ObjectIdGetDatum(parent_relid);
	proc_fcinfo.argnull[0] = false;
	proc_fcinfo.arg[1] = ObjectIdGetDatum(partition_relid);
	proc_fcinfo.argnull[1] = false;

	/* Make function expression, we will need it to determine argument types */
	fn_args = list_make4(NULL, NULL, lval, rval);
	proc_fcinfo.flinfo->fn_expr =
		(Node *) make_fn_expr(proc_fcinfo.flinfo->fn_oid, fn_args);

	proc_fcinfo.arg[2] = lval->constvalue;
	proc_fcinfo.argnull[2] = lval->constisnull;
	proc_fcinfo.arg[3] = rval->constvalue;
	proc_fcinfo.argnull[3] = rval->constisnull;

	/* Invoke the callback */
	FunctionCallInvoke(&proc_fcinfo);
}

/* handle ALTER TABLE .. DETACH PARTITION command */
void
handle_detach_partition(AlterTableCmd *cmd)
{
	List				   *proc_name;
	FmgrInfo				proc_flinfo;
	FunctionCallInfoData	proc_fcinfo;
	char				   *pathman_schema;
	Oid						partition_relid,
							args	= REGCLASSOID;
	PartitionCmd			*pcmd	= (PartitionCmd *) cmd->def;

	Assert(cmd->subtype == AT_DetachPartition);
	partition_relid = RangeVarGetRelid(pcmd->name, NoLock, false);

	/* Fetch pg_pathman's schema */
	pathman_schema = get_namespace_name(get_pathman_schema());
	if (pathman_schema == NULL)
		elog(ERROR, "pg_pathman schema not initialized");

	/* Build function's name */
	proc_name = list_make2(makeString(pathman_schema),
						   makeString(CppAsString(detach_range_partition)));

	/* Lookup function's Oid and get FmgrInfo */
	fmgr_info(LookupFuncName(proc_name, 1, &args, false), &proc_flinfo);

	InitFunctionCallInfoData(proc_fcinfo, &proc_flinfo,
							 4, InvalidOid, NULL, NULL);
	proc_fcinfo.arg[0] = ObjectIdGetDatum(partition_relid);
	proc_fcinfo.argnull[0] = false;

	/* Invoke the callback */
	FunctionCallInvoke(&proc_fcinfo);
}

/* handle CREATE TABLE .. PARTITION OF <parent> FOR VALUES FROM .. TO .. */
void
handle_create_partition_of(Oid parent_relid, CreateStmt *stmt)
{
	Bound					start,
							end;
	PartRelationInfo	   *prel;
	ParseState			   *pstate = make_parsestate(NULL);
	PartitionRangeDatum	   *ldatum,
						   *rdatum;
	Const				   *lval,
						   *rval;
	A_Const				   *con;

	/* in 10beta1, PartitionCmd->bound is (Node *) */
	PartitionBoundSpec	*bound = (PartitionBoundSpec *) stmt->partbound;

	/* we show errors earlier for these asserts */
	Assert(stmt->inhRelations != NULL);
	Assert(stmt->tableElts == NIL);

	if (bound->strategy != PARTITION_STRATEGY_RANGE)
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
		errmsg("pg_pathman only supports queries for range partitions")));

	if ((prel = get_pathman_relation_info(parent_relid)) == NULL)
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("table \"%s\" is not partitioned",
								get_rel_name_or_relid(parent_relid))));

	if (prel->parttype != PT_RANGE)
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("table \"%s\" is not partitioned by RANGE",
								get_rel_name_or_relid(parent_relid))));

	ldatum = (PartitionRangeDatum *) linitial(bound->lowerdatums);
	con = castNode(A_Const, ldatum->value);
	lval = transform_bound_value(pstate, con, prel->ev_type, prel->ev_typmod);

	rdatum = (PartitionRangeDatum *) linitial(bound->upperdatums);
	con = castNode(A_Const, rdatum->value);
	rval = transform_bound_value(pstate, con, prel->ev_type, prel->ev_typmod);
	close_pathman_relation_info(prel);

	start = lval->constisnull?
		MakeBoundInf(MINUS_INFINITY) :
		MakeBound(lval->constvalue);

	end = rval->constisnull?
		MakeBoundInf(PLUS_INFINITY) :
		MakeBound(rval->constvalue);

	/* more checks */
	check_range_available(parent_relid, &start, &end, lval->consttype, true);

	/* Create a new RANGE partition and return its Oid */
	create_single_range_partition_internal(parent_relid,
										   &start,
										   &end,
										   lval->consttype,
										   stmt->relation,
										   stmt->tablespacename);
}
