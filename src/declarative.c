#include "declarative.h"
#include "utils.h"

#include "fmgr.h"
#include "access/htup_details.h"
#include "catalog/namespace.h"
#include "catalog/pg_type.h"
#include "catalog/pg_proc.h"
#include "nodes/nodeFuncs.h"
#include "parser/parse_func.h"
#include "parser/parse_coerce.h"
#include "utils/int8.h"
#include "utils/lsyscache.h"
#include "utils/builtins.h"
#include "utils/int8.h"
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
modify_declative_partitioning_query(Query *query)
{
	if (query->commandType != CMD_UTILITY)
		return;

	if (IsA(query->utilityStmt, AlterTableStmt))
	{
		ListCell   *lcmd;
		Oid			relid;

		AlterTableStmt *stmt = (AlterTableStmt *) query->utilityStmt;
		relid = RangeVarGetRelid(stmt->relation, NoLock, true);
		if (get_pathman_relation_info(relid) != NULL)
		{
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
bool is_pathman_related_partitioning_cmd(Node *parsetree)
{
	if (IsA(parsetree, AlterTableStmt))
	{
		ListCell	   *lc;
		AlterTableStmt *stmt = (AlterTableStmt *) parsetree;
		int				cnt = 0;

		foreach(lc, stmt->cmds)
		{
			AlterTableCmd  *cmd = (AlterTableCmd *) lfirst(lc);
			int				subtype = cmd->subtype;

			if (subtype < 0)
				subtype = -subtype;

			switch (subtype)
			{
				case AT_AttachPartition:
				case AT_DetachPartition:
					/*
					 * we need to fix all subtypes,
					 * possibly we're not going to handle this
					 */
					cmd->subtype = -(cmd->subtype);
					continue;
				default:
					cnt++;
			}
		}

		return (cnt == 0);
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
void handle_attach_partition(AlterTableStmt *stmt, AlterTableCmd *cmd)
{
	Oid		parent_relid,
			partition_relid,
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
	const PartRelationInfo *prel;

	PartitionCmd	*pcmd	= (PartitionCmd *) cmd->def;

	Assert(cmd->subtype == AT_AttachPartition);

	parent_relid = RangeVarGetRelid(stmt->relation, NoLock, false);
	if ((prel = get_pathman_relation_info(parent_relid)) == NULL)
		elog(ERROR, "relation is not partitioned");

	partition_relid = RangeVarGetRelid(pcmd->name, NoLock, false);

	/* Fetch pg_pathman's schema */
	pathman_schema = get_namespace_name(get_pathman_schema());

	/* Build function's name */
	proc_name = list_make2(makeString(pathman_schema),
						   makeString(CppAsString(attach_range_partition)));

	ldatum = (PartitionRangeDatum *) linitial(pcmd->bound->lowerdatums);
	con = castNode(A_Const, ldatum->value);
	lval = transform_bound_value(pstate, con, prel->ev_type, prel->ev_typmod);

	rdatum = (PartitionRangeDatum *) linitial(pcmd->bound->upperdatums);
	con = castNode(A_Const, rdatum->value);
	rval = transform_bound_value(pstate, con, prel->ev_type, prel->ev_typmod);

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

	if ((!list_length(pcmd->bound->lowerdatums)) ||
		(!list_length(pcmd->bound->upperdatums)))
		elog(ERROR, "provide start and end value for range partition");

	proc_fcinfo.arg[2] = lval->constvalue;
	proc_fcinfo.argnull[2] = ldatum->infinite || lval->constisnull;
	proc_fcinfo.arg[3] = rval->constvalue;
	proc_fcinfo.argnull[3] = rdatum->infinite || rval->constisnull;

	/* Invoke the callback */
	FunctionCallInvoke(&proc_fcinfo);
}

/* handle ALTER TABLE .. DETACH PARTITION command */
void handle_detach_partition(AlterTableStmt *stmt, AlterTableCmd *cmd)
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
