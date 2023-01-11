/* ------------------------------------------------------------------------
 *
 * utils.c
 *		definitions of various support functions
 *
 * Copyright (c) 2016, Postgres Professional
 * Portions Copyright (c) 1996-2016, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * ------------------------------------------------------------------------
 */

#include "pathman.h"
#include "utils.h"

#include "access/htup_details.h"
#include "access/nbtree.h"
#include "access/sysattr.h"
#include "catalog/namespace.h"
#include "catalog/pg_class.h"
#include "catalog/pg_operator.h"
#include "catalog/pg_type.h"
#include "miscadmin.h"
#include "nodes/nodeFuncs.h"
#include "parser/parse_coerce.h"
#include "parser/parse_oper.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/datetime.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"
#include "utils/typcache.h"

#if PG_VERSION_NUM >= 100000
#include "utils/regproc.h"
#endif

static const Node *
drop_irrelevant_expr_wrappers(const Node *expr)
{
	switch (nodeTag(expr))
	{
		/* Strip relabeling */
		case T_RelabelType:
			return (const Node *) ((const RelabelType *) expr)->arg;

		/* no special actions required */
		default:
			return expr;
	}
}

static bool
clause_contains_params_walker(Node *node, void *context)
{
	if (node == NULL)
		return false;

	if (IsA(node, Param))
		return true;

	return expression_tree_walker(node,
								  clause_contains_params_walker,
								  context);
}

/*
 * Check whether clause contains PARAMs or not.
 */
bool
clause_contains_params(Node *clause)
{
	return expression_tree_walker(clause,
								  clause_contains_params_walker,
								  NULL);
}

/*
 * Check if this is a "date"-related type.
 */
bool
is_date_type_internal(Oid typid)
{
	return typid == TIMESTAMPOID ||
		   typid == TIMESTAMPTZOID ||
		   typid == DATEOID;
}

/*
 * Check if user can alter/drop specified relation. This function is used to
 * make sure that current user can change pg_pathman's config. Returns true
 * if user can manage relation, false otherwise.
 *
 * XXX currently we just check if user is a table owner. Probably it's
 * better to check user permissions in order to let other users participate.
 */
bool
check_security_policy_internal(Oid relid, Oid role)
{
	Oid owner;

	/* Superuser is allowed to do anything */
	if (superuser())
		return true;

	/* Fetch the owner */
	owner = get_rel_owner(relid);

	/*
	 * Sometimes the relation doesn't exist anymore but there is still
	 * a record in config. For instance, it happens in DDL event trigger.
	 * Still we should be able to remove this record.
	 */
	if (owner == InvalidOid)
		return true;

	/* Check if current user is the owner of the relation */
	if (owner != role)
		return false;

	return true;
}

/* Compare clause operand with expression */
bool
match_expr_to_operand(const Node *expr, const Node *operand)
{
	expr = drop_irrelevant_expr_wrappers(expr);
	operand = drop_irrelevant_expr_wrappers(operand);

	/* compare expressions and return result right away */
	return equal(expr, operand);
}


List *
list_reverse(List *l)
{
	List *result = NIL;
	ListCell *lc;

	foreach (lc, l)
	{
		result = lcons(lfirst(lc), result);
	}
	return result;
}


/*
 * Get relation owner.
 */
Oid
get_rel_owner(Oid relid)
{
	HeapTuple	tp;
	Oid 		owner;

	tp = SearchSysCache1(RELOID, ObjectIdGetDatum(relid));
	if (HeapTupleIsValid(tp))
	{
		Form_pg_class reltup = (Form_pg_class) GETSTRUCT(tp);

		owner = reltup->relowner;
		ReleaseSysCache(tp);

		return owner;
	}

	return InvalidOid;
}

/*
 * Try to get relname or at least relid as cstring.
 */
char *
get_rel_name_or_relid(Oid relid)
{
	char *relname = get_rel_name(relid);

	if (!relname)
		return DatumGetCString(DirectFunctionCall1(oidout, ObjectIdGetDatum(relid)));

	return relname;
}

/*
 * Return palloced fully qualified relation name as a cstring
 */
char *
get_qualified_rel_name(Oid relid)
{
	Oid nspid = get_rel_namespace(relid);

	return psprintf("%s.%s",
					quote_identifier(get_namespace_name(nspid)),
					quote_identifier(get_rel_name(relid)));
}

RangeVar *
makeRangeVarFromRelid(Oid relid)
{
	char *relname = get_rel_name(relid);
	char *nspname = get_namespace_name(get_rel_namespace(relid));

	return makeRangeVar(nspname, relname, -1);
}



/*
 * Try to find binary operator.
 *
 * Returns operator function's Oid or throws an ERROR on InvalidOid.
 */
Operator
get_binary_operator(char *oprname, Oid arg1, Oid arg2)
{
	Operator op;

	op = compatible_oper(NULL, list_make1(makeString(oprname)),
						 arg1, arg2, true, -1);

	if (!op)
		elog(ERROR, "cannot find operator %s(%s, %s)",
			 oprname,
			 format_type_be(arg1),
			 format_type_be(arg2));

	return op;
}

/*
 * Get BTORDER_PROC for two types described by Oids.
 */
void
fill_type_cmp_fmgr_info(FmgrInfo *finfo, Oid type1, Oid type2)
{
	Oid				cmp_proc_oid;
	TypeCacheEntry *tce_1,
				   *tce_2;

	/* Check type compatibility */
	if (IsBinaryCoercible(type1, type2))
		type1 = type2;

	else if (IsBinaryCoercible(type2, type1))
		type2 = type1;

	tce_1 = lookup_type_cache(type1, TYPECACHE_BTREE_OPFAMILY);
	tce_2 = lookup_type_cache(type2, TYPECACHE_BTREE_OPFAMILY);

	/* Both types should belong to the same opfamily */
	if (tce_1->btree_opf != tce_2->btree_opf)
		goto fill_type_cmp_fmgr_info_error;

	cmp_proc_oid = get_opfamily_proc(tce_1->btree_opf,
									 tce_1->btree_opintype,
									 tce_2->btree_opintype,
									 BTORDER_PROC);

	/* No such function, emit ERROR */
	if (!OidIsValid(cmp_proc_oid))
		goto fill_type_cmp_fmgr_info_error;

	/* Fill FmgrInfo struct */
	fmgr_info(cmp_proc_oid, finfo);

	return; /* everything is OK */

/* Handle errors (no such function) */
fill_type_cmp_fmgr_info_error:
	elog(ERROR, "missing comparison function for types %s & %s",
		 format_type_be(type1), format_type_be(type2));
}

/*
 * Fetch binary operator by name and return it's function and ret type.
 */
void
extract_op_func_and_ret_type(char *opname,
							 Oid type1, Oid type2,
							 Oid *op_func,		/* ret value #1 */
							 Oid *op_ret_type)	/* ret value #2 */
{
	Operator op;

	/* Get "move bound operator" descriptor */
	op = get_binary_operator(opname, type1, type2);
	Assert(op);

	*op_func = oprfuncid(op);
	*op_ret_type = ((Form_pg_operator) GETSTRUCT(op))->oprresult;

	/* Don't forget to release system cache */
	ReleaseSysCache(op);
}



/*
 * Get CSTRING representation of Datum using the type Oid.
 */
char *
datum_to_cstring(Datum datum, Oid typid)
{
	char	   *result;
	HeapTuple	tup = SearchSysCache1(TYPEOID, ObjectIdGetDatum(typid));

	if (HeapTupleIsValid(tup))
	{
		Form_pg_type typtup = (Form_pg_type) GETSTRUCT(tup);
		result = OidOutputFunctionCall(typtup->typoutput, datum);
		ReleaseSysCache(tup);
	}
	else
		result = pstrdup("[error]");

	return result;
}



/*
 * Try casting value of type 'in_type' to 'out_type'.
 *
 * This function might emit ERROR.
 */
Datum
perform_type_cast(Datum value, Oid in_type, Oid out_type, bool *success)
{
	CoercionPathType	ret;
	Oid					castfunc = InvalidOid;

	/* Speculative success */
	if (success) *success = true;

	/* Fast and trivial path */
	if (in_type == out_type)
		return value;

	/* Check that types are binary coercible */
	if (IsBinaryCoercible(in_type, out_type))
		return value;

	/* If not, try to perform a type cast */
	ret = find_coercion_pathway(out_type, in_type,
								COERCION_EXPLICIT,
								&castfunc);

	/* Handle coercion paths */
	switch (ret)
	{
		/* There's a function */
		case COERCION_PATH_FUNC:
			{
				/* Perform conversion */
				Assert(castfunc != InvalidOid);
				return OidFunctionCall1(castfunc, value);
			}

		/* Types are binary compatible (no implicit cast) */
		case COERCION_PATH_RELABELTYPE:
			{
				/* We don't perform any checks here */
				return value;
			}

		/* TODO: implement these casts if needed */
		case COERCION_PATH_ARRAYCOERCE:
		case COERCION_PATH_COERCEVIAIO:

		/* There's no cast available */
		case COERCION_PATH_NONE:
		default:
			{
				/* Oops, something is wrong */
				if (success)
					*success = false;
				else
					elog(ERROR, "cannot cast %s to %s",
						 format_type_be(in_type),
						 format_type_be(out_type));

				return (Datum) 0;
			}
	}
}

/*
 * Convert interval from TEXT to binary form using partitioninig expression type.
 */
Datum
extract_binary_interval_from_text(Datum interval_text,	/* interval as TEXT */
								  Oid part_atttype,		/* expression type */
								  Oid *interval_type)	/* ret value #1 */
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
			elog(ERROR, "cannot find input function for type %u", part_atttype);

		/*
		 * Convert interval from CSTRING to 'prel->ev_type'.
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

/* Convert Datum into CSTRING array */
char **
deconstruct_text_array(Datum array, int *array_size)
{
	ArrayType  *array_ptr = DatumGetArrayTypeP(array);
	int16		elemlen;
	bool		elembyval;
	char		elemalign;

	Datum	   *elem_values;
	bool	   *elem_nulls;

	int			arr_size = 0;

	/* Check type invariant */
	Assert(ARR_ELEMTYPE(array_ptr) == TEXTOID);

	/* Check number of dimensions */
	if (ARR_NDIM(array_ptr) > 1)
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("array should contain only 1 dimension")));

	get_typlenbyvalalign(ARR_ELEMTYPE(array_ptr),
						 &elemlen, &elembyval, &elemalign);

	deconstruct_array(array_ptr,
					  ARR_ELEMTYPE(array_ptr),
					  elemlen, elembyval, elemalign,
					  &elem_values, &elem_nulls, &arr_size);

	/* If there are actual values, convert them into CSTRINGs */
	if (arr_size > 0)
	{
		char  **strings = palloc(arr_size * sizeof(char *));
		int		i;

		for (i = 0; i < arr_size; i++)
		{
			if (elem_nulls[i])
				ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
								errmsg("array should not contain NULLs")));

			strings[i] = TextDatumGetCString(elem_values[i]);
		}

		/* Return an array and it's size */
		*array_size = arr_size;
		return strings;
	}
	/* Else emit ERROR */
	else ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("array should not be empty")));

	/* Keep compiler happy */
	return NULL;
}

/*
 * Convert schema qualified relation names array to RangeVars array
 */
RangeVar **
qualified_relnames_to_rangevars(char **relnames, size_t nrelnames)
{
	RangeVar  **rangevars = NULL;
	int			i;

	/* Convert partition names into RangeVars */
	if (relnames)
	{
		rangevars = palloc(sizeof(RangeVar *) * nrelnames);
		for (i = 0; i < nrelnames; i++)
		{
			List *nl = stringToQualifiedNameListCompat(relnames[i]);

			rangevars[i] = makeRangeVarFromNameList(nl);
		}
	}

	return rangevars;
}

