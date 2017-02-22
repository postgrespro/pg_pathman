/* ------------------------------------------------------------------------
 *
 * utils.c
 *		definitions of various support functions
 *
 * Copyright (c) 2016, Postgres Professional
 *
 * ------------------------------------------------------------------------
 */

#include "utils.h"

#include "access/htup_details.h"
#include "access/nbtree.h"
#include "access/sysattr.h"
#include "access/xact.h"
#include "catalog/heap.h"
#include "catalog/pg_type.h"
#include "catalog/pg_extension.h"
#include "catalog/pg_operator.h"
#include "catalog/pg_inherits.h"
#include "commands/extension.h"
#include "miscadmin.h"
#include "optimizer/var.h"
#include "parser/parse_coerce.h"
#include "parser/parse_oper.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"
#include "utils/typcache.h"


static bool clause_contains_params_walker(Node *node, void *context);


/*
 * Check whether clause contains PARAMs or not
 */
bool
clause_contains_params(Node *clause)
{
	return expression_tree_walker(clause,
								  clause_contains_params_walker,
								  NULL);
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



/*
 * Return pg_pathman schema's Oid or InvalidOid if that's not possible.
 */
Oid
get_pathman_schema(void)
{
	Oid				result;
	Relation		rel;
	SysScanDesc		scandesc;
	HeapTuple		tuple;
	ScanKeyData		entry[1];
	Oid				ext_schema;

	/* It's impossible to fetch pg_pathman's schema now */
	if (!IsTransactionState())
		return InvalidOid;

	ext_schema = get_extension_oid("pg_pathman", true);
	if (ext_schema == InvalidOid)
		return InvalidOid; /* exit if pg_pathman does not exist */

	ScanKeyInit(&entry[0],
				ObjectIdAttributeNumber,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(ext_schema));

	rel = heap_open(ExtensionRelationId, AccessShareLock);
	scandesc = systable_beginscan(rel, ExtensionOidIndexId, true,
								  NULL, 1, entry);

	tuple = systable_getnext(scandesc);

	/* We assume that there can be at most one matching tuple */
	if (HeapTupleIsValid(tuple))
		result = ((Form_pg_extension) GETSTRUCT(tuple))->extnamespace;
	else
		result = InvalidOid;

	systable_endscan(scandesc);

	heap_close(rel, AccessShareLock);

	return result;
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
		return DatumGetCString(DirectFunctionCall1(oidout,
												   ObjectIdGetDatum(relid)));
	return relname;
}

/*
 * Get type of column by its name.
 */
Oid
get_attribute_type(Oid relid, const char *attname, bool missing_ok)
{
	Oid			result;
	HeapTuple	tp;

	/* NOTE: for now it's the most efficient way */
	tp = SearchSysCacheAttName(relid, attname);
	if (HeapTupleIsValid(tp))
	{
		Form_pg_attribute att_tup = (Form_pg_attribute) GETSTRUCT(tp);
		result = att_tup->atttypid;
		ReleaseSysCache(tp);

		return result;
	}

	if (!missing_ok)
		elog(ERROR, "cannot find type name for attribute \"%s\" "
					"of relation \"%s\"",
			 attname, get_rel_name_or_relid(relid));

	return InvalidOid;
}

#if PG_VERSION_NUM < 90600
/*
 * Returns the relpersistence associated with a given relation.
 *
 * NOTE: this function is implemented in 9.6
 */
char
get_rel_persistence(Oid relid)
{
	HeapTuple		tp;
	Form_pg_class	reltup;
	char 			result;

	tp = SearchSysCache1(RELOID, ObjectIdGetDatum(relid));
	if (!HeapTupleIsValid(tp))
		elog(ERROR, "cache lookup failed for relation %u", relid);

	reltup = (Form_pg_class) GETSTRUCT(tp);
	result = reltup->relpersistence;
	ReleaseSysCache(tp);

	return result;
}
#endif



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
		elog(ERROR, "Cannot find operator \"%s\"(%u, %u)", oprname, arg1, arg2);

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
							 Oid *op_func,		/* returned value #1 */
							 Oid *op_ret_type)	/* returned value #2 */
{
	Operator op;

	/* Get "move bound operator" descriptor */
	op = get_binary_operator(opname, type1, type2);
	if (!op)
		elog(ERROR, "missing %s operator for types %s and %s",
			 opname, format_type_be(type1), format_type_be(type2));

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

	/* If not, try to perfrom a type cast */
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
 * Convert interval from TEXT to binary form using partitioned column's type.
 */
Datum
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
			elog(ERROR, "cannot find input function for type %u", part_atttype);

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
