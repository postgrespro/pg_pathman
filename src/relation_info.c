/* ------------------------------------------------------------------------
 *
 * relation_info.c
 *		Data structures describing partitioned relations
 *
 * Copyright (c) 2016, Postgres Professional
 *
 * ------------------------------------------------------------------------
 */

#include "compat/pg_compat.h"

#include "relation_info.h"
#include "init.h"
#include "utils.h"
#include "xact_handling.h"

#include "access/htup_details.h"
#include "access/xact.h"
#include "catalog/catalog.h"
#include "catalog/indexing.h"
#include "catalog/pg_constraint.h"
#include "catalog/pg_inherits.h"
#include "catalog/pg_type.h"
#include "miscadmin.h"
#include "nodes/nodeFuncs.h"
#include "optimizer/clauses.h"
#include "optimizer/var.h"
#include "parser/analyze.h"
#include "parser/parser.h"
#include "storage/lmgr.h"
#include "tcop/tcopprot.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/hsearch.h"
#include "utils/memutils.h"
#include "utils/ruleutils.h"
#include "utils/syscache.h"
#include "utils/lsyscache.h"
#include "utils/typcache.h"

#if PG_VERSION_NUM < 90600
#include "optimizer/planmain.h"
#endif

#if PG_VERSION_NUM >= 90600
#include "catalog/pg_constraint_fn.h"
#endif


/* Error messages for partitioning expression */
#define PARSE_PART_EXPR_ERROR	"failed to parse partitioning expression \"%s\""
#define COOK_PART_EXPR_ERROR	"failed to analyze partitioning expression \"%s\""


/* Comparison function info */
typedef struct cmp_func_info
{
	FmgrInfo	flinfo;
	Oid			collid;
} cmp_func_info;

/*
 * For pg_pathman.enable_bounds_cache GUC.
 */
bool			pg_pathman_enable_bounds_cache = true;


/*
 * We delay all invalidation jobs received in relcache hook.
 */
static List	   *delayed_invalidation_parent_rels = NIL;
static List	   *delayed_invalidation_vague_rels = NIL;
static bool		delayed_shutdown = false; /* pathman was dropped */


/* Add unique Oid to list, allocate in TopPathmanContext */
#define list_add_unique(list, oid) \
	do { \
		MemoryContext old_mcxt = MemoryContextSwitchTo(TopPathmanContext); \
		list = list_append_unique_oid(list, ObjectIdGetDatum(oid)); \
		MemoryContextSwitchTo(old_mcxt); \
	} while (0)

#define free_invalidation_list(list) \
	do { \
		list_free(list); \
		list = NIL; \
	} while (0)

/* Handy wrappers for Oids */
#define bsearch_oid(key, array, array_size) \
	bsearch((const void *) &(key), (array), (array_size), sizeof(Oid), oid_cmp)


static bool try_invalidate_parent(Oid relid, Oid *parents, int parents_count);
static Oid try_catalog_parent_search(Oid partition, PartParentSearch *status);
static Oid get_parent_of_partition_internal(Oid partition,
											PartParentSearch *status,
											HASHACTION action);

static Expr *get_partition_constraint_expr(Oid partition);

static void fill_prel_with_partitions(PartRelationInfo *prel,
									  const Oid *partitions,
									  const uint32 parts_count);

static void fill_pbin_with_bounds(PartBoundInfo *pbin,
								  const PartRelationInfo *prel,
								  const Expr *constraint_expr);

static int cmp_range_entries(const void *p1, const void *p2, void *arg);

static bool query_contains_subqueries(Node *node, void *context);


void
init_relation_info_static_data(void)
{
	DefineCustomBoolVariable("pg_pathman.enable_bounds_cache",
							 "Make updates of partition dispatch cache faster",
							 NULL,
							 &pg_pathman_enable_bounds_cache,
							 true,
							 PGC_USERSET,
							 0,
							 NULL,
							 NULL,
							 NULL);
}

/*
 * refresh\invalidate\get\remove PartRelationInfo functions.
 */

const PartRelationInfo *
refresh_pathman_relation_info(Oid relid,
							  Datum *values,
							  bool allow_incomplete)
{
	const LOCKMODE			lockmode = AccessShareLock;
	const TypeCacheEntry   *typcache;
	Oid					   *prel_children;
	uint32					prel_children_count = 0,
							i;
	PartRelationInfo	   *prel;
	Datum					param_values[Natts_pathman_config_params];
	bool					param_isnull[Natts_pathman_config_params];
	char				   *expr;
	MemoryContext			old_mcxt;

	AssertTemporaryContext();
	prel = invalidate_pathman_relation_info(relid, NULL);
	Assert(prel);

	/* Try locking parent, exit fast if 'allow_incomplete' */
	if (allow_incomplete)
	{
		if (!ConditionalLockRelationOid(relid, lockmode))
			return NULL; /* leave an invalid entry */
	}
	else LockRelationOid(relid, lockmode);

	/* Check if parent exists */
	if (!SearchSysCacheExists1(RELOID, ObjectIdGetDatum(relid)))
	{
		/* Nope, it doesn't, remove this entry and exit */
		UnlockRelationOid(relid, lockmode);
		remove_pathman_relation_info(relid);
		return NULL; /* exit */
	}

	/* Make both arrays point to NULL */
	prel->children	= NULL;
	prel->ranges	= NULL;

	/* Set partitioning type */
	prel->parttype	= DatumGetPartType(values[Anum_pathman_config_parttype - 1]);

	/* Fetch cooked partitioning expression */
	expr = TextDatumGetCString(values[Anum_pathman_config_cooked_expr - 1]);

	/* Create a new memory context to store expression tree etc */
	prel->mcxt = AllocSetContextCreate(PathmanRelationCacheContext,
									   CppAsString(refresh_pathman_relation_info),
									   ALLOCSET_SMALL_SIZES);

	/* Switch to persistent memory context */
	old_mcxt = MemoryContextSwitchTo(prel->mcxt);

	/* Build partitioning expression tree */
	prel->expr_cstr = TextDatumGetCString(values[Anum_pathman_config_expr - 1]);
	prel->expr = (Node *) stringToNode(expr);
	fix_opfuncids(prel->expr);

	/* Extract Vars and varattnos of partitioning expression */
	prel->expr_vars = NIL;
	prel->expr_atts = NULL;
	prel->expr_vars = pull_var_clause_compat(prel->expr, 0, 0);
	pull_varattnos((Node *) prel->expr_vars, PART_EXPR_VARNO, &prel->expr_atts);

	MemoryContextSwitchTo(old_mcxt);

	/* First, fetch type of partitioning expression */
	prel->ev_type	= exprType(prel->expr);
	prel->ev_typmod	= exprTypmod(prel->expr);
	prel->ev_collid = exprCollation(prel->expr);

	/* Fetch HASH & CMP fuctions and other stuff from type cache */
	typcache = lookup_type_cache(prel->ev_type,
								 TYPECACHE_CMP_PROC | TYPECACHE_HASH_PROC);

	prel->ev_byval	= typcache->typbyval;
	prel->ev_len	= typcache->typlen;
	prel->ev_align	= typcache->typalign;

	prel->cmp_proc	= typcache->cmp_proc;
	prel->hash_proc	= typcache->hash_proc;

	/* Try searching for children (don't wait if we can't lock) */
	switch (find_inheritance_children_array(relid, lockmode,
											allow_incomplete,
											&prel_children_count,
											&prel_children))
	{
		/* If there's no children at all, remove this entry */
		case FCS_NO_CHILDREN:
			elog(DEBUG2, "refresh: relation %u has no children [%u]",
						 relid, MyProcPid);

			UnlockRelationOid(relid, lockmode);
			remove_pathman_relation_info(relid);
			return NULL; /* exit */

		/* If can't lock children, leave an invalid entry */
		case FCS_COULD_NOT_LOCK:
			elog(DEBUG2, "refresh: cannot lock children of relation %u [%u]",
						 relid, MyProcPid);

			UnlockRelationOid(relid, lockmode);
			return NULL; /* exit */

		/* Found some children, just unlock parent */
		case FCS_FOUND:
			elog(DEBUG2, "refresh: found children of relation %u [%u]",
						 relid, MyProcPid);

			UnlockRelationOid(relid, lockmode);
			break; /* continue */

		/* Error: unknown result code */
		default:
			elog(ERROR, "error in function "
						CppAsString(find_inheritance_children_array));
	}

	/*
	 * Fill 'prel' with partition info, raise ERROR if anything is wrong.
	 * This way PartRelationInfo will remain 'invalid', and 'get' procedure
	 * will try to refresh it again (and again), until the error is fixed
	 * by user manually (i.e. invalid check constraints etc).
	 */
	PG_TRY();
	{
		fill_prel_with_partitions(prel, prel_children, prel_children_count);
	}
	PG_CATCH();
	{
		/* Remove this parent from parents cache */
		ForgetParent(prel);

		/* Delete unused 'prel_mcxt' */
		MemoryContextDelete(prel->mcxt);

		prel->children	= NULL;
		prel->ranges	= NULL;
		prel->mcxt		= NULL;

		/* Rethrow ERROR further */
		PG_RE_THROW();
	}
	PG_END_TRY();

	/* Peform some actions for each child */
	for (i = 0; i < prel_children_count; i++)
	{
		/* Add "partition+parent" pair to cache */
		cache_parent_of_partition(prel_children[i], relid);

		/* Now it's time to unlock this child */
		UnlockRelationOid(prel_children[i], lockmode);
	}

	if (prel_children)
		pfree(prel_children);

	/* Read additional parameters ('enable_parent' at the moment) */
	if (read_pathman_params(relid, param_values, param_isnull))
	{
		prel->enable_parent = param_values[Anum_pathman_config_params_enable_parent - 1];
	}
	/* Else set default values if they cannot be found */
	else
	{
		prel->enable_parent = DEFAULT_PATHMAN_ENABLE_PARENT;
	}

	/* We've successfully built a cache entry */
	prel->valid = true;

	return prel;
}

/* Invalidate PartRelationInfo cache entry. Create new entry if 'found' is NULL. */
PartRelationInfo *
invalidate_pathman_relation_info(Oid relid, bool *found)
{
	bool				prel_found;
	HASHACTION			action = found ? HASH_FIND : HASH_ENTER;
	PartRelationInfo   *prel;

	prel = pathman_cache_search_relid(partitioned_rels,
									  relid, action,
									  &prel_found);

	/* Handle valid PartRelationInfo */
	if ((action == HASH_FIND ||
		(action == HASH_ENTER && prel_found)) && PrelIsValid(prel))
	{
		/* Remove this parent from parents cache */
		ForgetParent(prel);

		/* Drop cached bounds etc */
		MemoryContextDelete(prel->mcxt);
	}

	/* Set important default values */
	if (prel)
	{
		prel->children	= NULL;
		prel->ranges	= NULL;
		prel->mcxt		= NULL;

		prel->valid	= false; /* now cache entry is invalid */
	}

	/* Set 'found' if necessary */
	if (found) *found = prel_found;

	elog(DEBUG2,
		 "Invalidating record for relation %u in pg_pathman's cache [%u]",
		 relid, MyProcPid);

	return prel;
}

/* Get PartRelationInfo from local cache. */
const PartRelationInfo *
get_pathman_relation_info(Oid relid)
{
	const PartRelationInfo *prel = pathman_cache_search_relid(partitioned_rels,
															  relid, HASH_FIND,
															  NULL);
	/* Refresh PartRelationInfo if needed */
	if (prel && !PrelIsValid(prel))
	{
		ItemPointerData		iptr;
		Datum				values[Natts_pathman_config];
		bool				isnull[Natts_pathman_config];

		/* Check that PATHMAN_CONFIG table contains this relation */
		if (pathman_config_contains_relation(relid, values, isnull, NULL, &iptr))
		{
			bool upd_expr = isnull[Anum_pathman_config_cooked_expr - 1];
			if (upd_expr)
				pathman_config_refresh_parsed_expression(relid, values, isnull, &iptr);

			/* Refresh partitioned table cache entry (might turn NULL) */
			prel = refresh_pathman_relation_info(relid, values, false);
		}

		/* Else clear remaining cache entry */
		else
		{
			remove_pathman_relation_info(relid);
			prel = NULL; /* don't forget to reset 'prel' */
		}
	}

	elog(DEBUG2,
		 "Fetching %s record for relation %u from pg_pathman's cache [%u]",
		 (prel ? "live" : "NULL"), relid, MyProcPid);

	/* Make sure that 'prel' is valid */
	Assert(!prel || PrelIsValid(prel));

	return prel;
}

/* Acquire lock on a table and try to get PartRelationInfo */
const PartRelationInfo *
get_pathman_relation_info_after_lock(Oid relid,
									 bool unlock_if_not_found,
									 LockAcquireResult *lock_result)
{
	const PartRelationInfo *prel;
	LockAcquireResult		acquire_result;

	/* Restrict concurrent partition creation (it's dangerous) */
	acquire_result = xact_lock_rel(relid, ShareUpdateExclusiveLock, false);

	/* Invalidate cache entry (see AcceptInvalidationMessages()) */
	invalidate_pathman_relation_info(relid, NULL);

	/* Set 'lock_result' if asked to */
	if (lock_result)
		*lock_result = acquire_result;

	prel = get_pathman_relation_info(relid);
	if (!prel && unlock_if_not_found)
		UnlockRelationOid(relid, ShareUpdateExclusiveLock);

	return prel;
}

/* Remove PartRelationInfo from local cache. */
void
remove_pathman_relation_info(Oid relid)
{
	bool found;

	/* Free resources */
	invalidate_pathman_relation_info(relid, &found);

	/* Now let's remove the entry completely */
	if (found)
		pathman_cache_search_relid(partitioned_rels, relid, HASH_REMOVE, NULL);

	elog(DEBUG2,
		 "Removing record for relation %u in pg_pathman's cache [%u]",
		 relid, MyProcPid);
}

/* Fill PartRelationInfo with partition-related info */
static void
fill_prel_with_partitions(PartRelationInfo *prel,
						  const Oid *partitions,
						  const uint32 parts_count)
{
/* Allocate array if partitioning type matches 'prel' (or "ANY") */
#define AllocZeroArray(part_type, context, elem_num, elem_type) \
	( \
		((part_type) == PT_ANY || (part_type) == prel->parttype) ? \
			MemoryContextAllocZero((context), (elem_num) * sizeof(elem_type)) : \
			NULL \
	)

	uint32			i;
	MemoryContext	temp_mcxt,	/* reference temporary mcxt */
					old_mcxt;	/* reference current mcxt */

	AssertTemporaryContext();

	/* Allocate memory for 'prel->children' & 'prel->ranges' (if needed) */
	prel->children	= AllocZeroArray(PT_ANY,   prel->mcxt, parts_count, Oid);
	prel->ranges	= AllocZeroArray(PT_RANGE, prel->mcxt, parts_count, RangeEntry);

	/* Set number of children */
	PrelChildrenCount(prel) = parts_count;

	/* Create temporary memory context for loop */
	temp_mcxt = AllocSetContextCreate(CurrentMemoryContext,
									  CppAsString(fill_prel_with_partitions),
									  ALLOCSET_DEFAULT_SIZES);

	/* Initialize bounds of partitions */
	for (i = 0; i < PrelChildrenCount(prel); i++)
	{
		PartBoundInfo *pbin;

		/* Clear all previous allocations */
		MemoryContextReset(temp_mcxt);

		/* Switch to the temporary memory context */
		old_mcxt = MemoryContextSwitchTo(temp_mcxt);
		{
			/* Fetch constraint's expression tree */
			pbin = get_bounds_of_partition(partitions[i], prel);
		}
		MemoryContextSwitchTo(old_mcxt);

		/* Copy bounds from bound cache */
		switch (prel->parttype)
		{
			case PT_HASH:
				prel->children[pbin->part_idx] = pbin->child_rel;
				break;

			case PT_RANGE:
				{
					/* Copy child's Oid */
					prel->ranges[i].child_oid = pbin->child_rel;

					/* Copy all min & max Datums to the persistent mcxt */
					old_mcxt = MemoryContextSwitchTo(prel->mcxt);
					{
						prel->ranges[i].min = CopyBound(&pbin->range_min,
														prel->ev_byval,
														prel->ev_len);

						prel->ranges[i].max = CopyBound(&pbin->range_max,
														prel->ev_byval,
														prel->ev_len);
					}
					MemoryContextSwitchTo(old_mcxt);
				}
				break;

			default:
				{
					DisablePathman(); /* disable pg_pathman since config is broken */
					WrongPartType(prel->parttype);
				}
				break;
		}
	}

	/* Drop temporary memory context */
	MemoryContextDelete(temp_mcxt);

	/* Finalize 'prel' for a RANGE-partitioned table */
	if (prel->parttype == PT_RANGE)
	{
		cmp_func_info	cmp_info;

		/* Prepare function info */
		fmgr_info(prel->cmp_proc, &cmp_info.flinfo);
		cmp_info.collid = prel->ev_collid;

		/* Sort partitions by RangeEntry->min asc */
		qsort_arg((void *) prel->ranges, PrelChildrenCount(prel),
				  sizeof(RangeEntry), cmp_range_entries,
				  (void *) &cmp_info);

		/* Initialize 'prel->children' array */
		for (i = 0; i < PrelChildrenCount(prel); i++)
			prel->children[i] = prel->ranges[i].child_oid;
	}

#ifdef USE_ASSERT_CHECKING
	/* Check that each partition Oid has been assigned properly */
	if (prel->parttype == PT_HASH)
		for (i = 0; i < PrelChildrenCount(prel); i++)
		{
			if (!OidIsValid(prel->children[i]))
			{
				DisablePathman(); /* disable pg_pathman since config is broken */
				elog(ERROR, "pg_pathman's cache for relation \"%s\" "
							"has not been properly initialized",
					 get_rel_name_or_relid(PrelParentRelid(prel)));
			}
		}
#endif
}

/* qsort comparison function for RangeEntries */
static int
cmp_range_entries(const void *p1, const void *p2, void *arg)
{
	const RangeEntry   *v1 = (const RangeEntry *) p1;
	const RangeEntry   *v2 = (const RangeEntry *) p2;
	cmp_func_info	   *info = (cmp_func_info *) arg;

	return cmp_bounds(&info->flinfo, info->collid, &v1->min, &v2->min);
}


/*
 * Partitioning expression routines.
 */

/* Wraps expression in SELECT query and returns parse tree */
Node *
parse_partitioning_expression(const Oid relid,
							  const char *expr_cstr,
							  char **query_string_out,	/* ret value #1 */
							  Node **parsetree_out)		/* ret value #2 */
{
	SelectStmt		   *select_stmt;
	List			   *parsetree_list;
	MemoryContext		old_mcxt;

	const char *sql = "SELECT (%s) FROM ONLY %s.%s";
	char	   *relname = get_rel_name(relid),
			   *nspname = get_namespace_name(get_rel_namespace(relid));
	char	   *query_string = psprintf(sql, expr_cstr,
										quote_identifier(nspname),
										quote_identifier(relname));

	old_mcxt = CurrentMemoryContext;

	PG_TRY();
	{
		parsetree_list = raw_parser(query_string);
	}
	PG_CATCH();
	{
		ErrorData  *error;

		/* Switch to the original context & copy edata */
		MemoryContextSwitchTo(old_mcxt);
		error = CopyErrorData();
		FlushErrorState();

		/* Adjust error message */
		error->detail		= error->message;
		error->message		= psprintf(PARSE_PART_EXPR_ERROR, expr_cstr);
		error->sqlerrcode	= ERRCODE_INVALID_PARAMETER_VALUE;
		error->cursorpos	= 0;
		error->internalpos	= 0;

		ReThrowError(error);
	}
	PG_END_TRY();

	if (list_length(parsetree_list) != 1)
		elog(ERROR, "expression \"%s\" produced more than one query", expr_cstr);

#if PG_VERSION_NUM >= 100000
	select_stmt = (SelectStmt *) ((RawStmt *) linitial(parsetree_list))->stmt;
#else
	select_stmt = (SelectStmt *) linitial(parsetree_list);
#endif

	if (query_string_out)
		*query_string_out = query_string;

	if (parsetree_out)
		*parsetree_out = (Node *) linitial(parsetree_list);

	return ((ResTarget *) linitial(select_stmt->targetList))->val;
}

/* Parse partitioning expression and return its type and nodeToString() as TEXT */
Datum
cook_partitioning_expression(const Oid relid,
							 const char *expr_cstr,
							 Oid *expr_type_out) /* ret value #1 */
{
	Node		   *parse_tree;
	List		   *query_tree_list;

	char		   *query_string,
				   *expr_serialized = ""; /* keep compiler happy */

	Datum			expr_datum;

	MemoryContext	parse_mcxt,
					old_mcxt;

	AssertTemporaryContext();

	/*
	 * We use separate memory context here, just to make sure we won't
	 * leave anything behind after parsing, rewriting and planning.
	 */
	parse_mcxt = AllocSetContextCreate(CurrentMemoryContext,
									   CppAsString(cook_partitioning_expression),
									   ALLOCSET_DEFAULT_SIZES);

	/* Switch to mcxt for cooking :) */
	old_mcxt = MemoryContextSwitchTo(parse_mcxt);

	/* First we have to build a raw AST */
	(void) parse_partitioning_expression(relid, expr_cstr,
										 &query_string, &parse_tree);

	/* We don't need pg_pathman's magic here */
	pathman_hooks_enabled = false;

	PG_TRY();
	{
		Query	   *query;
		Node	   *expr;
		int			expr_attr;
		Relids		expr_varnos;
		Bitmapset  *expr_varattnos = NULL;

		/* This will fail with ERROR in case of wrong expression */
		query_tree_list = pg_analyze_and_rewrite_compat(parse_tree, query_string,
														NULL, 0, NULL);

		/* Sanity check #1 */
		if (list_length(query_tree_list) != 1)
			elog(ERROR, "partitioning expression produced more than 1 query");

		query = (Query *) linitial(query_tree_list);

		/* Sanity check #2 */
		if (list_length(query->targetList) != 1)
			elog(ERROR, "there should be exactly 1 partitioning expression");

		/* Sanity check #3 */
		if (query_tree_walker(query, query_contains_subqueries, NULL, 0))
			elog(ERROR, "subqueries are not allowed in partitioning expression");

		expr = (Node *) ((TargetEntry *) linitial(query->targetList))->expr;
		expr = eval_const_expressions(NULL, expr);

		/* Sanity check #4 */
		if (contain_mutable_functions(expr))
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
					 errmsg("functions in partitioning expression"
							" must be marked IMMUTABLE")));

		/* Sanity check #5 */
		expr_varnos = pull_varnos(expr);
		if (bms_num_members(expr_varnos) != 1 ||
			relid != ((RangeTblEntry *) linitial(query->rtable))->relid)
		{
			elog(ERROR, "partitioning expression should reference table \"%s\"",
				 get_rel_name(relid));
		}

		/* Sanity check #6 */
		pull_varattnos(expr, bms_singleton_member(expr_varnos), &expr_varattnos);
		expr_attr = -1;
		while ((expr_attr = bms_next_member(expr_varattnos, expr_attr)) >= 0)
		{
			AttrNumber	attnum = expr_attr + FirstLowInvalidHeapAttributeNumber;
			HeapTuple	htup;

			/* Check that there's no system attributes in expression */
			if (attnum < InvalidAttrNumber)
				ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
								errmsg("system attributes are not supported")));

			htup = SearchSysCache2(ATTNUM,
								   ObjectIdGetDatum(relid),
								   Int16GetDatum(attnum));
			if (HeapTupleIsValid(htup))
			{
				bool nullable;

				/* Fetch 'nullable' and free syscache tuple */
				nullable = !((Form_pg_attribute) GETSTRUCT(htup))->attnotnull;
				ReleaseSysCache(htup);

				if (nullable)
					ereport(ERROR, (errcode(ERRCODE_NOT_NULL_VIOLATION),
									errmsg("column \"%s\" should be marked NOT NULL",
										   get_attname(relid, attnum))));
			}
		}

		/* Free sets */
		bms_free(expr_varnos);
		bms_free(expr_varattnos);

		Assert(expr);
		expr_serialized = nodeToString(expr);

		/* Set 'expr_type_out' if needed */
		if (expr_type_out)
			*expr_type_out = exprType(expr);
	}
	PG_CATCH();
	{
		ErrorData  *error;

		/* Don't forget to enable pg_pathman's hooks */
		pathman_hooks_enabled = true;

		/* Switch to the original context & copy edata */
		MemoryContextSwitchTo(old_mcxt);
		error = CopyErrorData();
		FlushErrorState();

		/* Adjust error message */
		error->detail		= error->message;
		error->message		= psprintf(COOK_PART_EXPR_ERROR, expr_cstr);
		error->sqlerrcode	= ERRCODE_INVALID_PARAMETER_VALUE;
		error->cursorpos	= 0;
		error->internalpos	= 0;

		ReThrowError(error);
	}
	PG_END_TRY();

	/* Don't forget to enable pg_pathman's hooks */
	pathman_hooks_enabled = true;

	/* Switch to previous mcxt */
	MemoryContextSwitchTo(old_mcxt);

	/* Get Datum of serialized expression (right mcxt) */
	expr_datum = CStringGetTextDatum(expr_serialized);

	/* Free memory */
	MemoryContextDelete(parse_mcxt);

	return expr_datum;
}

/* Canonicalize user's expression (trim whitespaces etc) */
char *
canonicalize_partitioning_expression(const Oid relid,
									 const char *expr_cstr)
{
	Node		   *parse_tree;
	Expr		   *expr;
	char		   *query_string;
	Query		   *query;

	AssertTemporaryContext();

	/* First we have to build a raw AST */
	(void) parse_partitioning_expression(relid, expr_cstr,
										 &query_string, &parse_tree);

	query = parse_analyze_compat(parse_tree, query_string, NULL, 0, NULL);
	expr = ((TargetEntry *) linitial(query->targetList))->expr;

	/* We don't care about memory efficiency here */
	return deparse_expression((Node *) expr,
							  deparse_context_for(get_rel_name(relid), relid),
							  false, false);
}

/* Check if query has subqueries */
static bool
query_contains_subqueries(Node *node, void *context)
{
	if (node == NULL)
		return false;

	/* We've met a subquery */
	if (IsA(node, Query))
		return true;

	return expression_tree_walker(node, query_contains_subqueries, NULL);
}


/*
 * Functions for delayed invalidation.
 */

/* Add new delayed pathman shutdown job (DROP EXTENSION) */
void
delay_pathman_shutdown(void)
{
	delayed_shutdown = true;
}

/* Add new delayed invalidation job for a [ex-]parent relation */
void
delay_invalidation_parent_rel(Oid parent)
{
	list_add_unique(delayed_invalidation_parent_rels, parent);
}

/* Add new delayed invalidation job for a vague relation */
void
delay_invalidation_vague_rel(Oid vague_rel)
{
	list_add_unique(delayed_invalidation_vague_rels, vague_rel);
}

/* Finish all pending invalidation jobs if possible */
void
finish_delayed_invalidation(void)
{
	/* Exit early if there's nothing to do */
	if (delayed_invalidation_parent_rels == NIL &&
		delayed_invalidation_vague_rels == NIL &&
		delayed_shutdown == false)
	{
		return;
	}

	/* Check that current state is transactional */
	if (IsTransactionState())
	{
		Oid		   *parents = NULL;
		int			parents_count;
		bool		parents_fetched = false;
		ListCell   *lc;

		/* Handle the probable 'DROP EXTENSION' case */
		if (delayed_shutdown)
		{
			Oid		cur_pathman_config_relid;

			/* Unset 'shutdown' flag */
			delayed_shutdown = false;

			/* Get current PATHMAN_CONFIG relid */
			cur_pathman_config_relid = get_relname_relid(PATHMAN_CONFIG,
														 get_pathman_schema());

			/* Check that PATHMAN_CONFIG table has indeed been dropped */
			if (cur_pathman_config_relid == InvalidOid ||
				cur_pathman_config_relid != get_pathman_config_relid(true))
			{
				/* Ok, let's unload pg_pathman's config */
				unload_config();

				/* Disregard all remaining invalidation jobs */
				free_invalidation_list(delayed_invalidation_parent_rels);
				free_invalidation_list(delayed_invalidation_vague_rels);

				/* No need to continue, exit */
				return;
			}
		}

		/* Process relations that are (or were) definitely partitioned */
		foreach (lc, delayed_invalidation_parent_rels)
		{
			Oid		parent = lfirst_oid(lc);

			/* Skip if it's a TOAST table */
			if (IsToastNamespace(get_rel_namespace(parent)))
				continue;

			/* Fetch all partitioned tables */
			if (!parents_fetched)
			{
				parents = read_parent_oids(&parents_count);
				parents_fetched = true;
			}

			/* Check if parent still exists */
			if (bsearch_oid(parent, parents, parents_count))
				/* get_pathman_relation_info() will refresh this entry */
				invalidate_pathman_relation_info(parent, NULL);
			else
				remove_pathman_relation_info(parent);
		}

		/* Process all other vague cases */
		foreach (lc, delayed_invalidation_vague_rels)
		{
			Oid		vague_rel = lfirst_oid(lc);

			/* Skip if it's a TOAST table */
			if (IsToastNamespace(get_rel_namespace(vague_rel)))
				continue;

			/* Fetch all partitioned tables */
			if (!parents_fetched)
			{
				parents = read_parent_oids(&parents_count);
				parents_fetched = true;
			}

			/* It might be a partitioned table or a partition */
			if (!try_invalidate_parent(vague_rel, parents, parents_count))
			{
				PartParentSearch	search;
				Oid					parent;
				List			   *fresh_rels = delayed_invalidation_parent_rels;

				parent = get_parent_of_partition(vague_rel, &search);

				switch (search)
				{
					/*
					 * Two main cases:
					 *	- It's *still* parent (in PATHMAN_CONFIG)
					 *	- It *might have been* parent before (not in PATHMAN_CONFIG)
					 */
					case PPS_ENTRY_PART_PARENT:
					case PPS_ENTRY_PARENT:
						{
							/* Skip if we've already refreshed this parent */
							if (!list_member_oid(fresh_rels, parent))
								try_invalidate_parent(parent, parents, parents_count);
						}
						break;

					/* How come we still don't know?? */
					case PPS_NOT_SURE:
						elog(ERROR, "Unknown table status, this should never happen");
						break;

					default:
						break;
				}
			}
		}

		free_invalidation_list(delayed_invalidation_parent_rels);
		free_invalidation_list(delayed_invalidation_vague_rels);

		if (parents)
			pfree(parents);
	}
}


/*
 * cache\forget\get PartParentInfo functions.
 */

/* Create "partition+parent" pair in local cache */
void
cache_parent_of_partition(Oid partition, Oid parent)
{
	bool			found;
	PartParentInfo *ppar;

	ppar = pathman_cache_search_relid(parent_cache,
									  partition,
									  HASH_ENTER,
									  &found);
	elog(DEBUG2,
		 found ?
			 "Refreshing record for child %u in pg_pathman's cache [%u]" :
			 "Creating new record for child %u in pg_pathman's cache [%u]",
		 partition, MyProcPid);

	ppar->child_rel = partition;
	ppar->parent_rel = parent;
}

/* Remove "partition+parent" pair from cache & return parent's Oid */
Oid
forget_parent_of_partition(Oid partition, PartParentSearch *status)
{
	return get_parent_of_partition_internal(partition, status, HASH_REMOVE);
}

/* Return partition parent's Oid */
Oid
get_parent_of_partition(Oid partition, PartParentSearch *status)
{
	return get_parent_of_partition_internal(partition, status, HASH_FIND);
}

/*
 * Get [and remove] "partition+parent" pair from cache,
 * also check syscache if 'status' is provided.
 *
 * "status == NULL" implies that we don't care about
 * neither syscache nor PATHMAN_CONFIG table contents.
 */
static Oid
get_parent_of_partition_internal(Oid partition,
								 PartParentSearch *status,
								 HASHACTION action)
{
	const char	   *action_str; /* "Fetching"\"Resetting" */
	Oid				parent;
	PartParentInfo *ppar = pathman_cache_search_relid(parent_cache,
													  partition,
													  HASH_FIND,
													  NULL);
	/* Set 'action_str' */
	switch (action)
	{
		case HASH_REMOVE:
			action_str = "Resetting";
			break;

		case HASH_FIND:
			action_str = "Fetching";
			break;

		default:
			elog(ERROR, "Unexpected HTAB action %u", action);
	}

	elog(DEBUG2,
		 "%s %s record for child %u from pg_pathman's cache [%u]",
		 action_str, (ppar ? "live" : "NULL"), partition, MyProcPid);

	if (ppar)
	{
		if (status) *status = PPS_ENTRY_PART_PARENT;
		parent = ppar->parent_rel;

		/* Remove entry if necessary */
		if (action == HASH_REMOVE)
			pathman_cache_search_relid(parent_cache, partition,
									   HASH_REMOVE, NULL);
	}
	/* Try fetching parent from syscache if 'status' is provided */
	else if (status)
		parent = try_catalog_parent_search(partition, status);
	else
		parent = InvalidOid; /* we don't have to set status */

	return parent;
}

/* Try to find parent of a partition using catalog & PATHMAN_CONFIG */
static Oid
try_catalog_parent_search(Oid partition, PartParentSearch *status)
{
	if (!IsTransactionState())
	{
		/* We could not perform search */
		if (status) *status = PPS_NOT_SURE;

		return InvalidOid;
	}
	else
	{
		Relation		relation;
		ScanKeyData		key[1];
		SysScanDesc		scan;
		HeapTuple		inheritsTuple;
		Oid				parent = InvalidOid;

		/* At first we assume parent does not exist (not a partition) */
		if (status) *status = PPS_ENTRY_NOT_FOUND;

		relation = heap_open(InheritsRelationId, AccessShareLock);

		ScanKeyInit(&key[0],
					Anum_pg_inherits_inhrelid,
					BTEqualStrategyNumber, F_OIDEQ,
					ObjectIdGetDatum(partition));

		scan = systable_beginscan(relation, InheritsRelidSeqnoIndexId,
								  true, NULL, 1, key);

		while ((inheritsTuple = systable_getnext(scan)) != NULL)
		{
			parent = ((Form_pg_inherits) GETSTRUCT(inheritsTuple))->inhparent;

			/*
			 * NB: don't forget that 'inh' flag does not immediately
			 * mean that this is a pg_pathman's partition. It might
			 * be just a casual inheriting table.
			 */
			if (status) *status = PPS_ENTRY_PARENT;

			/* Check that PATHMAN_CONFIG contains this table */
			if (pathman_config_contains_relation(parent, NULL, NULL, NULL, NULL))
			{
				/* We've found the entry, update status */
				if (status) *status = PPS_ENTRY_PART_PARENT;
			}

			break; /* there should be no more rows */
		}

		systable_endscan(scan);
		heap_close(relation, AccessShareLock);

		return parent;
	}
}

/* Try to invalidate cache entry for relation 'parent' */
static bool
try_invalidate_parent(Oid relid, Oid *parents, int parents_count)
{
	/* Check if this is a partitioned table */
	if (bsearch_oid(relid, parents, parents_count))
	{
		/* get_pathman_relation_info() will refresh this entry */
		invalidate_pathman_relation_info(relid, NULL);

		/* Success */
		return true;
	}

	/* Clear remaining cache entry */
	remove_pathman_relation_info(relid);

	/* Not a partitioned relation */
	return false;
}


/*
 * forget\get constraint functions.
 */

/* Remove partition's constraint from cache */
void
forget_bounds_of_partition(Oid partition)
{
	PartBoundInfo *pbin;

	/* Should we search in bounds cache? */
	pbin = pg_pathman_enable_bounds_cache ?
				pathman_cache_search_relid(bound_cache,
										   partition,
										   HASH_FIND,
										   NULL) :
				NULL; /* don't even bother */

	/* Free this entry */
	if (pbin)
	{
		/* Call pfree() if it's RANGE bounds */
		if (pbin->parttype == PT_RANGE)
		{
			FreeBound(&pbin->range_min, pbin->byval);
			FreeBound(&pbin->range_max, pbin->byval);
		}

		/* Finally remove this entry from cache */
		pathman_cache_search_relid(bound_cache,
								   partition,
								   HASH_REMOVE,
								   NULL);
	}
}

/* Return partition's constraint as expression tree */
PartBoundInfo *
get_bounds_of_partition(Oid partition, const PartRelationInfo *prel)
{
	PartBoundInfo *pbin;

	/*
	 * We might end up building the constraint
	 * tree that we wouldn't want to keep.
	 */
	AssertTemporaryContext();

	/* Should we search in bounds cache? */
	pbin = pg_pathman_enable_bounds_cache ?
				pathman_cache_search_relid(bound_cache,
										   partition,
										   HASH_FIND,
										   NULL) :
				NULL; /* don't even bother */

	/* Build new entry */
	if (!pbin)
	{
		PartBoundInfo	pbin_local;
		Expr		   *con_expr;

		/* Initialize other fields */
		pbin_local.child_rel = partition;
		pbin_local.byval = prel->ev_byval;

		/* Try to build constraint's expression tree (may emit ERROR) */
		con_expr = get_partition_constraint_expr(partition);

		/* Grab bounds/hash and fill in 'pbin_local' (may emit ERROR) */
		fill_pbin_with_bounds(&pbin_local, prel, con_expr);

		/* We strive to delay the creation of cache's entry */
		pbin = pg_pathman_enable_bounds_cache ?
					pathman_cache_search_relid(bound_cache,
											   partition,
											   HASH_ENTER,
											   NULL) :
					palloc(sizeof(PartBoundInfo));

		/* Copy data from 'pbin_local' */
		memcpy(pbin, &pbin_local, sizeof(PartBoundInfo));
	}

	return pbin;
}

/*
 * Get constraint expression tree of a partition.
 *
 * build_check_constraint_name_internal() is used to build conname.
 */
static Expr *
get_partition_constraint_expr(Oid partition)
{
	Oid			conid;			/* constraint Oid */
	char	   *conname;		/* constraint name */
	HeapTuple	con_tuple;
	Datum		conbin_datum;
	bool		conbin_isnull;
	Expr	   *expr;			/* expression tree for constraint */

	conname = build_check_constraint_name_relid_internal(partition);
	conid = get_relation_constraint_oid(partition, conname, true);

	if (!OidIsValid(conid))
	{
		DisablePathman(); /* disable pg_pathman since config is broken */
		ereport(ERROR,
				(errmsg("constraint \"%s\" of partition \"%s\" does not exist",
						conname, get_rel_name_or_relid(partition)),
				 errhint(INIT_ERROR_HINT)));
	}

	con_tuple = SearchSysCache1(CONSTROID, ObjectIdGetDatum(conid));
	conbin_datum = SysCacheGetAttr(CONSTROID, con_tuple,
								   Anum_pg_constraint_conbin,
								   &conbin_isnull);
	if (conbin_isnull)
	{
		DisablePathman(); /* disable pg_pathman since config is broken */
		ereport(WARNING,
				(errmsg("constraint \"%s\" of partition \"%s\" has NULL conbin",
						conname, get_rel_name_or_relid(partition)),
				 errhint(INIT_ERROR_HINT)));
		pfree(conname);

		return NULL; /* could not parse */
	}
	pfree(conname);

	/* Finally we get a constraint expression tree */
	expr = (Expr *) stringToNode(TextDatumGetCString(conbin_datum));

	/* Don't foreget to release syscache tuple */
	ReleaseSysCache(con_tuple);

	return expr;
}

/* Fill PartBoundInfo with bounds/hash */
static void
fill_pbin_with_bounds(PartBoundInfo *pbin,
					  const PartRelationInfo *prel,
					  const Expr *constraint_expr)
{
	AssertTemporaryContext();

	/* Copy partitioning type to 'pbin' */
	pbin->parttype = prel->parttype;

	/* Perform a partitioning_type-dependent task */
	switch (prel->parttype)
	{
		case PT_HASH:
			{
				if (!validate_hash_constraint(constraint_expr,
											  prel, &pbin->part_idx))
				{
					DisablePathman(); /* disable pg_pathman since config is broken */
					ereport(ERROR,
							(errmsg("wrong constraint format for HASH partition \"%s\"",
									get_rel_name_or_relid(pbin->child_rel)),
							 errhint(INIT_ERROR_HINT)));
				}
			}
			break;

		case PT_RANGE:
			{
				Datum	lower, upper;
				bool	lower_null, upper_null;

				if (validate_range_constraint(constraint_expr,
											  prel, &lower, &upper,
											  &lower_null, &upper_null))
				{
					MemoryContext old_mcxt;

					/* Switch to the persistent memory context */
					old_mcxt = MemoryContextSwitchTo(PathmanBoundCacheContext);

					pbin->range_min = lower_null ?
											MakeBoundInf(MINUS_INFINITY) :
											MakeBound(datumCopy(lower,
																prel->ev_byval,
																prel->ev_len));

					pbin->range_max = upper_null ?
											MakeBoundInf(PLUS_INFINITY) :
											MakeBound(datumCopy(upper,
																prel->ev_byval,
																prel->ev_len));

					/* Switch back */
					MemoryContextSwitchTo(old_mcxt);
				}
				else
				{
					DisablePathman(); /* disable pg_pathman since config is broken */
					ereport(ERROR,
							(errmsg("wrong constraint format for RANGE partition \"%s\"",
									get_rel_name_or_relid(pbin->child_rel)),
							 errhint(INIT_ERROR_HINT)));
				}
			}
			break;

		default:
			{
				DisablePathman(); /* disable pg_pathman since config is broken */
				WrongPartType(prel->parttype);
			}
			break;
	}
}


/*
 * Common PartRelationInfo checks. Emit ERROR if anything is wrong.
 */
void
shout_if_prel_is_invalid(const Oid parent_oid,
						 const PartRelationInfo *prel,
						 const PartType expected_part_type)
{
	if (!prel)
		elog(ERROR, "relation \"%s\" has no partitions",
			 get_rel_name_or_relid(parent_oid));

	if (!PrelIsValid(prel))
		elog(ERROR, "pg_pathman's cache contains invalid entry "
					"for relation \"%s\" [%u]",
			 get_rel_name_or_relid(parent_oid),
			 MyProcPid);

	/* Check partitioning type unless it's "ANY" */
	if (expected_part_type != PT_ANY &&
		expected_part_type != prel->parttype)
	{
		char *expected_str;

		switch (expected_part_type)
		{
			case PT_HASH:
				expected_str = "HASH";
				break;

			case PT_RANGE:
				expected_str = "RANGE";
				break;

			default:
				WrongPartType(expected_part_type);
				expected_str = NULL; /* keep compiler happy */
		}

		elog(ERROR, "relation \"%s\" is not partitioned by %s",
			 get_rel_name_or_relid(parent_oid),
			 expected_str);
	}
}
