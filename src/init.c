/* ------------------------------------------------------------------------
 *
 * init.c
 *		Initialization functions
 *
 * Copyright (c) 2015-2016, Postgres Professional
 *
 * ------------------------------------------------------------------------
 */
#include "pathman.h"
#include "miscadmin.h"
#include "executor/spi.h"
#include "catalog/pg_type.h"
#include "catalog/pg_constraint.h"
#include "access/htup_details.h"
#include "utils/syscache.h"
#include "utils/builtins.h"
#include "utils/typcache.h"
#include "utils/lsyscache.h"
#include "utils/snapmgr.h"
#include "optimizer/clauses.h"


HTAB   *relations = NULL;
HTAB   *range_restrictions = NULL;
bool	initialization_needed = true;

static FmgrInfo *qsort_type_cmp_func;
static bool globalByVal;

static bool validate_range_constraint(Expr *, PartRelationInfo *, Datum *, Datum *);
static bool validate_hash_constraint(Expr *expr, PartRelationInfo *prel, int *hash);
static bool read_opexpr_const(OpExpr *opexpr, int varattno, Datum *val);
static int cmp_range_entries(const void *p1, const void *p2);

Size
pathman_memsize()
{
	Size size;

	size = get_dsm_shared_size() + MAXALIGN(sizeof(PathmanState));
	return size;
}

void
init_shmem_config()
{
	bool found;

	/* Check if module was initialized in postmaster */
	pmstate = ShmemInitStruct("pathman state", sizeof(PathmanState), &found);
	if (!found)
	{
		/*
		 * Initialize locks in postmaster
		 */
		if (!IsUnderPostmaster)
		{
			/* Initialize locks */
			pmstate->load_config_lock		= LWLockAssign();
			pmstate->dsm_init_lock			= LWLockAssign();
			pmstate->edit_partitions_lock	= LWLockAssign();
		}
#ifdef WIN32
		else
		{
			elog(ERROR, "Pathman module must be initialized in postmaster. "
						"Put the following line to configuration file: "
						"shared_preload_libraries='pg_pathman'");
			initialization_needed = false;
		}
#endif
	}

	create_relations_hashtable();
	create_range_restrictions_hashtable();
}

/*
 * Initialize hashtables
 */
void
load_config(void)
{
	bool new_segment_created;
	Oid *databases;

	initialization_needed = false;

	LWLockAcquire(pmstate->dsm_init_lock, LW_EXCLUSIVE);
	new_segment_created = init_dsm_segment(INITIAL_BLOCKS_COUNT, 32);

	/* If dsm segment just created */
	if (new_segment_created)
	{
		/*
		 * Allocate databases array and put current database
		 * oid into it. This array contains databases oids
		 * that have already been cached (to prevent repeat caching)
		 */
		if (&pmstate->databases.elem_count > 0)
			free_dsm_array(&pmstate->databases);
		alloc_dsm_array(&pmstate->databases, sizeof(Oid), 1);
		databases = (Oid *) dsm_array_get_pointer(&pmstate->databases, false);
		databases[0] = MyDatabaseId;
	}
	else
	{
		int databases_count = pmstate->databases.elem_count;
		int i;

		/* Check if we already cached config for current database */
		databases = (Oid *) dsm_array_get_pointer(&pmstate->databases, false);
		for(i = 0; i < databases_count; i++)
			if (databases[i] == MyDatabaseId)
			{
				LWLockRelease(pmstate->dsm_init_lock);
				return;
			}

		/* Put current database oid to databases list */
		resize_dsm_array(&pmstate->databases, sizeof(Oid), databases_count + 1);
		databases = (Oid *) dsm_array_get_pointer(&pmstate->databases, false);
		databases[databases_count] = MyDatabaseId;
	}

	/* Load cache */
	LWLockAcquire(pmstate->load_config_lock, LW_EXCLUSIVE);
	load_relations_hashtable(new_segment_created);
	LWLockRelease(pmstate->load_config_lock);
	LWLockRelease(pmstate->dsm_init_lock);
}

/*
 * Returns extension schema name or NULL. Caller is responsible for freeing
 * the memory.
 */
char *
get_extension_schema()
{
	int ret;
	bool isnull;

	ret = SPI_exec("SELECT extnamespace::regnamespace::text FROM pg_extension WHERE extname = 'pg_pathman'", 0);
	if (ret > 0 && SPI_tuptable != NULL && SPI_processed > 0)
	{
		TupleDesc tupdesc = SPI_tuptable->tupdesc;
		SPITupleTable *tuptable = SPI_tuptable;
		HeapTuple tuple = tuptable->vals[0];
		Datum datum = SPI_getbinval(tuple, tupdesc, 1, &isnull);

		if (isnull)
			return NULL;

		return TextDatumGetCString(datum);
	}
	return NULL;
}

/*
 * Loads partitioned tables structure to hashtable
 */
void
load_relations_hashtable(bool reinitialize)
{
	int			ret,
				i,
				proc;
	bool		isnull;
	List	   *part_oids = NIL;
	ListCell   *lc;
	char	   *schema;
	TypeCacheEntry *tce;
	PartRelationInfo *prel;
	char		sql[] = "SELECT pg_class.oid, pg_attribute.attnum,"
								"cfg.parttype, pg_attribute.atttypid, pg_attribute.atttypmod "
						"FROM %s.pathman_config as cfg "
						"JOIN pg_class ON pg_class.oid = cfg.relname::regclass::oid "
						"JOIN pg_attribute ON pg_attribute.attname = lower(cfg.attname) "
						"AND attrelid = pg_class.oid";
	char *query;

	SPI_connect();
	schema = get_extension_schema();

	/* If extension isn't exist then just quit */
	if (!schema)
	{
		SPI_finish();
		return;
	}

	/* Put schema name to the query */
	query = psprintf(sql, schema);
	ret = SPI_exec(query, 0);
	proc = SPI_processed;

	if (ret > 0 && SPI_tuptable != NULL)
	{
		TupleDesc tupdesc = SPI_tuptable->tupdesc;
		SPITupleTable *tuptable = SPI_tuptable;

		for (i = 0; i < proc; i++)
		{
			RelationKey key;
			HeapTuple tuple = tuptable->vals[i];
			Oid oid = DatumGetObjectId(SPI_getbinval(tuple, tupdesc, 1, &isnull));

			key.dbid = MyDatabaseId;
			key.relid = oid;
			prel = (PartRelationInfo*)
				hash_search(relations, (const void *) &key, HASH_ENTER, NULL);

			prel->attnum = DatumGetInt32(SPI_getbinval(tuple, tupdesc, 2, &isnull));
			prel->parttype = DatumGetInt32(SPI_getbinval(tuple, tupdesc, 3, &isnull));
			prel->atttype = DatumGetObjectId(SPI_getbinval(tuple, tupdesc, 4, &isnull));
			prel->atttypmod = DatumGetInt32(SPI_getbinval(tuple, tupdesc, 5, &isnull));

			tce = lookup_type_cache(prel->atttype, 	TYPECACHE_CMP_PROC | TYPECACHE_HASH_PROC);
			prel->cmp_proc = tce->cmp_proc;
			prel->hash_proc = tce->hash_proc;

			part_oids = lappend_int(part_oids, oid);
		}
	}
	pfree(query);

	/* Load children information */
	foreach(lc, part_oids)
	{
		Oid oid = (int) lfirst_int(lc);

		prel = get_pathman_relation_info(oid, NULL);
		switch(prel->parttype)
		{
			case PT_RANGE:
				if (reinitialize && prel->children.elem_count > 0)
				{
					RangeRelation *rangerel = get_pathman_range_relation(oid, NULL);
					free_dsm_array(&prel->children);
					free_dsm_array(&rangerel->ranges);
					prel->children_count = 0;
				}
				load_check_constraints(oid, GetCatalogSnapshot(oid));
				break;
			case PT_HASH:
				if (reinitialize && prel->children.elem_count > 0)
				{
					free_dsm_array(&prel->children);
					prel->children_count = 0;
				}
				load_check_constraints(oid, GetCatalogSnapshot(oid));
				break;
		}
	}
	SPI_finish();
}

void
create_relations_hashtable()
{
	HASHCTL		ctl;

	memset(&ctl, 0, sizeof(ctl));
	ctl.keysize = sizeof(RelationKey);
	ctl.entrysize = sizeof(PartRelationInfo);

	/* Already exists, recreate */
	if (relations != NULL)
		hash_destroy(relations);

	relations = ShmemInitHash("Partitioning relation info", 1024, 1024, &ctl, HASH_ELEM | HASH_BLOBS);
}

/*
 * Load and validate CHECK constraints
 */
void
load_check_constraints(Oid parent_oid, Snapshot snapshot)
{
	PartRelationInfo *prel = NULL;
	RangeRelation *rangerel = NULL;
	SPIPlanPtr plan;
	bool	found;
	int		ret,
			i,
			proc;
	Datum	vals[1];
	Oid		oids[1] = {INT4OID};
	bool	nulls[1] = {false};

	vals[0] = Int32GetDatum(parent_oid);
	prel = get_pathman_relation_info(parent_oid, NULL);

	/* Skip if already loaded */
	if (prel->children.elem_count > 0)
		return;

	plan = SPI_prepare("select pg_constraint.* "
					   "from pg_constraint "
					   "join pg_inherits on inhrelid = conrelid "
					   "where inhparent = $1 and contype='c';",
					   1, oids);
	ret = SPI_execute_snapshot(plan, vals, nulls,
							   snapshot, InvalidSnapshot, true, false, 0);

	proc = SPI_processed;

	if (ret > 0 && SPI_tuptable != NULL)
	{
		SPITupleTable *tuptable = SPI_tuptable;
		Oid *children;
		RangeEntry *ranges = NULL;
		Datum min;
		Datum max;
		int hash;

		alloc_dsm_array(&prel->children, sizeof(Oid), proc);
		children = (Oid *) dsm_array_get_pointer(&prel->children, false);

		if (prel->parttype == PT_RANGE)
		{
			TypeCacheEntry	   *tce;
			RelationKey key;
			key.dbid = MyDatabaseId;
			key.relid = parent_oid;

			rangerel = (RangeRelation *)
				hash_search(range_restrictions, (void *) &key, HASH_ENTER, &found);

			alloc_dsm_array(&rangerel->ranges, sizeof(RangeEntry), proc);
			ranges = (RangeEntry *) dsm_array_get_pointer(&rangerel->ranges, false);

			tce = lookup_type_cache(prel->atttype, 0);
			rangerel->by_val = tce->typbyval;
		}

		for (i = 0; i < proc; i++)
		{
			RangeEntry	re;
			HeapTuple	tuple = tuptable->vals[i];
			bool		isnull;
			Datum		val;
			char	   *conbin;
			Expr	   *expr;

			Form_pg_constraint con;

			con = (Form_pg_constraint) GETSTRUCT(tuple);

			val = SysCacheGetAttr(CONSTROID, tuple, Anum_pg_constraint_conbin,
								  &isnull);
			if (isnull)
				elog(ERROR, "null conbin for constraint %u",
					 HeapTupleGetOid(tuple));
			conbin = TextDatumGetCString(val);
			expr = (Expr *) stringToNode(conbin);

			switch(prel->parttype)
			{
				case PT_RANGE:
					if (!validate_range_constraint(expr, prel, &min, &max))
					{
						elog(WARNING, "Wrong CHECK constraint for relation '%s'. "
									  "It MUST have exact format: "
									  "VARIABLE >= CONST AND VARIABLE < CONST. Skipping...",
							 get_rel_name(con->conrelid));
						continue;
					}

					/* If datum is referenced by val then just assign */
					if (rangerel->by_val)
					{
						re.min = min;
						re.max = max;
					}
					/* else copy the memory by pointer */
					else
					{
						memcpy(&re.min, DatumGetPointer(min), sizeof(re.min));
						memcpy(&re.max, DatumGetPointer(max), sizeof(re.max));
					}
					re.child_oid = con->conrelid;
					ranges[i] = re;
					break;

				case PT_HASH:
					if (!validate_hash_constraint(expr, prel, &hash))
					{
						elog(WARNING, "Wrong CHECK constraint format for relation '%s'. "
									  "Skipping...",
							 get_rel_name(con->conrelid));
						continue;
					}
					children[hash] = con->conrelid;
			}
		}
		prel->children_count = proc;

		if (prel->parttype == PT_RANGE)
		{
			TypeCacheEntry	   *tce;
			bool byVal = rangerel->by_val;

			/* Sort ascending */
			tce = lookup_type_cache(prel->atttype, TYPECACHE_CMP_PROC_FINFO);
			qsort_type_cmp_func = &tce->cmp_proc_finfo;
			globalByVal = byVal;
			qsort(ranges, proc, sizeof(RangeEntry), cmp_range_entries);

			/* Copy oids to prel */
			for(i=0; i < proc; i++)
				children[i] = ranges[i].child_oid;

			/* Check if some ranges overlap */
			for(i=0; i < proc-1; i++)
			{
				Datum cur_upper = PATHMAN_GET_DATUM(ranges[i].max, byVal);
				Datum next_lower = PATHMAN_GET_DATUM(ranges[i+1].min, byVal);
				bool overlap = DatumGetInt32(FunctionCall2(qsort_type_cmp_func, next_lower, cur_upper)) < 0;

				if (overlap)
				{
					RelationKey key;
					key.dbid = MyDatabaseId;
					key.relid = parent_oid;

					elog(WARNING, "Partitions %u and %u overlap. Disabling pathman for relation %u...",
						 ranges[i].child_oid, ranges[i+1].child_oid, parent_oid);
					hash_search(relations, (const void *) &key, HASH_REMOVE, &found);
				}
			}
		}
	}
}


/* qsort comparison function for oids */
static int
cmp_range_entries(const void *p1, const void *p2)
{
	const RangeEntry	*v1 = (const RangeEntry *) p1;
	const RangeEntry	*v2 = (const RangeEntry *) p2;

	return FunctionCall2(qsort_type_cmp_func,
						 PATHMAN_GET_DATUM(v1->min, globalByVal),
						 PATHMAN_GET_DATUM(v2->min, globalByVal));
}

/*
 * Validates range constraint. It MUST have the exact format:
 * VARIABLE >= CONST AND VARIABLE < CONST
 */
static bool
validate_range_constraint(Expr *expr, PartRelationInfo *prel, Datum *min, Datum *max)
{
	TypeCacheEntry *tce;
	BoolExpr *boolexpr = (BoolExpr *) expr;
	OpExpr *opexpr;

	/* it should be an AND operator on top */
	if (!and_clause((Node *) expr))
		return false;

	tce = lookup_type_cache(prel->atttype, TYPECACHE_BTREE_OPFAMILY);

	/* check that left operand is >= operator */
	opexpr = (OpExpr *) linitial(boolexpr->args);
	if (get_op_opfamily_strategy(opexpr->opno, tce->btree_opf) == BTGreaterEqualStrategyNumber)
	{
		if (!read_opexpr_const(opexpr, prel->attnum, min))
			return false;
	}
	else
		return false;

	/* check that right operand is < operator */
	opexpr = (OpExpr *) lsecond(boolexpr->args);
	if (get_op_opfamily_strategy(opexpr->opno, tce->btree_opf) == BTLessStrategyNumber)
	{
		if (!read_opexpr_const(opexpr, prel->attnum, max))
			return false;
	}
	else
		return false;

	return true;
}

/*
 * Reads const value from expressions of kind: VAR >= CONST or VAR < CONST
 */
static bool
read_opexpr_const(OpExpr *opexpr, int varattno, Datum *val)
{
	Node *left = linitial(opexpr->args);
	Node *right = lsecond(opexpr->args);

	if ( !IsA(left, Var) || !IsA(right, Const) )
		return false;
	if ( ((Var*) left)->varattno != varattno )
		return false;
	*val = ((Const*) right)->constvalue;

	return true;
}

/*
 * Validate hash constraint. It MUST have the exact format
 * VARIABLE % CONST = CONST
 */
static bool
validate_hash_constraint(Expr *expr, PartRelationInfo *prel, int *hash)
{
	OpExpr	   *eqexpr;
	TypeCacheEntry *tce;
	FuncExpr   *gethashfunc;
	FuncExpr   *funcexpr;
	Var		   *var;

	if (!IsA(expr, OpExpr))
		return false;
	eqexpr = (OpExpr *) expr;

	/*
	 * We expect get_hash() function on the left
	 * TODO: check that it is really the 'get_hash' function
	 */
	if (!IsA(linitial(eqexpr->args), FuncExpr))
		return false;
	gethashfunc = (FuncExpr *) linitial(eqexpr->args);

	/* Is this an equality operator? */
	tce = lookup_type_cache(gethashfunc->funcresulttype, TYPECACHE_BTREE_OPFAMILY);
	if (get_op_opfamily_strategy(eqexpr->opno, tce->btree_opf) != BTEqualStrategyNumber)
		return false;

	if (list_length(gethashfunc->args) == 2)
	{
		Node *first = linitial(gethashfunc->args);
		Node *second = lsecond(gethashfunc->args);
		Const *mod_result;

		if (!IsA(first, FuncExpr) || !IsA(second, Const))
			return false;

		/* Check that function is the base hash function for the type  */
		funcexpr = (FuncExpr *) first;
		if (funcexpr->funcid != prel->hash_proc ||
			(!IsA(linitial(funcexpr->args), Var) && !IsA(linitial(funcexpr->args), RelabelType)))
			return false;

		/* Check that argument is partitioning key attribute */
		if (IsA(linitial(funcexpr->args), RelabelType))
			var = (Var *) ((RelabelType *) linitial(funcexpr->args))->arg;
		else
			var = (Var *) linitial(funcexpr->args);
		if (var->varattno != prel->attnum)
			return false;

		/* Check that const value less than partitions count */
		if (DatumGetInt32(((Const*) second)->constvalue) != prel->children.elem_count)
			return false;

		if (!IsA(lsecond(eqexpr->args), Const))
			return false;

		mod_result = lsecond(eqexpr->args);
		*hash = DatumGetInt32(mod_result->constvalue);
		return true;
	}

	return false;
}

/*
 * Create range restrictions table
 */
void
create_range_restrictions_hashtable()
{
	HASHCTL		ctl;

	memset(&ctl, 0, sizeof(ctl));
	ctl.keysize = sizeof(RelationKey);
	ctl.entrysize = sizeof(RangeRelation);
	range_restrictions = ShmemInitHash("pg_pathman range restrictions",
									   1024, 1024, &ctl, HASH_ELEM | HASH_BLOBS);
}

/*
 * Remove partitions
 */
void
remove_relation_info(Oid relid)
{
	PartRelationInfo   *prel;
	RangeRelation	   *rangerel;
	RelationKey			key;

	key.dbid = MyDatabaseId;
	key.relid = relid;

	prel = get_pathman_relation_info(relid, NULL);

	/* If there is nothing to remove then just return */
	if (!prel)
		return;

	/* Remove children relations */
	switch (prel->parttype)
	{
		case PT_HASH:
			free_dsm_array(&prel->children);
			break;
		case PT_RANGE:
			rangerel = get_pathman_range_relation(relid, NULL);
			free_dsm_array(&rangerel->ranges);
			free_dsm_array(&prel->children);
			hash_search(range_restrictions, (const void *) &key, HASH_REMOVE, NULL);
			break;
	}
	prel->children_count = 0;
	hash_search(relations, (const void *) &key, HASH_REMOVE, 0);
}
