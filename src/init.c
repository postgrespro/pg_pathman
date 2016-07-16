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
#include "catalog/pg_class.h"
#include "catalog/pg_constraint.h"
#include "catalog/pg_operator.h"
#include "access/htup_details.h"
#include "utils/syscache.h"
#include "utils/builtins.h"
#include "utils/typcache.h"
#include "utils/lsyscache.h"
#include "utils/bytea.h"
#include "utils/snapmgr.h"
#include "optimizer/clauses.h"


HTAB   *relations = NULL;
HTAB   *range_restrictions = NULL;
bool	initialization_needed = true;


static bool validate_partition_constraints(const Oid *children_oids,
										   const uint32 children_count,
										   Snapshot snapshot,
										   PartRelationInfo *prel,
										   RangeRelation *rangerel);
static bool validate_range_constraint(Expr *, PartRelationInfo *, Datum *, Datum *);
static bool validate_hash_constraint(Expr *expr, PartRelationInfo *prel, int *hash);
static bool read_opexpr_const(OpExpr *opexpr, int varattno, Datum *val);
static int cmp_range_entries(const void *p1, const void *p2, void *arg);


/*
 * This argument contains type compare function
 * and 'by_val' flag (absent in RangeEntry) which
 * are required for the cmp_range_entries() function.
 */
typedef struct
{
	FmgrInfo   *cmp_proc;
	bool		by_val;
} cmp_range_entries_arg;


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
			pmstate->load_config_lock = LWLockAssign();
			pmstate->dsm_init_lock    = LWLockAssign();
			pmstate->edit_partitions_lock = LWLockAssign();
		}
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
		if (&pmstate->databases.length > 0)
			free_dsm_array(&pmstate->databases);
		alloc_dsm_array(&pmstate->databases, sizeof(Oid), 1);
		databases = (Oid *) dsm_array_get_pointer(&pmstate->databases);
		databases[0] = MyDatabaseId;
	}
	else
	{
		int databases_count = pmstate->databases.length;
		int i;

		/* Check if we already cached config for current database */
		databases = (Oid *) dsm_array_get_pointer(&pmstate->databases);
		for(i=0; i<databases_count; i++)
			if (databases[i] == MyDatabaseId)
			{
				LWLockRelease(pmstate->dsm_init_lock);
				return;
			}

		/* Put current database oid to databases list */
		resize_dsm_array(&pmstate->databases, sizeof(Oid), databases_count + 1);
		databases = (Oid *) dsm_array_get_pointer(&pmstate->databases);
		databases[databases_count] = MyDatabaseId;
	}

	/* Load cache */
	LWLockAcquire(pmstate->load_config_lock, LW_EXCLUSIVE);
	load_relations(new_segment_created);
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
 * Loads partitioned tables structure to hashtable.
 *
 * TODO: reload just the specified relation
 */
void
load_relations(bool reinitialize)
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
	char		sql[] = "SELECT pg_class.oid, pg_attribute.attnum, cfg.parttype, pg_attribute.atttypid "
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

		for (i=0; i<proc; i++)
		{
			RelationKey key;
			HeapTuple tuple = tuptable->vals[i];
			int oid = DatumGetObjectId(SPI_getbinval(tuple, tupdesc, 1, &isnull));

			key.dbid = MyDatabaseId;
			key.relid = oid;
			prel = (PartRelationInfo*)
				hash_search(relations, (const void *) &key, HASH_ENTER, NULL);

			prel->attnum = DatumGetInt32(SPI_getbinval(tuple, tupdesc, 2, &isnull));
			prel->parttype = DatumGetInt32(SPI_getbinval(tuple, tupdesc, 3, &isnull));
			prel->atttype = DatumGetObjectId(SPI_getbinval(tuple, tupdesc, 4, &isnull));

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
				if (reinitialize && prel->children.length > 0)
				{
					RangeRelation *rangerel = get_pathman_range_relation(oid, NULL);
					free_dsm_array(&prel->children);
					free_dsm_array(&rangerel->ranges);
					prel->children_count = 0;
				}
				load_partitions(oid, GetCatalogSnapshot(oid));
				break;
			case PT_HASH:
				if (reinitialize && prel->children.length > 0)
				{
					free_dsm_array(&prel->children);
					prel->children_count = 0;
				}
				load_partitions(oid, GetCatalogSnapshot(oid));
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

	relations = ShmemInitHash("Partitioning relation info", 1024, 1024, &ctl,
							  HASH_ELEM | HASH_BLOBS);
}

/*
 * Load and validate CHECK constraints
 */
void
load_partitions(Oid parent_oid, Snapshot snapshot)
{
	PartRelationInfo   *prel = NULL;
	RangeRelation	   *rangerel = NULL;
	SPIPlanPtr			plan = NULL;
	bool				found;
	int					ret,
						i,
						children_count = 0;
	Datum				vals[1];
	Oid					types[1] = { INT4OID };
	const bool			nulls[1] = { false };
	Oid				   *children_oids;

	vals[0] = Int32GetDatum(parent_oid);
	prel = get_pathman_relation_info(parent_oid, NULL);

	/* Skip if already loaded */
	if (prel->children.length > 0)
		return;

	/* Load children oids */
	plan = SPI_prepare("SELECT inhrelid FROM pg_inherits "
					   "WHERE inhparent = $1",
					   1, types);

	ret = SPI_execute_snapshot(plan, vals, nulls, snapshot,
							   InvalidSnapshot, true, false, 0);

	if (ret == SPI_OK_SELECT)
	{
		/* Initialize children data for validate_partition_constraints() */
		children_count = SPI_processed;
		children_oids = palloc(sizeof(Oid) * children_count);

		for(i = 0; i < children_count; i++)
		{
			TupleDesc	tupdesc = SPI_tuptable->tupdesc;
			HeapTuple	tuple = SPI_tuptable->vals[i];
			bool		isnull;

			children_oids[i] = DatumGetObjectId(SPI_getbinval(tuple,
															  tupdesc,
															  1, &isnull));
		}
	}
	else return; /* there are no children */

	if (children_count > 0)
	{
		alloc_dsm_array(&prel->children, sizeof(Oid), children_count);

		/* allocate ranges array is dsm */
		if (prel->parttype == PT_RANGE)
		{
			TypeCacheEntry	   *tce = lookup_type_cache(prel->atttype, 0);
			RelationKey 		key;

			key.dbid = MyDatabaseId;
			key.relid = parent_oid;
			rangerel = (RangeRelation *) hash_search(range_restrictions,
													 (const void *) &key,
													 HASH_ENTER, &found);
			rangerel->by_val = tce->typbyval;
			alloc_dsm_array(&rangerel->ranges, sizeof(RangeEntry), children_count);
		}

		/* Validate partitions constraints */
		if (!validate_partition_constraints(children_oids,
											children_count,
											snapshot,
											prel,
											rangerel))
		{
			RelationKey	key;

			/*
			 * If validation failed then pg_pathman cannot handle this relation.
			 * Remove it from the cache
			 */
			key.dbid = MyDatabaseId;
			key.relid = parent_oid;

			free_dsm_array(&prel->children);
			free_dsm_array(&rangerel->ranges);
			hash_search(relations, (const void *) &key, HASH_REMOVE, &found);
			if (prel->parttype == PT_RANGE)
				hash_search(range_restrictions,
							(const void *) &key,
							HASH_REMOVE, &found);

			elog(WARNING, "Validation failed for relation '%s'. "
						  "It will not be handled by pg_pathman",
				get_rel_name(parent_oid));
		}
		else
			prel->children_count = children_count;

		pfree(children_oids);
	}
}

static bool
validate_partition_constraints(const Oid *children_oids,
							   const uint32 children_count,
							   Snapshot snapshot,
							   PartRelationInfo *prel,
							   RangeRelation *rangerel)
{
	Expr		   *expr = NULL; /* constraint object tree */
	RangeEntry	   *ranges = NULL;
	Oid			   *dsm_children = NULL; /* points to prel->children */
	int				i;


	if (children_count == 0)
		return false; /* nothing to do here */

	dsm_children = dsm_array_get_pointer(&prel->children);
	if (prel->parttype == PT_RANGE)
		ranges = (RangeEntry *) dsm_array_get_pointer(&rangerel->ranges);

	/* Iterate through children */
	for (i = 0; i < children_count; i++)
	{
		Form_pg_constraint	constraint = NULL;
		uint32				cur_processed;
		bool				found_valid_constraint = false;

		/* SPI args */
		Datum				oids[1]  = { ObjectIdGetDatum(children_oids[i]) };
		const bool			nulls[1] = { false };
		Oid					types[1] = { INT4OID };

		SPIPlanPtr			plan = NULL;
		int					ret; /* SPI result code */


		/* Select constraints for this partition */
		plan = SPI_prepare("SELECT * FROM pg_constraint "
						   "WHERE conrelid = $1 AND contype = 'c'",
						   1, types);

		ret = SPI_execute_snapshot(plan, oids, nulls, snapshot,
								   InvalidSnapshot, true, false, 0);

		if (ret != SPI_OK_SELECT)
		{
			elog(WARNING,
				 "No constraints found for partition %s",
				 get_rel_name(children_oids[i]));

			return false; /* keep compiler happy */
		}

		/* Iterate through all available check constraints to find a valid one */
		for (cur_processed = 0;
			 cur_processed < SPI_processed && !found_valid_constraint;
			 cur_processed++)
		{
			int			hash;			/* temp hash value for HASH partitioning */
			RangeEntry	re;				/* temporary RangeEntry */
			Datum		min,			/* RangeEntry's min value */
						max;			/* RangeEntry's max value */

			Datum		conbin_datum;	/* nodeToString representation of constraint */
			bool		conbin_isnull;

			HeapTuple	tuple = SPI_tuptable->vals[cur_processed];


			/* Get 'conbin' from the 'pg_constraint' */
			constraint = (Form_pg_constraint) GETSTRUCT(tuple);
			conbin_datum = SysCacheGetAttr(CONSTROID, tuple,
										   Anum_pg_constraint_conbin,
										   &conbin_isnull);

			/* Handle unexpected null value */
			if (conbin_isnull)
				continue; /* not a valid value anyway, skipping */

			/* Convert constraint datum to object tree */
			expr = (Expr *) stringToNode(TextDatumGetCString(conbin_datum));

			switch(prel->parttype)
			{
				case PT_RANGE:
					if (!validate_range_constraint(expr, prel, &min, &max))
						continue;

					/* If datum is referenced by val then just assign */
					if (rangerel->by_val)
					{
						re.min = min;
						re.max = max;
					}
					/* else copy data by pointer */
					else
					{
						memcpy(&re.min, DatumGetPointer(min), sizeof(re.min));
						memcpy(&re.max, DatumGetPointer(max), sizeof(re.max));
					}
					re.child_oid = constraint->conrelid;
					ranges[i] = re; /* copy struct 're' */

					/* Found valid range constraint */
					found_valid_constraint = true;
					break;

				case PT_HASH:
					if (!validate_hash_constraint(expr, prel, &hash))
						continue;

					/* Copy oid to the prel's 'children' array */
					dsm_children[hash] = constraint->conrelid;

					/* Found valid hash constraint */
					found_valid_constraint = true;
					break;
			}
		}

		/* No constraint matches pattern, aborting */
		if (!found_valid_constraint)
		{
			elog(ERROR,
				 "Cannot find valid check constraint for partition %s",
				 get_rel_name(children_oids[i]));

			return false; /* keep compiler happy */
		}

		/* Don't forget to free plan */
		SPI_freeplan(plan);
	}

	/*
	 * Sort range partitions and check if they overlap
	 */
	if (prel->parttype == PT_RANGE)
	{
		TypeCacheEntry		   *tce;
		bool					byVal = rangerel->by_val;
		cmp_range_entries_arg	sort_arg; /* for qsort_arg() */

		/* Sort ascending */
		tce = lookup_type_cache(prel->atttype,
								TYPECACHE_CMP_PROC | TYPECACHE_CMP_PROC_FINFO);

		/* Initialize qsort_arg comparator()'s argument */
		sort_arg.cmp_proc = &tce->cmp_proc_finfo;
		sort_arg.by_val = byVal;

		qsort_arg(ranges, children_count, sizeof(RangeEntry),
				  cmp_range_entries, (void *) &sort_arg);

		/* Copy sorted oids to the prel's 'children' array */
		for(i = 0; i < children_count; i++)
			dsm_children[i] = ranges[i].child_oid;

		/* Check if some ranges overlap */
		for(i = 0; i < children_count - 1; i++)
		{
			Datum	cur_upper = PATHMAN_GET_DATUM(ranges[i].max, byVal),
					next_lower = PATHMAN_GET_DATUM(ranges[i + 1].min, byVal);

			bool	overlap = DatumGetInt32(FunctionCall2(&tce->cmp_proc_finfo,
														  next_lower,
														  cur_upper)) < 0;
			if (overlap)
			{
				elog(WARNING, "Partitions %s and %s overlap",
					 get_rel_name(ranges[i].child_oid),
					 get_rel_name(ranges[i + 1].child_oid));

				return false; /* keep compiler happy */
			}
		}
	}

	return true; /* everything seems to be fine */
}

/* qsort comparison function for RangeEntries */
static int
cmp_range_entries(const void *p1, const void *p2, void *arg)
{
	const RangeEntry			*v1 = (const RangeEntry *) p1;
	const RangeEntry			*v2 = (const RangeEntry *) p2;
	const cmp_range_entries_arg *sort_arg = (const cmp_range_entries_arg *) arg;

	return FunctionCall2(sort_arg->cmp_proc,
						 PATHMAN_GET_DATUM(v1->min, sort_arg->by_val),
						 PATHMAN_GET_DATUM(v2->min, sort_arg->by_val));
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

		if ( !IsA(first, FuncExpr) || !IsA(second, Const) )
			return false;

		/* Check that function is the base hash function for the type  */
		funcexpr = (FuncExpr *) first;
		if (funcexpr->funcid != prel->hash_proc ||
			(!IsA(linitial(funcexpr->args), Var) && !IsA(linitial(funcexpr->args),
														 RelabelType)))
			return false;

		/* Check that argument is partitioning key attribute */
		if (IsA(linitial(funcexpr->args), RelabelType))
			var = (Var *) ((RelabelType *) linitial(funcexpr->args))->arg;
		else
			var = (Var *) linitial(funcexpr->args);
		if (var->varattno != prel->attnum)
			return false;

		/* Check that const value less than partitions count */
		if (DatumGetInt32(((Const*) second)->constvalue) != prel->children.length)
			return false;

		if ( !IsA(lsecond(eqexpr->args), Const) )
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
 * Remove partitions from pathman's cache
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
	{
		elog(DEBUG2, "pg_pathman's cache does not contain relation %u", relid);
		return;
	}

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
