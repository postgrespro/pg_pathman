#include "pathman.h"
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


HTAB   *relations = NULL;
HTAB   *range_restrictions = NULL;
bool	initialization_needed = true;


static bool validate_range_constraint(Expr *, PartRelationInfo *, Datum *, Datum *);
static bool validate_hash_constraint(Expr *expr, PartRelationInfo *prel, int *hash);
static int cmp_range_entries(const void *p1, const void *p2);
static char *get_extension_schema();

/*
 * Initialize hashtables
 */
void
load_config(void)
{
	bool new_segment_created;

	initialization_needed = false;
	new_segment_created = init_dsm_segment(32);

	LWLockAcquire(load_config_lock, LW_EXCLUSIVE);
	load_relations_hashtable(new_segment_created);
	LWLockRelease(load_config_lock);
}

/*
 * Returns extension schema name or NULL. Caller is responsible for freeing
 * the memory.
 */
static char *
get_extension_schema()
{
	int ret;
	bool isnull;

	ret = SPI_exec("SELECT extnamespace::regnamespace::text FROM pg_extension WHERE extname = 'pathman'", 0);
	if (ret > 0 && SPI_tuptable != NULL)
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
	PartRelationInfo *prel;
	char		sql[] = "SELECT pg_class.relfilenode, pg_attribute.attnum, pathman_config.parttype, pg_attribute.atttypid "
						"FROM %s.pathman_config "
						"JOIN pg_class ON pg_class.relfilenode = pathman_config.relname::regclass::oid "
						"JOIN pg_attribute ON pg_attribute.attname = pathman_config.attname "
						"AND attrelid = pg_class.relfilenode";
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
			HeapTuple tuple = tuptable->vals[i];
			int oid = DatumGetObjectId(SPI_getbinval(tuple, tupdesc, 1, &isnull));

			prel = (PartRelationInfo*)
				hash_search(relations, (const void *)&oid, HASH_ENTER, NULL);
			prel->oid = oid;
			prel->attnum = DatumGetInt32(SPI_getbinval(tuple, tupdesc, 2, &isnull));
			prel->parttype = DatumGetInt32(SPI_getbinval(tuple, tupdesc, 3, &isnull));
			prel->atttype = DatumGetObjectId(SPI_getbinval(tuple, tupdesc, 4, &isnull));

			part_oids = lappend_int(part_oids, oid);
		}
	}
	pfree(query);

	/* Load children information */
	foreach(lc, part_oids)
	{
		Oid oid = (int) lfirst_int(lc);

		prel = (PartRelationInfo*)
			hash_search(relations, (const void *)&oid, HASH_FIND, NULL);	

		switch(prel->parttype)
		{
			case PT_RANGE:
				if (reinitialize && prel->children.length > 0)
				{
					RangeRelation *rangerel = (RangeRelation *)
						hash_search(range_restrictions, (void *) &oid, HASH_FIND, NULL);
					free_dsm_array(&prel->children);
					free_dsm_array(&rangerel->ranges);
					prel->children_count = 0;
				}
				load_check_constraints(oid);
				break;
			case PT_HASH:
				if (reinitialize && prel->children.length > 0)
				{
					free_dsm_array(&prel->children);
					prel->children_count = 0;
				}
				load_check_constraints(oid);
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
	ctl.keysize = sizeof(int);
	ctl.entrysize = sizeof(PartRelationInfo);

	/* Already exists, recreate */
	if (relations != NULL)
		hash_destroy(relations);

	relations = ShmemInitHash("Partitioning relation info", 1024, &ctl, HASH_ELEM);
}

/*
 * Load and validate constraints
 */
void
load_check_constraints(Oid parent_oid)
{
	bool		found;
	PartRelationInfo *prel;
	RangeRelation *rangerel;
	int ret;
	int i;
	int proc;

	Datum vals[1];
	Oid oids[1] = {INT4OID};
	bool nulls[1] = {false};
	vals[0] = Int32GetDatum(parent_oid);

	prel = (PartRelationInfo*)
		hash_search(relations, (const void *) &parent_oid, HASH_FIND, &found);

	/* Skip if already loaded */
	if (prel->children.length > 0)
		return;

	ret = SPI_execute_with_args("select pg_constraint.* "
								"from pg_constraint "
								"join pg_inherits on inhrelid = conrelid "
								"where inhparent = $1 and contype='c';",
								1, oids, vals, nulls, true, 0);
	proc = SPI_processed;

	if (ret > 0 && SPI_tuptable != NULL)
	{
		SPITupleTable *tuptable = SPI_tuptable;
		Oid *children;
		RangeEntry *ranges;
		Datum min;
		Datum max;
		int hash;

		alloc_dsm_array(&prel->children, sizeof(Oid), proc);
		children = (Oid *) dsm_array_get_pointer(&prel->children);

		if (prel->parttype == PT_RANGE)
		{
			rangerel = (RangeRelation *)
				hash_search(range_restrictions, (void *) &parent_oid, HASH_ENTER, &found);

			alloc_dsm_array(&rangerel->ranges, sizeof(RangeEntry), proc);
			ranges = (RangeEntry *) dsm_array_get_pointer(&rangerel->ranges);
		}

		for (i=0; i<proc; i++)
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
						elog(WARNING, "Range constraint for relation %u MUST have exact format: "
									  "VARIABLE >= CONST AND VARIABLE < CONST. Skipping...",
							 (Oid) con->conrelid);
						continue;
					}

					re.child_oid = con->conrelid;
					re.min = min;
					re.max = max;

					ranges[i] = re;
					break;
			
				case PT_HASH:
					if (!validate_hash_constraint(expr, prel, &hash))
					{
						elog(WARNING, "Hash constraint for relation %u MUST have exact format: "
									  "VARIABLE %% CONST = CONST. Skipping...",
							 (Oid) con->conrelid);
						continue;
					}
					children[hash] = con->conrelid;
			}
		}
		prel->children_count = proc;

		if (prel->parttype == PT_RANGE)
		{
			/* Sort ascending */
			qsort(ranges, proc, sizeof(RangeEntry), cmp_range_entries);

			/* Copy oids to prel */
			for(i=0; i < proc; i++)
				children[i] = ranges[i].child_oid;
		}

		/* Check if some ranges overlap */
		for(i=0; i < proc-1; i++)
		{
			if (ranges[i].max > ranges[i+1].min)
			{
				elog(WARNING, "Partitions %u and %u overlap. Disabling pathman for relation %u..",
					 ranges[i].child_oid, ranges[i+1].child_oid, parent_oid);
				hash_search(relations, (const void *) &parent_oid, HASH_REMOVE, &found);
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

	if (v1->min < v2->min)
		return -1;
	if (v1->min > v2->min)
		return 1;
	return 0;
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
	if ( !(IsA(expr, BoolExpr) && boolexpr->boolop == AND_EXPR) )
		return false;

	/* and it should have exactly two operands */
	if (list_length(boolexpr->args) != 2)
		return false;

	tce = lookup_type_cache(prel->atttype, TYPECACHE_EQ_OPR | TYPECACHE_LT_OPR | TYPECACHE_GT_OPR);

	/* check that left operand is >= operator */
	opexpr = (OpExpr *) linitial(boolexpr->args);
	if (get_op_opfamily_strategy(opexpr->opno, tce->btree_opf) == BTGreaterEqualStrategyNumber)
	{
		Node *left = linitial(opexpr->args);
		Node *right = lsecond(opexpr->args);
		if ( !IsA(left, Var) || !IsA(right, Const) )
			return false;
		if ( ((Var*) left)->varattno != prel->attnum )
			return false;
		*min = ((Const*) right)->constvalue;
	}
	else
		return false;

	/* TODO: rewrite this */
	/* check that right operand is < operator */
	opexpr = (OpExpr *) lsecond(boolexpr->args);
	if (get_op_opfamily_strategy(opexpr->opno, tce->btree_opf) == BTLessStrategyNumber)
	{
		Node *left = linitial(opexpr->args);
		Node *right = lsecond(opexpr->args);
		if ( !IsA(left, Var) || !IsA(right, Const) )
			return false;
		if ( ((Var*) left)->varattno != prel->attnum )
			return false;
		*max = ((Const*) right)->constvalue;
	}
	else
		return false;

	return true;
}

/*
 * Validate hash constraint. It MUST have the exact format
 * VARIABLE % CONST = CONST
 */
static bool
validate_hash_constraint(Expr *expr, PartRelationInfo *prel, int *hash)
{
	OpExpr *eqexpr;
	OpExpr *modexpr;

	if (!IsA(expr, OpExpr))
		return false;
	eqexpr = (OpExpr *) expr;

	/* Is this an equality operator? */
	if (eqexpr->opno != Int4EqualOperator)
		return false;

	if (!IsA(linitial(eqexpr->args), OpExpr))
		return false;

	/* Is this a modulus operator? */
	modexpr = (OpExpr *) linitial(eqexpr->args);
	if (modexpr->opno != 530)
		return false;

	if (list_length(modexpr->args) == 2)
	{
		Node *left = linitial(modexpr->args);
		Node *right = lsecond(modexpr->args);
		Const *mod_result;

		if ( !IsA(left, Var) || !IsA(right, Const) )
			return false;
		if ( ((Var*) left)->varattno != prel->attnum )
			return false;
		if (DatumGetInt32(((Const*) right)->constvalue) != prel->children.length)
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
	ctl.keysize = sizeof(int);
	ctl.entrysize = sizeof(RangeRelation);
	range_restrictions = ShmemInitHash("pg_pathman range restrictions",
									   1024, &ctl, HASH_ELEM | HASH_BLOBS);
}

/*
 * Remove partitions
 */
void
remove_relation_info(Oid relid)
{
	PartRelationInfo   *prel;
	RangeRelation	   *rangerel;

	prel = (PartRelationInfo *)
		hash_search(relations, (const void *) &relid, HASH_FIND, 0);

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
			rangerel = (RangeRelation *)
				hash_search(range_restrictions, (const void *) &relid, HASH_FIND, 0);
			free_dsm_array(&rangerel->ranges);
			free_dsm_array(&prel->children);
			hash_search(range_restrictions, (const void *) &relid, HASH_REMOVE, 0);
			break;
	}
	prel->children_count = 0;
	hash_search(relations, (const void *) &relid, HASH_REMOVE, 0);
}