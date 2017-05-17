/* ------------------------------------------------------------------------
 *
 * ref_integrity.c
 *		Referential integrity for partitioned tables
 *
 * Copyright (c) 2016, Postgres Professional
 *
 * ------------------------------------------------------------------------
 */

#include "c.h"
#include "postgres.h"
#include "miscadmin.h"

#include "access/htup_details.h"
#include "access/relscan.h"
#include "access/sysattr.h"
#include "access/xact.h"
#include "catalog/indexing.h"
#include "catalog/pg_am.h"
#include "catalog/pg_class.h"
#include "catalog/pg_constraint.h"
/* Constraint function were moved to pg_constraint_fn.h in version 9.6 */
#if PG_VERSION_NUM >= 90600
#include "catalog/pg_constraint_fn.h"
#endif
#include "catalog/pg_index.h"
#include "catalog/pg_type.h"
#include "catalog/pg_opclass.h"
#include "catalog/pg_operator.h"
#include "catalog/pg_trigger.h"
#include "commands/trigger.h"
#include "executor/spi.h"
#include "parser/parse_relation.h"
#include "parser/parse_coerce.h"
#include "storage/bufmgr.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/inval.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/tqual.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"
#include "utils/memutils.h"

#include "ref_integrity.h"
#include "relation_info.h"
#include "partition_filter.h"


#define MAX_QUOTED_NAME_LEN				(NAMEDATALEN*2+3)
#define MAX_QUOTED_REL_NAME_LEN			(MAX_QUOTED_NAME_LEN*2)

#define RI_INIT_CONSTRAINTHASHSIZE		64
#define RI_INIT_QUERYHASHSIZE 			(RI_INIT_CONSTRAINTHASHSIZE * 4)

#define RIAttName(rel, attnum)	NameStr(*attnumAttName(rel, attnum))

#define RI_TRIGTYPE_INSERT 1
#define RI_TRIGTYPE_UPDATE 2
#define RI_TRIGTYPE_DELETE 3

#define ERROR_SINGLE_COLUMN_KEY "relation %s must be partitioned by a single " \
								"column, not by an expression"


typedef struct ConstraintInfo
{
	Oid			constraint_id;	/* OID of pg_constraint entry (hash key) */
	bool		valid;			/* successfully initialized? */
	uint32		oidHashValue;	/* hash value of pg_constraint OID */
	NameData	conname;		/* name of the FK constraint */
	Oid			pk_relid;		/* referenced relation */
	Oid			fk_relid;		/* referencing relation */
	int16		pk_attnum;		/* attnum of referenced col */
	int16		fk_attnum;		/* attnum of referencing col */
	Oid			pf_eq_opr;		/* equality operator (PK = FK) */
	Oid			ff_eq_opr;		/* equality operator (FK = FK) */
} ConstraintInfo;

/* ----------
 * QueryKey
 *
 *	The key identifying a prepared SPI plan in our query hashtable
 * ----------
 */
typedef struct QueryKey
{
	Oid			relid;		/* OID of partitioned table */
	Oid			partid;		/* Partition OID */
} QueryKey;

/* ----------
 * QueryHashEntry
 * ----------
 */
typedef struct QueryHashEntry
{
	QueryKey	key;
	SPIPlanPtr	plan;
} QueryHashEntry;

/* ----------
 * RI_CompareKey
 *
 *	The key identifying an entry showing how to compare two values
 * ----------
 */
typedef struct CompareKey
{
	Oid			eq_opr;			/* the equality operator to apply */
	Oid			typeid;			/* the data type to apply it to */
} CompareKey;

/* ----------
 * CompareHashEntry
 * ----------
 */
typedef struct CompareHashEntry
{
	CompareKey	key;
	bool		valid;			/* successfully initialized? */
	FmgrInfo	eq_opr_finfo;	/* call info for equality fn */
	FmgrInfo	cast_func_finfo;	/* in case we must coerce input */
} CompareHashEntry;


/* ----------
 * Static functions
 * ----------
 */
static void ri_CheckTrigger(FunctionCallInfo fcinfo, const char *funcname, int tgkind);
static Datum ri_fkey_check(TriggerData *trigdata);
static Datum ri_restrict(TriggerData *trigdata, bool is_upd);
static bool ri_KeysEqual(Relation rel, HeapTuple oldtup, HeapTuple newtup,
			 const ConstraintInfo *riinfo);
static bool ri_AttributesEqual(Oid eq_opr, Oid typeid,
				   Datum oldvalue, Datum newvalue);
static CompareHashEntry *ri_HashCompareOp(Oid eq_opr, Oid typeid);
static void BuildQueryKey(QueryKey *key, Oid relid, Oid partid);
static SPIPlanPtr FetchPreparedPlan(QueryKey *key);
static const ConstraintInfo *
ri_LoadConstraintInfo(Oid constraintOid);
static const ConstraintInfo *
ri_FetchConstraintInfo(Trigger *trigger, Relation trig_rel, bool rel_is_pk);

static void ri_InitHashTables(void);
static void InvalidateConstraintCacheCallBack(Datum arg, int cacheid, uint32 hashvalue);

static void ri_GenerateQual(StringInfo buf,
				const char *sep,
				const char *leftop, Oid leftoptype,
				Oid opoid,
				const char *rightop, Oid rightoptype);
static void ri_add_cast_to(StringInfo buf, Oid typid);
static SPIPlanPtr ri_PlanCheck(const char *querystr,
			 Oid argtype,
			 QueryKey *qkey,
			 Relation query_rel,
			 bool cache_plan);
static bool ri_PerformCheck(const ConstraintInfo *riinfo,
				SPIPlanPtr qplan,
				Relation fk_rel, Relation pk_rel,
				bool source_is_pk,
				HeapTuple tuple,
				bool detectNewRows, int expect_OK);
static void quoteOneName(char *buffer, const char *name);
static void quoteRelationName(char *buffer, Oid relid);
static void ri_ReportViolation(const ConstraintInfo *riinfo,
				   Oid rel,
				   Oid refRel,
				   Datum key,
				   Oid keytype,
				   bool onfk);
static void SavePreparedPlan(QueryKey *key, SPIPlanPtr plan);
static void create_foreign_key_internal(Oid fk_table,
							  AttrNumber fk_attnum,
							  Oid pk_table,
							  AttrNumber pk_attnum);
static void createForeignKeyTriggers(Relation rel, Oid refRelOid,
						 Oid constraintOid, Oid indexOid);
static Oid transformFkeyCheckAttrs(Relation pkrel, int16 attnum,
						Oid *opclass);
static void validateForeignKeyConstraint(char *conname,
							 Relation rel,
							 Relation pkrel,
							 Oid pkindOid,
							 Oid constraintOid);

/* ----------
 * PL functions
 * ----------
 */
PG_FUNCTION_INFO_V1(pathman_fkey_check_ins);
PG_FUNCTION_INFO_V1(pathman_fkey_check_upd);
PG_FUNCTION_INFO_V1(pathman_fkey_restrict_del);
PG_FUNCTION_INFO_V1(pathman_fkey_restrict_upd);
PG_FUNCTION_INFO_V1(create_foreign_key);

/* ----------
 * Local data
 * ----------
 */
static HTAB *ri_constraint_cache = NULL;
static HTAB *ri_query_cache = NULL;
static HTAB *ri_compare_cache = NULL;
static bool ri_triggers_enabled = true;


Datum
pathman_fkey_check_ins(PG_FUNCTION_ARGS)
{
	/*
	 * Check that this is a valid trigger call on the right time and event.
	 */
	ri_CheckTrigger(fcinfo, "pathman_fkey_check_ins", RI_TRIGTYPE_INSERT);

	/*
	 * Share code with UPDATE case.
	 */
	return ri_fkey_check((TriggerData *) fcinfo->context);
}

Datum
pathman_fkey_check_upd(PG_FUNCTION_ARGS)
{
	/*
	 * Check that this is a valid trigger call on the right time and event.
	 */
	ri_CheckTrigger(fcinfo, "pathman_fkey_check_upd", RI_TRIGTYPE_UPDATE);

	/*
	 * Share code with INSERT case.
	 */
	return ri_fkey_check((TriggerData *) fcinfo->context);
}

Datum
pathman_fkey_restrict_del(PG_FUNCTION_ARGS)
{
	/*
	 * Check that this is a valid trigger call on the right time and event.
	 */
	ri_CheckTrigger(fcinfo, "pathman_fkey_restrict_del", RI_TRIGTYPE_DELETE);

	return ri_restrict((TriggerData *) fcinfo->context, false);
}

Datum
pathman_fkey_restrict_upd(PG_FUNCTION_ARGS)
{
	/*
	 * Check that this is a valid trigger call on the right time and event.
	 */
	ri_CheckTrigger(fcinfo, "pathman_fkey_restrict_upd", RI_TRIGTYPE_UPDATE);

	return ri_restrict((TriggerData *) fcinfo->context, true);
}

static void
ri_CheckTrigger(FunctionCallInfo fcinfo, const char *funcname, int tgkind)
{
	TriggerData *trigdata = (TriggerData *) fcinfo->context;

	if (!CALLED_AS_TRIGGER(fcinfo))
		ereport(ERROR,
				(errcode(ERRCODE_E_R_I_E_TRIGGER_PROTOCOL_VIOLATED),
				 errmsg("function \"%s\" was not called by trigger manager", funcname)));

	/*
	 * Check proper event
	 */
	if (!TRIGGER_FIRED_AFTER(trigdata->tg_event) ||
		!TRIGGER_FIRED_FOR_ROW(trigdata->tg_event))
		ereport(ERROR,
				(errcode(ERRCODE_E_R_I_E_TRIGGER_PROTOCOL_VIOLATED),
			   errmsg("function \"%s\" must be fired AFTER ROW", funcname)));

	switch (tgkind)
	{
		case RI_TRIGTYPE_INSERT:
			if (!TRIGGER_FIRED_BY_INSERT(trigdata->tg_event))
				ereport(ERROR,
						(errcode(ERRCODE_E_R_I_E_TRIGGER_PROTOCOL_VIOLATED),
						 errmsg("function \"%s\" must be fired for INSERT", funcname)));
			break;
		case RI_TRIGTYPE_UPDATE:
			if (!TRIGGER_FIRED_BY_UPDATE(trigdata->tg_event))
				ereport(ERROR,
						(errcode(ERRCODE_E_R_I_E_TRIGGER_PROTOCOL_VIOLATED),
						 errmsg("function \"%s\" must be fired for UPDATE", funcname)));
			break;
		case RI_TRIGTYPE_DELETE:
			if (!TRIGGER_FIRED_BY_DELETE(trigdata->tg_event))
				ereport(ERROR,
						(errcode(ERRCODE_E_R_I_E_TRIGGER_PROTOCOL_VIOLATED),
						 errmsg("function \"%s\" must be fired for DELETE", funcname)));
			break;
	}
}

/* ----------
 * RI_FKey_check -
 *
 *	Check foreign key existence (combined for INSERT and UPDATE).
 * ----------
 */
static Datum
ri_fkey_check(TriggerData *trigdata)
{
	const PartRelationInfo *prel;
	const ConstraintInfo *riinfo;
	Relation	fk_rel;
	Relation	pk_rel;
	Relation	part_rel;
	HeapTuple	new_row;
	Buffer		new_row_buf;
	QueryKey	qkey;
	SPIPlanPtr	qplan;
	bool		isnull;

	Datum		value;
	Oid			pk_type;
	Oid			fk_type;
	Oid			partid;

	if (!ri_triggers_enabled)
		return PointerGetDatum(NULL);

	/*
	 * Get arguments.
	 */
	riinfo = ri_FetchConstraintInfo(trigdata->tg_trigger,
									trigdata->tg_relation, false);

	if (TRIGGER_FIRED_BY_UPDATE(trigdata->tg_event))
	{
		new_row = trigdata->tg_newtuple;
		new_row_buf = trigdata->tg_newtuplebuf;
	}
	else
	{
		new_row = trigdata->tg_trigtuple;
		new_row_buf = trigdata->tg_trigtuplebuf;
	}

	/*
	 * We should not even consider checking the row if it is no longer valid,
	 * since it was either deleted (so the deferred check should be skipped)
	 * or updated (in which case only the latest version of the row should be
	 * checked).  Test its liveness according to SnapshotSelf.  We need pin
	 * and lock on the buffer to call HeapTupleSatisfiesVisibility.  Caller
	 * should be holding pin, but not lock.
	 */
	LockBuffer(new_row_buf, BUFFER_LOCK_SHARE);
	if (!HeapTupleSatisfiesVisibility(new_row, SnapshotSelf, new_row_buf))
	{
		LockBuffer(new_row_buf, BUFFER_LOCK_UNLOCK);
		return PointerGetDatum(NULL);
	}
	LockBuffer(new_row_buf, BUFFER_LOCK_UNLOCK);

	/*
	 * Get the relation descriptors of the FK and PK tables.
	 *
	 * pk_rel is opened in RowShareLock mode since that's what our eventual
	 * SELECT FOR KEY SHARE will get on it.
	 */
	fk_rel = trigdata->tg_relation;
	pk_rel = heap_open(riinfo->pk_relid, RowShareLock);

	/* Get partitioning info */
	prel = get_pathman_relation_info(pk_rel->rd_id);
	shout_if_prel_is_invalid(pk_rel->rd_id, prel, PT_ANY);

	pk_type = attnumTypeId(pk_rel, riinfo->pk_attnum);
	fk_type = attnumTypeId(fk_rel, riinfo->fk_attnum);

	value = heap_getattr(new_row, riinfo->fk_attnum, fk_rel->rd_att, &isnull);

	/* If foreign key value is NULL then just quit */
	if (isnull)
	{
		heap_close(pk_rel, RowShareLock);
		return PointerGetDatum(NULL);
	}

	/* Is there partition for the key? */
	partid = find_partition_for_value(value, fk_type, prel);
	if (partid == InvalidOid)
		ri_ReportViolation(riinfo, fk_rel->rd_id, pk_rel->rd_id,
						   value, fk_type, true);

	part_rel = heap_open(partid, RowShareLock);

	if (SPI_connect() != SPI_OK_CONNECT)
		elog(ERROR, "SPI_connect failed");

	/*
	 * Fetch or prepare a saved plan for the real check
	 */
	BuildQueryKey(&qkey, riinfo->pk_relid, partid);

	if ((qplan = FetchPreparedPlan(&qkey)) == NULL)
	{
		StringInfoData querybuf;
		char		pkrelname[MAX_QUOTED_REL_NAME_LEN];
		char		attname[MAX_QUOTED_NAME_LEN];

		/* ----------
		 * The query string built is
		 *	SELECT 1 FROM ONLY <pktable> x WHERE pkatt1 = $1 FOR KEY SHARE OF x
		 * ----------
		 */
		initStringInfo(&querybuf);
		quoteRelationName(pkrelname, part_rel->rd_id);
		appendStringInfo(&querybuf, "SELECT 1 FROM ONLY %s x", pkrelname);

		quoteOneName(attname, RIAttName(pk_rel, riinfo->pk_attnum));
		ri_GenerateQual(&querybuf, "WHERE",
						attname, pk_type,
						riinfo->pf_eq_opr,
						"$1", fk_type);

		appendStringInfoString(&querybuf, " FOR KEY SHARE OF x");

		/* Prepare and save the plan */
		qplan = ri_PlanCheck(querybuf.data, pk_type, &qkey, part_rel, true);
	}

	/*
	 * Now check that foreign key exists in PK table
	 */
	ri_PerformCheck(riinfo, qplan,
					fk_rel, part_rel,
					false,
					new_row,
					false,
					SPI_OK_SELECT);

	if (SPI_finish() != SPI_OK_FINISH)
		elog(ERROR, "SPI_finish failed");

	heap_close(part_rel, RowShareLock);
	heap_close(pk_rel, RowShareLock);

	return PointerGetDatum(NULL);
}

static Datum
ri_restrict(TriggerData *trigdata, bool is_upd)
{
	const ConstraintInfo *riinfo;
	Relation	fk_rel;
	Relation	pk_rel;
	HeapTuple	old_row;
	QueryKey	qkey;
	SPIPlanPtr	qplan;
	Oid			pk_type;
	Oid			fk_type;

	if (!ri_triggers_enabled)
		return PointerGetDatum(NULL);

	/*
	 * Get arguments.
	 */
	riinfo = ri_FetchConstraintInfo(trigdata->tg_trigger,
									trigdata->tg_relation, true);

	fk_rel = heap_open(riinfo->fk_relid, RowShareLock);
	pk_rel = trigdata->tg_relation;
	old_row = trigdata->tg_trigtuple;

	pk_type = attnumTypeId(pk_rel, riinfo->pk_attnum);
	fk_type = attnumTypeId(fk_rel, riinfo->fk_attnum);

	/*
	 * No need to check anything if old and new keys are equal
	 */
	if (is_upd)
	{
		HeapTuple	new_row = trigdata->tg_newtuple;

		if (ri_KeysEqual(pk_rel, old_row, new_row, riinfo))
		{
			heap_close(fk_rel, RowShareLock);
			return PointerGetDatum(NULL);
		}
	}

	if (SPI_connect() != SPI_OK_CONNECT)
		elog(ERROR, "SPI_connect failed");

	/*
	 * Fetch or prepare a saved plan for the real check
	 */
	BuildQueryKey(&qkey, riinfo->fk_relid, InvalidOid);

	if ((qplan = FetchPreparedPlan(&qkey)) == NULL)
	{
		StringInfoData querybuf;
		char		fkrelname[MAX_QUOTED_REL_NAME_LEN];
		char		attname[MAX_QUOTED_NAME_LEN];

		/* ----------
		 * The query string built is
		 *	SELECT 1 FROM ONLY <pktable> x WHERE pkatt1 = $1 FOR KEY SHARE OF x
		 * ----------
		 */
		initStringInfo(&querybuf);
		quoteRelationName(fkrelname, fk_rel->rd_id);
		appendStringInfo(&querybuf, "SELECT 1 FROM ONLY %s x", fkrelname);

		quoteOneName(attname, RIAttName(fk_rel, riinfo->fk_attnum));
		ri_GenerateQual(&querybuf, "WHERE",
						attname, pk_type,
						riinfo->pf_eq_opr,
						"$1", fk_type);

		appendStringInfoString(&querybuf, " FOR KEY SHARE OF x");

		/* Prepare and save the plan */
		qplan = ri_PlanCheck(querybuf.data, fk_type, &qkey, fk_rel, true);
	}

	/*
	 * Now check that foreign key exists in PK table
	 */
	ri_PerformCheck(riinfo, qplan,
					fk_rel, pk_rel,
					true,
					old_row,
					false,
					SPI_OK_SELECT);

	if (SPI_finish() != SPI_OK_FINISH)
		elog(ERROR, "SPI_finish failed");

	heap_close(fk_rel, RowShareLock);

	return PointerGetDatum(NULL);
}

static bool
ri_KeysEqual(Relation rel, HeapTuple oldtup, HeapTuple newtup,
			 const ConstraintInfo *riinfo)
{
	TupleDesc	tupdesc = RelationGetDescr(rel);
	Datum		oldvalue;
	Datum		newvalue;
	AttrNumber	attnum = riinfo->fk_attnum;
	Oid			eq_opr = riinfo->ff_eq_opr;
	bool		isnull;

	/*
	 * Get one attribute's oldvalue. If it is NULL - they're not equal.
	 */
	oldvalue = heap_getattr(oldtup, attnum, tupdesc, &isnull);
	if (isnull)
		return false;

	/*
	 * Get one attribute's newvalue. If it is NULL - they're not equal.
	 */
	newvalue = heap_getattr(newtup, attnum, tupdesc, &isnull);
	if (isnull)
		return false;

	/*
	 * Compare them with the appropriate equality operator.
	 */
	if (!ri_AttributesEqual(eq_opr, attnumTypeId(rel, attnum),
							oldvalue, newvalue))
		return false;

	return true;
}

/* ----------
 * ri_AttributesEqual -
 *
 *	Call the appropriate equality comparison operator for two values.
 *
 *	NB: we have already checked that neither value is null.
 * ----------
 */
static bool
ri_AttributesEqual(Oid eq_opr, Oid typeid,
				   Datum oldvalue, Datum newvalue)
{
	CompareHashEntry *entry = ri_HashCompareOp(eq_opr, typeid);

	/* Do we need to cast the values? */
	if (OidIsValid(entry->cast_func_finfo.fn_oid))
	{
		oldvalue = FunctionCall3(&entry->cast_func_finfo,
								 oldvalue,
								 Int32GetDatum(-1),		/* typmod */
								 BoolGetDatum(false));	/* implicit coercion */
		newvalue = FunctionCall3(&entry->cast_func_finfo,
								 newvalue,
								 Int32GetDatum(-1),		/* typmod */
								 BoolGetDatum(false));	/* implicit coercion */
	}

	/*
	 * Apply the comparison operator.  We assume it doesn't care about
	 * collations.
	 */
	return DatumGetBool(FunctionCall2(&entry->eq_opr_finfo,
									  oldvalue, newvalue));
}

/*
 * See ri_HashCompareOp(...) in ri_triggers.c
 */
static CompareHashEntry *
ri_HashCompareOp(Oid eq_opr, Oid typeid)
{
	CompareKey	key;
	CompareHashEntry *entry;
	bool		found;

	/*
	 * On the first call initialize the hashtable
	 */
	if (!ri_compare_cache)
		ri_InitHashTables();

	/*
	 * Find or create a hash entry.  Note we're assuming RI_CompareKey
	 * contains no struct padding.
	 */
	key.eq_opr = eq_opr;
	key.typeid = typeid;
	entry = (CompareHashEntry *) hash_search(ri_compare_cache,
												(void *) &key,
												HASH_ENTER, &found);
	if (!found)
		entry->valid = false;

	if (!entry->valid)
	{
		Oid			lefttype,
					righttype,
					castfunc;
		CoercionPathType pathtype;

		/* We always need to know how to call the equality operator */
		fmgr_info_cxt(get_opcode(eq_opr), &entry->eq_opr_finfo,
					  TopMemoryContext);

		/*
		 * If we chose to use a cast from FK to PK type, we may have to apply
		 * the cast function to get to the operator's input type.
		 */
		op_input_types(eq_opr, &lefttype, &righttype);
		Assert(lefttype == righttype);
		if (typeid == lefttype)
			castfunc = InvalidOid;		/* simplest case */
		else
		{
			pathtype = find_coercion_pathway(lefttype, typeid,
											 COERCION_IMPLICIT,
											 &castfunc);
			if (pathtype != COERCION_PATH_FUNC &&
				pathtype != COERCION_PATH_RELABELTYPE)
			{
				if (!IsBinaryCoercible(typeid, lefttype))
					elog(ERROR, "no conversion function from %s to %s",
						 format_type_be(typeid),
						 format_type_be(lefttype));
			}
		}
		if (OidIsValid(castfunc))
			fmgr_info_cxt(castfunc, &entry->cast_func_finfo,
						  TopMemoryContext);
		else
			entry->cast_func_finfo.fn_oid = InvalidOid;
		entry->valid = true;
	}

	return entry;
}

static void
BuildQueryKey(QueryKey *key, Oid relid, Oid partid)
{
	/*
	 * We assume struct RI_QueryKey contains no padding bytes, else we'd need
	 * to use memset to clear them.
	 */
	key->relid = relid;
	key->partid = partid;
}


/*
 *	Lookup for a query key in our private hash table of prepared
 *	and saved SPI execution plans. Return the plan if found or NULL.
 */
static SPIPlanPtr
FetchPreparedPlan(QueryKey *key)
{
	QueryHashEntry *entry;
	SPIPlanPtr		plan;

	/*
	 * On the first call initialize the hashtable
	 */
	if (!ri_query_cache)
		ri_InitHashTables();

	/*
	 * Lookup for the key
	 */
	entry = (QueryHashEntry *) hash_search(ri_query_cache,
										   (void *) key,
										   HASH_FIND, NULL);
	if (entry == NULL)
		return NULL;

	/*
	 * Check whether the plan is still valid.  If it isn't, we don't want to
	 * simply rely on plancache.c to regenerate it; rather we should start
	 * from scratch and rebuild the query text too.  This is to cover cases
	 * such as table/column renames.  We depend on the plancache machinery to
	 * detect possible invalidations, though.
	 *
	 * CAUTION: this check is only trustworthy if the caller has already
	 * locked both FK and PK rels.
	 */
	plan = entry->plan;
	if (plan && SPI_plan_is_valid(plan))
		return plan;

	/*
	 * Otherwise we might as well flush the cached plan now, to free a little
	 * memory space before we make a new one.
	 */
	entry->plan = NULL;
	if (plan)
		SPI_freeplan(plan);

	return NULL;
}

/*
 * Fetch or create the ConstraintInfo struct for an FK constraint.
 */
static const ConstraintInfo *
ri_LoadConstraintInfo(Oid constraintOid)
{

#define read_constr_attribute(riinfo_attr, attr, attr_type)					   \
	do {																	   \
		adatum = SysCacheGetAttr(CONSTROID, tup, Anum_pg_constraint_##attr, &isNull); \
		if (isNull)															   \
			elog(ERROR, "null #attr for constraint %u", constraintOid);		   \
		arr = DatumGetArrayTypeP(adatum);	/* ensure not toasted */		   \
		if (ARR_NDIM(arr) != 1 || ARR_HASNULL(arr) || ARR_ELEMTYPE(arr) != attr_type) \
			elog(ERROR, "#attr is not a 1-D smallint array");				   \
		if (ARR_DIMS(arr)[0] != 1)											   \
			elog(ERROR, "foreign key constraint must have only one column");   \
		riinfo->riinfo_attr = ARR_DATA_PTR(arr)[0];							   \
		if ((Pointer) arr != DatumGetPointer(adatum))						   \
			pfree(arr);														   \
	} while (0)

	ConstraintInfo *riinfo;
	bool		found;
	HeapTuple	tup;
	Form_pg_constraint conForm;
	Datum		adatum;
	bool		isNull;
	ArrayType  *arr;

	/*
	 * On the first call initialize the hashtable
	 */
	if (!ri_constraint_cache)
		ri_InitHashTables();

	/*
	 * Find or create a hash entry.  If we find a valid one, just return it.
	 */
	riinfo = (ConstraintInfo *) hash_search(ri_constraint_cache,
										    (void *) &constraintOid,
										    HASH_ENTER, &found);
	if (!found)
		riinfo->valid = false;
	else if (riinfo->valid)
		return riinfo;

	/*
	 * Fetch the pg_constraint row so we can fill in the entry.
	 */
	tup = SearchSysCache1(CONSTROID, ObjectIdGetDatum(constraintOid));
	if (!HeapTupleIsValid(tup)) /* should not happen */
		elog(ERROR, "cache lookup failed for constraint %u", constraintOid);
	conForm = (Form_pg_constraint) GETSTRUCT(tup);

	if (conForm->contype != CONSTRAINT_FOREIGN) /* should not happen */
		elog(ERROR, "constraint %u is not a foreign key constraint",
			 constraintOid);

	/* And extract data */
	Assert(riinfo->constraint_id == constraintOid);
	riinfo->oidHashValue = GetSysCacheHashValue1(CONSTROID,
											ObjectIdGetDatum(constraintOid));
	memcpy(&riinfo->conname, &conForm->conname, sizeof(NameData));
	riinfo->pk_relid = conForm->confrelid;
	riinfo->fk_relid = conForm->conrelid;

	read_constr_attribute(fk_attnum, conkey, INT2OID);
	read_constr_attribute(pk_attnum, confkey, INT2OID);
	read_constr_attribute(pf_eq_opr, conpfeqop, OIDOID);
	read_constr_attribute(ff_eq_opr, conffeqop, OIDOID);

	ReleaseSysCache(tup);

	riinfo->valid = true;

	return riinfo;
}


/*
 * Fetch the ConstraintInfo struct for the trigger's FK constraint.
 */
static const ConstraintInfo *
ri_FetchConstraintInfo(Trigger *trigger, Relation trig_rel, bool rel_is_pk)
{
	Oid			constraintOid = trigger->tgconstraint;
	const ConstraintInfo *riinfo;

	/*
	 * Check that the FK constraint's OID is available; it might not be if
	 * we've been invoked via an ordinary trigger or an old-style "constraint
	 * trigger".
	 */
	if (!OidIsValid(constraintOid))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
		  errmsg("no pg_constraint entry for trigger \"%s\" on table \"%s\"",
				 trigger->tgname, RelationGetRelationName(trig_rel)),
				 errhint("Remove this referential integrity trigger and its mates, then do ALTER TABLE ADD CONSTRAINT.")));

	/* Find or create a hashtable entry for the constraint */
	riinfo = ri_LoadConstraintInfo(constraintOid);

	/* Do some easy cross-checks against the trigger call data */
	if (rel_is_pk)
	{
		/* XXX Probably we should check that trig_rel is a partition */
		if (riinfo->fk_relid != trigger->tgconstrrelid)
			elog(ERROR, "wrong pg_constraint entry for trigger \"%s\" on table \"%s\"",
				 trigger->tgname, RelationGetRelationName(trig_rel));
	}
	else
	{
		if (riinfo->fk_relid != RelationGetRelid(trig_rel) ||
			riinfo->pk_relid != trigger->tgconstrrelid)
			elog(ERROR, "wrong pg_constraint entry for trigger \"%s\" on table \"%s\"",
				 trigger->tgname, RelationGetRelationName(trig_rel));
	}

	return riinfo;
}


static void
ri_InitHashTables(void)
{
	HASHCTL		ctl;

	memset(&ctl, 0, sizeof(ctl));
	ctl.keysize = sizeof(Oid);
	ctl.entrysize = sizeof(ConstraintInfo);
	ri_constraint_cache = hash_create("pathman ri_constraint cache",
									  RI_INIT_CONSTRAINTHASHSIZE,
									  &ctl, HASH_ELEM | HASH_BLOBS);

	/* Arrange to flush cache on pg_constraint changes */
	CacheRegisterSyscacheCallback(CONSTROID,
								  InvalidateConstraintCacheCallBack,
								  (Datum) 0);

	memset(&ctl, 0, sizeof(ctl));
	ctl.keysize = sizeof(QueryKey);
	ctl.entrysize = sizeof(QueryHashEntry);
	ri_query_cache = hash_create("pathman ri query cache",
								 RI_INIT_QUERYHASHSIZE,
								 &ctl, HASH_ELEM | HASH_BLOBS);

	memset(&ctl, 0, sizeof(ctl));
	ctl.keysize = sizeof(CompareKey);
	ctl.entrysize = sizeof(CompareHashEntry);
	ri_compare_cache = hash_create("pathman ri compare cache",
								   RI_INIT_QUERYHASHSIZE,
								   &ctl, HASH_ELEM | HASH_BLOBS);

}

static void
InvalidateConstraintCacheCallBack(Datum arg, int cacheid, uint32 hashvalue)
{
	HASH_SEQ_STATUS		hash_seq;
	ConstraintInfo	   *riinfo;

	hash_seq_init(&hash_seq, ri_constraint_cache);
	while ((riinfo = hash_seq_search(&hash_seq)) != NULL)
	{
		/*
		 * If all constraints or the current one are invalidated
		 */
		if (hashvalue == 0 || riinfo->oidHashValue == hashvalue)
			riinfo->valid = false;
	}
}

/*
 * ri_GenerateQual --- generate a WHERE clause equating two variables
 *
 * The idea is to append " sep leftop op rightop" to buf.  The complexity
 * comes from needing to be sure that the parser will select the desired
 * operator.  We always name the operator using OPERATOR(schema.op) syntax
 * (readability isn't a big priority here), so as to avoid search-path
 * uncertainties.  We have to emit casts too, if either input isn't already
 * the input type of the operator; else we are at the mercy of the parser's
 * heuristics for ambiguous-operator resolution.
 */
static void
ri_GenerateQual(StringInfo buf,
				const char *sep,
				const char *leftop, Oid leftoptype,
				Oid opoid,
				const char *rightop, Oid rightoptype)
{
	HeapTuple	opertup;
	Form_pg_operator operform;
	char	   *oprname;
	char	   *nspname;

	opertup = SearchSysCache1(OPEROID, ObjectIdGetDatum(opoid));
	if (!HeapTupleIsValid(opertup))
		elog(ERROR, "cache lookup failed for operator %u", opoid);
	operform = (Form_pg_operator) GETSTRUCT(opertup);
	Assert(operform->oprkind == 'b');
	oprname = NameStr(operform->oprname);

	nspname = get_namespace_name(operform->oprnamespace);

	appendStringInfo(buf, " %s %s", sep, leftop);
	if (leftoptype != operform->oprleft)
		ri_add_cast_to(buf, operform->oprleft);
	appendStringInfo(buf, " OPERATOR(%s.", quote_identifier(nspname));
	appendStringInfoString(buf, oprname);
	appendStringInfo(buf, ") %s", rightop);
	if (rightoptype != operform->oprright)
		ri_add_cast_to(buf, operform->oprright);

	ReleaseSysCache(opertup);
}

/*
 * Add a cast specification to buf.  We spell out the type name the hard way,
 * intentionally not using format_type_be().  This is to avoid corner cases
 * for CHARACTER, BIT, and perhaps other types, where specifying the type
 * using SQL-standard syntax results in undesirable data truncation.  By
 * doing it this way we can be certain that the cast will have default (-1)
 * target typmod.
 */
static void
ri_add_cast_to(StringInfo buf, Oid typid)
{
	HeapTuple	typetup;
	Form_pg_type typform;
	char	   *typname;
	char	   *nspname;

	typetup = SearchSysCache1(TYPEOID, ObjectIdGetDatum(typid));
	if (!HeapTupleIsValid(typetup))
		elog(ERROR, "cache lookup failed for type %u", typid);
	typform = (Form_pg_type) GETSTRUCT(typetup);

	typname = NameStr(typform->typname);
	nspname = get_namespace_name(typform->typnamespace);

	appendStringInfo(buf, "::%s.%s",
					 quote_identifier(nspname), quote_identifier(typname));

	ReleaseSysCache(typetup);
}

/*
 * Prepare execution plan for a query to enforce an RI restriction
 *
 * If cache_plan is true, the plan is saved into our plan hashtable
 * so that we don't need to plan it again.
 */
static SPIPlanPtr
ri_PlanCheck(const char *querystr,
			 Oid argtype,
			 QueryKey *qkey,
			 Relation query_rel,
			 bool cache_plan)
{
	SPIPlanPtr	qplan;
	Oid			save_userid;
	int			save_sec_context;
	Oid			argtypes[1] = {argtype};

	/* Switch to proper UID to perform check as */
	GetUserIdAndSecContext(&save_userid, &save_sec_context);
	SetUserIdAndSecContext(RelationGetForm(query_rel)->relowner,
						   save_sec_context | SECURITY_LOCAL_USERID_CHANGE |
						   SECURITY_NOFORCE_RLS);

	/* Create the plan */
	qplan = SPI_prepare(querystr, 1, argtypes);

	if (qplan == NULL)
		elog(ERROR, "SPI_prepare returned %d for %s", SPI_result, querystr);

	/* Restore UID and security context */
	SetUserIdAndSecContext(save_userid, save_sec_context);

	/* Save the plan if requested */
	if (cache_plan)
	{
		SPI_keepplan(qplan);
		SavePreparedPlan(qkey, qplan);
	}

	return qplan;
}

/*
 * Perform a query to enforce an RI restriction
 */
static bool
ri_PerformCheck(const ConstraintInfo *riinfo,
				SPIPlanPtr qplan,
				Relation fk_rel, Relation pk_rel,
				bool source_is_pk,
				HeapTuple tuple,
				bool detectNewRows, int expect_OK)
{
	Relation	query_rel,
				source_rel;
	Snapshot	test_snapshot;
	Snapshot	crosscheck_snapshot;
	int			limit;
	int			spi_result;
	Oid			save_userid;
	int			save_sec_context;
	Datum		key[1];
	char		nulls[1];
	Oid			keytype;
	bool		isnull = false;
	AttrNumber	attnum;

	if (source_is_pk)
	{
		query_rel = fk_rel;
		source_rel = pk_rel;
		attnum = riinfo->pk_attnum;
	}
	else
	{
		query_rel = pk_rel;
		source_rel = fk_rel;
		attnum = riinfo->fk_attnum;
	}

	key[0] = heap_getattr(tuple, attnum, source_rel->rd_att, &isnull);
	nulls[0] = isnull ? 'n' : ' ';

	Assert(attnum <= source_rel->rd_att->natts);
	keytype = source_rel->rd_att->attrs[attnum-1]->atttypid;

	/*
	 * In READ COMMITTED mode, we just need to use an up-to-date regular
	 * snapshot, and we will see all rows that could be interesting. But in
	 * transaction-snapshot mode, we can't change the transaction snapshot. If
	 * the caller passes detectNewRows == false then it's okay to do the query
	 * with the transaction snapshot; otherwise we use a current snapshot, and
	 * tell the executor to error out if it finds any rows under the current
	 * snapshot that wouldn't be visible per the transaction snapshot.  Note
	 * that SPI_execute_snapshot will register the snapshots, so we don't need
	 * to bother here.
	 */
	if (IsolationUsesXactSnapshot() && detectNewRows)
	{
		CommandCounterIncrement();		/* be sure all my own work is visible */
		test_snapshot = GetLatestSnapshot();
		crosscheck_snapshot = GetTransactionSnapshot();
	}
	else
	{
		/* the default SPI behavior is okay */
		test_snapshot = InvalidSnapshot;
		crosscheck_snapshot = InvalidSnapshot;
	}

	/*
	 * If this is a select query (e.g., for a 'no action' or 'restrict'
	 * trigger), we only need to see if there is a single row in the table,
	 * matching the key.  Otherwise, limit = 0 - because we want the query to
	 * affect ALL the matching rows.
	 */
	limit = (expect_OK == SPI_OK_SELECT) ? 1 : 0;

	/* Switch to proper UID to perform check as */
	GetUserIdAndSecContext(&save_userid, &save_sec_context);
	SetUserIdAndSecContext(RelationGetForm(query_rel)->relowner,
						   save_sec_context | SECURITY_LOCAL_USERID_CHANGE |
						   SECURITY_NOFORCE_RLS);

	/* Finally we can run the query. */
	spi_result = SPI_execute_snapshot(qplan,
									  key, nulls,
									  test_snapshot, crosscheck_snapshot,
									  false, false, limit);

	/* Restore UID and security context */
	SetUserIdAndSecContext(save_userid, save_sec_context);

	/* Check result */
	if (spi_result < 0)
		elog(ERROR, "SPI_execute_snapshot returned %d", spi_result);

	/* Result isn't the one we expected */
	if (expect_OK >= 0 && spi_result != expect_OK)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("referential integrity query on \"%s\" gave unexpected result",
						RelationGetRelationName(query_rel)),
				 errhint("This is most likely due to a rule having rewritten the query.")));

	/* XXX wouldn't it be clearer to do this part at the caller? */
	if ((SPI_processed == 0) != source_is_pk)
		ri_ReportViolation(riinfo, source_rel->rd_id, query_rel->rd_id,
						   key[0], keytype, !source_is_pk);

	return SPI_processed != 0;
}

/*
 * quoteOneName --- safely quote a single SQL name
 *
 * buffer must be MAX_QUOTED_NAME_LEN long (includes room for \0)
 */
static void
quoteOneName(char *buffer, const char *name)
{
	/* Rather than trying to be smart, just always quote it. */
	*buffer++ = '"';
	while (*name)
	{
		if (*name == '"')
			*buffer++ = '"';
		*buffer++ = *name++;
	}
	*buffer++ = '"';
	*buffer = '\0';
}

/*
 * quoteRelationName --- safely quote a fully qualified relation name
 *
 * buffer must be MAX_QUOTED_REL_NAME_LEN long (includes room for \0)
 */
static void
quoteRelationName(char *buffer, Oid relid)
{
	quoteOneName(buffer, get_namespace_name(get_rel_namespace(relid)));
	buffer += strlen(buffer);
	*buffer++ = '.';
	quoteOneName(buffer, get_rel_name(relid));
}

static void
ri_ReportViolation(const ConstraintInfo *riinfo,
				   Oid rel,
				   Oid refRel,
				   Datum key,
				   Oid keytype,
				   bool onfk)
{
	if (onfk)
		ereport(ERROR,
			(errcode(ERRCODE_FOREIGN_KEY_VIOLATION),
			 errmsg("insert or update on table \"%s\" violates foreign key constraint \"%s\"",
					get_rel_name(rel),
					NameStr(riinfo->conname)),
			 errdetail("Key (%s)=(%s) is not present in table \"%s\".",
			 	get_attname(riinfo->pk_relid, riinfo->pk_attnum),
			 	datum_to_cstring(key, keytype),
			 	get_rel_name(refRel))));
	else
		ereport(ERROR,
			(errcode(ERRCODE_FOREIGN_KEY_VIOLATION),
			 errmsg("update or delete on table \"%s\" violates foreign key constraint \"%s\" on table \"%s\"",
					get_rel_name(rel),
					NameStr(riinfo->conname),
					get_rel_name(refRel)),
			 errdetail("Key (%s)=(%s) is still referenced from table \"%s\".",
			 	get_attname(riinfo->fk_relid, riinfo->fk_attnum),
			 	datum_to_cstring(key, keytype),
			 	get_rel_name(refRel))));
}

static void
SavePreparedPlan(QueryKey *key, SPIPlanPtr plan)
{
	QueryHashEntry *entry;
	bool			found;

	/*
	 * On the first call initialize the hashtable
	 */
	if (!ri_query_cache)
		ri_InitHashTables();

	/*
	 * Add the new plan.  We might be overwriting an entry previously found
	 * invalid by ri_FetchPreparedPlan.
	 */
	entry = (QueryHashEntry *) hash_search(ri_query_cache,
										   (void *) key,
										   HASH_ENTER, &found);
	Assert(!found || entry->plan == NULL);
	entry->plan = plan;
}

/*
 * PL-function for foreign key creation
 */
Datum
create_foreign_key(PG_FUNCTION_ARGS)
{
	const PartRelationInfo *prel;
	Oid						fk_table = PG_GETARG_OID(0),
							pk_table = PG_GETARG_OID(2);
	char				   *fk_attr = TextDatumGetCString(PG_GETARG_TEXT_P(1));
	AttrNumber				fk_attnum = get_attnum(fk_table, fk_attr),
							pk_attnum;

	prel = get_pathman_relation_info(pk_table);
	if (!prel)
		elog(ERROR,
			 "table %s isn't partitioned by pg_pathman",
			 get_rel_name(pk_table));

	if ((pk_attnum = var_get_attnum(prel->expr)) == InvalidOid)
		elog(ERROR, ERROR_SINGLE_COLUMN_KEY,
			 get_rel_name_or_relid(pk_table));

	create_foreign_key_internal(fk_table, fk_attnum, pk_table, pk_attnum);

	PG_RETURN_VOID();
}

static void
create_foreign_key_internal(Oid fk_table,
							  AttrNumber fk_attnum,
							  Oid pk_table,
							  AttrNumber pk_attnum)
{
	Oid			indexOid;
	Oid			opclass;
	Oid			fktypoid = get_atttype(fk_table, fk_attnum);
	Oid			fktyped;
	char	   *fkname;
	Oid			constrOid;

	Relation	fkrel;
	Relation	pkrel;

	HeapTuple	cla_ht;
	Form_pg_opclass cla_tup;
	Oid			amid;
	Oid			pfeqop;
	Oid			ppeqop;
	Oid			ffeqop;

	Oid			opfamily;
	Oid			opcintype;

	fkrel = heap_open(fk_table, ShareRowExclusiveLock);
	pkrel = heap_open(pk_table, ShareRowExclusiveLock);

	fkname = ChooseConstraintName(RelationGetRelationName(fkrel),
								  get_attname(fk_table, fk_attnum),
								  "fkey",
								  RelationGetNamespace(fkrel),
								  NIL);

	indexOid = transformFkeyCheckAttrs(pkrel, pk_attnum, &opclass);

	/* We need several fields out of the pg_opclass entry */
	cla_ht = SearchSysCache1(CLAOID, ObjectIdGetDatum(opclass));
	if (!HeapTupleIsValid(cla_ht))
		elog(ERROR, "cache lookup failed for opclass %u", opclass);
	cla_tup = (Form_pg_opclass) GETSTRUCT(cla_ht);
	amid = cla_tup->opcmethod;
	opfamily = cla_tup->opcfamily;
	opcintype = cla_tup->opcintype;
	ReleaseSysCache(cla_ht);

	if (amid != BTREE_AM_OID)
		elog(ERROR, "only b-tree indexes are supported for foreign keys");

	/* Operator for PK = PK comparisons */
	ppeqop = get_opfamily_member(opfamily, opcintype, opcintype,
								 BTEqualStrategyNumber);
	if (!OidIsValid(ppeqop))
		elog(ERROR, "missing operator %d(%u,%u) in opfamily %u",
			 BTEqualStrategyNumber, opcintype, opcintype, opfamily);

	/*
	 * Are there equality operators that take exactly the FK type? Assume
	 * we should look through any domain here.
	 */
	fktyped = getBaseType(fktypoid);

	pfeqop = get_opfamily_member(opfamily, opcintype, fktyped,
								 BTEqualStrategyNumber);
	if (OidIsValid(pfeqop))
	{
		ffeqop = get_opfamily_member(opfamily, fktyped, fktyped,
									 BTEqualStrategyNumber);
	}
	else
	{
		ereport(ERROR,
				(errcode(ERRCODE_DATATYPE_MISMATCH),
				 errmsg("foreign key constraint \"%s\" "
						"cannot be implemented",
						fkname)));
	}

	/*
	 * Record the FK constraint in pg_constraint.
	 */
	constrOid = CreateConstraintEntry(fkname,
									  RelationGetNamespace(fkrel),
									  CONSTRAINT_FOREIGN,
									  false,
									  false,
									  true,	/* initially_valid */
									  fk_table,
									  &fk_attnum,
									  1,
									  InvalidOid,		/* not a domain
														 * constraint */
									  indexOid,
									  pk_table,
									  &pk_attnum,
									  &pfeqop,
									  &ppeqop,
									  &ffeqop,
									  1,
									  'a',
									  'a',
									  FKCONSTR_MATCH_FULL,
									  NULL,		/* no exclusion constraint */
									  NULL,		/* no check constraint */
									  NULL,
									  NULL,
									  true,		/* islocal */
									  0,		/* inhcount */
									  true,		/* isnoinherit */
									  false);	/* is_internal */

	/* Make changes-so-far visible */
	CommandCounterIncrement();

	/* Add RI triggers */
	createForeignKeyTriggers(fkrel, pk_table, constrOid, indexOid);

	/* Go through fk table and apply RI trigger on each row */
	validateForeignKeyConstraint(fkname, fkrel, pkrel, indexOid, constrOid);

	heap_close(fkrel, ShareRowExclusiveLock);
	heap_close(pkrel, ShareRowExclusiveLock);
}

static void
createForeignKeyTriggers(Relation rel, Oid refRelOid,
						 Oid constraintOid, Oid indexOid)
{
#define pathman_funcname(funcname)					\
	list_make2(makeString(pathman_schema_name),		\
			   makeString(funcname))

	const PartRelationInfo *prel;
	Oid						myRelOid;
	Oid						pathman_schema = get_pathman_schema();
	char				   *pathman_schema_name = get_namespace_name(pathman_schema);
	List				   *funcname_del,
						   *funcname_upd;
	Oid					   *children;
	uint32					nchildren;
	int						i;
	AttrNumber				attnum;
	const char			   *attname;

	myRelOid = RelationGetRelid(rel);

	/*
	 * XXX refRelOid is a partitioned PK table for now. But it may change
	 * in future when we implement FK on partitioned table referencing a regular
	 * one
	 */
	prel = get_pathman_relation_info(refRelOid);
	if (!prel)
		elog(ERROR,
			 "table %s isn't partitioned by pg_pathman",
			 get_rel_name(refRelOid));

	if ((attnum = var_get_attnum(prel->expr)) == InvalidOid)
		elog(ERROR, ERROR_SINGLE_COLUMN_KEY,
			 get_rel_name_or_relid(refRelOid));
	attname = get_attname(refRelOid, attnum);

	/*
	 * Build and execute a CREATE CONSTRAINT TRIGGER statement for the ON
	 * INSERT action on the FK table.
	 */
	createSingleForeignKeyTrigger(myRelOid, refRelOid,
								  pathman_funcname("pathman_fkey_check_ins"),
								  "RI_ConstraintTrigger_c",
								  TRIGGER_TYPE_INSERT,
								  constraintOid,
								  indexOid,
								  true);

	/*
	 * Build and execute a CREATE CONSTRAINT TRIGGER statement for the ON
	 * INSERT action on the FK table.
	 */
	createSingleForeignKeyTrigger(myRelOid, refRelOid,
								  pathman_funcname("pathman_fkey_check_upd"),
								  "RI_ConstraintTrigger_c",
								  TRIGGER_TYPE_UPDATE,
								  constraintOid,
								  indexOid,
								  true);

	nchildren = PrelChildrenCount(prel);
	children = PrelGetChildrenArray(prel);

	/*
	 * Create ON DELETE triggers on each partition
	 */
	funcname_del = pathman_funcname("pathman_fkey_restrict_del");
	funcname_upd = pathman_funcname("pathman_fkey_restrict_upd");
	for (i = 0; i < nchildren; i++)
	{
		AttrNumber	child_attnum = get_attnum(children[i], attname);

		createPartitionForeignKeyTriggers(children[i],
										  child_attnum,
										  myRelOid,
										  constraintOid,
										  funcname_upd,
										  funcname_del);
	}
}

static Oid
transformFkeyCheckAttrs(Relation pkrel, int16 attnum,
						Oid *opclass) /* output parameter */
{
	Oid			indexoid = InvalidOid;
	HeapTuple	indexTuple;
	Datum		indclassDatum;
	oidvector  *indclass;
	bool		isnull;

	*opclass = InvalidOid;
	indexTuple = get_index_for_key(pkrel, attnum, NULL);

	if (!HeapTupleIsValid(indexTuple))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_FOREIGN_KEY),
				 errmsg("there is no unique constraint matching given keys for referenced table \"%s\"",
						RelationGetRelationName(pkrel))));

	indclassDatum = SysCacheGetAttr(INDEXRELID, indexTuple,
									Anum_pg_index_indclass, &isnull);
	Assert(!isnull);

	indexoid = SysCacheGetAttr(INDEXRELID, indexTuple,
							   Anum_pg_index_indexrelid, &isnull);
	Assert(!isnull);
	indclass = (oidvector *) DatumGetPointer(indclassDatum);
	*opclass = indclass->values[0];
	ReleaseSysCache(indexTuple);

	Assert(OidIsValid(*opclass));

	return indexoid;
}

/*
 * Scan the existing rows in a table to verify they meet a proposed FK
 * constraint.
 */
static void
validateForeignKeyConstraint(char *conname,
							 Relation rel,
							 Relation pkrel,
							 Oid pkindOid,
							 Oid constraintOid)
{
	HeapScanDesc scan;
	HeapTuple	tuple;
	Trigger		trig;
	Snapshot	snapshot;

	/* Build a trigger call structure */
	MemSet(&trig, 0, sizeof(trig));
	trig.tgoid = InvalidOid;
	trig.tgname = conname;
	trig.tgenabled = TRIGGER_FIRES_ON_ORIGIN;
	trig.tgisinternal = TRUE;
	trig.tgconstrrelid = RelationGetRelid(pkrel);
	trig.tgconstrindid = pkindOid;
	trig.tgconstraint = constraintOid;
	trig.tgdeferrable = FALSE;
	trig.tginitdeferred = FALSE;
	/* we needn't fill in tgargs or tgqual */

	/*
	 * Scan through each tuple, calling RI_FKey_check_ins (insert trigger) as
	 * if that tuple had just been inserted.  If any of those fail, it should
	 * ereport(ERROR) and that's that.
	 */
	snapshot = RegisterSnapshot(GetLatestSnapshot());
	scan = heap_beginscan(rel, snapshot, 0, NULL);

	while ((tuple = heap_getnext(scan, ForwardScanDirection)) != NULL)
	{
		FunctionCallInfoData fcinfo;
		TriggerData trigdata;

		/*
		 * Make a call to the trigger function
		 *
		 * No parameters are passed, but we do set a context
		 */
		MemSet(&fcinfo, 0, sizeof(fcinfo));

		/*
		 * We assume RI_FKey_check_ins won't look at flinfo...
		 */
		trigdata.type = T_TriggerData;
		trigdata.tg_event = TRIGGER_EVENT_INSERT | TRIGGER_EVENT_ROW;
		trigdata.tg_relation = rel;
		trigdata.tg_trigtuple = tuple;
		trigdata.tg_newtuple = NULL;
		trigdata.tg_trigger = &trig;
		trigdata.tg_trigtuplebuf = scan->rs_cbuf;
		trigdata.tg_newtuplebuf = InvalidBuffer;

		fcinfo.context = (Node *) &trigdata;

		pathman_fkey_check_ins(&fcinfo);
	}

	heap_endscan(scan);
	UnregisterSnapshot(snapshot);
}

/*
 * Find all foreign keys where table is an FK side
 */
void
pathman_get_fkeys(Oid parent_relid, List **constraints, List **refrelids)
{
	Relation		pg_constraint_rel;
	HeapTuple		tuple;
	ScanKeyData		skey[2];
	SysScanDesc		scan;
	bool			isnull;

	*constraints = NIL;
	*refrelids = NIL;

	pg_constraint_rel = heap_open(ConstraintRelationId, RowExclusiveLock);

	/* Search by confrelid and contype = 'f' */
	ScanKeyInit(&skey[0],
				Anum_pg_constraint_confrelid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(parent_relid));

	ScanKeyInit(&skey[1],
				Anum_pg_constraint_contype,
				BTEqualStrategyNumber, F_CHAREQ,
				CharGetDatum('f'));

	scan = systable_beginscan(pg_constraint_rel, InvalidOid, false,
							  NULL, 2, skey);

	while ((tuple = systable_getnext(scan)) != NULL)
	{
		Form_pg_constraint con = (Form_pg_constraint) GETSTRUCT(tuple);
		Datum	oid = heap_getsysattr(tuple, ObjectIdAttributeNumber,
									  RelationGetDescr(pg_constraint_rel),
									  &isnull);

		*constraints = lappend_oid(*constraints, DatumGetObjectId(oid));
		*refrelids = lappend_oid(*refrelids, con->conrelid);
	}

	systable_endscan(scan);
	heap_close(pg_constraint_rel, RowExclusiveLock);
}

/*
 * Create RI triggers for partition. Also as a bonus function adds dependency
 * for index on FK constraint
 */
void
createPartitionForeignKeyTriggers(Oid partition,
								  AttrNumber attnum,
								  Oid fkrelid,
								  Oid constraintOid,
								  List *upd_funcname,
								  List *del_funcname)
{
	HeapTuple			indexTuple;
	Relation			childRel;
	// Oid					parent,
	Oid					indexOid;
	// PartParentSearch	parent_search;
	// PartRelationInfo   *prel;
	ObjectAddress		constrAddress;
	ObjectAddress		indexAddress;

	/*
	 * Foreign key can be created for only single attribute key. So first thing
	 * we must check is that partitioning key is a single column
	 */
	// parent = get_parent_of_partition(partition, &parent_search);
	// if (parent_search != PPS_ENTRY_PART_PARENT)
	// 	ereport(ERROR, (errmsg("relation %s is not a partition",
	// 						   get_rel_name_or_relid(partition))));
	// prel = get_pathman_relation_info(parent);
	// shout_if_prel_is_invalid(parent, prel, PT_ANY);

	// if ((attnum = VarGetAttnum(prel->expr)) == InvalidOid)
	// 	ereport(ERROR, (errmsg(ERROR_SINGLE_COLUMN_KEY,
	// 						   get_rel_name_or_relid(parent))));

	/* Lock partition so no one deletes rows until we're done */
	childRel = heap_open(partition, ShareRowExclusiveLock);

	/* Is there unique index on partition */
	indexTuple = get_index_for_key(childRel, attnum, &indexOid);

	if (!HeapTupleIsValid(indexTuple))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_FOREIGN_KEY),
				 errmsg("there is no unique constraint for partition \"%s\"",
						RelationGetRelationName(childRel))));

	createSingleForeignKeyTrigger(partition, fkrelid, del_funcname,
								  "RI_ConstraintTrigger_a",
								  TRIGGER_TYPE_DELETE,
								  constraintOid,
								  indexOid,
								  true);

	createSingleForeignKeyTrigger(partition, fkrelid, upd_funcname,
								  "RI_ConstraintTrigger_a",
								  TRIGGER_TYPE_UPDATE,
								  constraintOid,
								  indexOid,
								  true);

	ReleaseSysCache(indexTuple);

	/*
	 * Add index to pg_depend
	 *
	 * XXX Probably we should do it outside this function
	 */
	ObjectAddressSet(constrAddress, ConstraintRelationId, constraintOid);
	ObjectAddressSet(indexAddress, RelationRelationId, indexOid);
	recordDependencyOn(&constrAddress, &indexAddress, DEPENDENCY_NORMAL);

	/* TODO: Should we release lock? */
	heap_close(childRel, ShareRowExclusiveLock);
}

void
createSingleForeignKeyTrigger(Oid relOid, Oid refRelOid, List *funcname,
							  char *trigname, int16 events, Oid constraintOid,
							  Oid indexOid, bool is_internal)
{
	CreateTrigStmt *fk_trigger;
	ObjectAddress	trig_address,
					rel_address;

	fk_trigger = makeNode(CreateTrigStmt);
	fk_trigger->trigname = "RI_ConstraintTrigger_c";
	fk_trigger->relation = NULL;
	fk_trigger->row = true;
	fk_trigger->timing = TRIGGER_TYPE_AFTER;
	fk_trigger->events = events;
	fk_trigger->columns = NIL;
	fk_trigger->whenClause = NULL;
	fk_trigger->isconstraint = true;
	fk_trigger->constrrel = NULL;
	fk_trigger->deferrable = false;
	fk_trigger->initdeferred = false;
	fk_trigger->funcname = funcname;
	fk_trigger->args = NIL;

	trig_address = CreateTrigger(fk_trigger, NULL, relOid, refRelOid,
								 constraintOid, indexOid, is_internal);

	/* We make trigger be auto-dropped if its relation is dropped */
	rel_address.classId = RelationRelationId;
	rel_address.objectId = relOid;
	rel_address.objectSubId = 0;
	recordDependencyOn(&trig_address, &rel_address, DEPENDENCY_AUTO);

	/* Make changes-so-far visible */
	CommandCounterIncrement();
}

/*
 * Return UNIQUE INDEX tuple from pg_index corresponding to the relation
 *
 * Caller is responsible for releasing tuple
 */
HeapTuple
get_index_for_key(Relation rel, AttrNumber attnum, Oid *index_id)
{
	List	   *indexoidlist;
	ListCell   *indexoidscan;
	Oid			indexoid;

	/*
	 * Get the list of index OIDs for the table from the relcache, and look up
	 * each one in the pg_index syscache, and match unique indexes to the list
	 * of attnums we are given.
	 */
	indexoidlist = RelationGetIndexList(rel);

	foreach(indexoidscan, indexoidlist)
	{
		HeapTuple	indexTuple;
		Form_pg_index indexStruct;

		indexoid = lfirst_oid(indexoidscan);
		indexTuple = SearchSysCache1(INDEXRELID, ObjectIdGetDatum(indexoid));
		if (!HeapTupleIsValid(indexTuple))
			elog(ERROR, "cache lookup failed for index %u", indexoid);
		indexStruct = (Form_pg_index) GETSTRUCT(indexTuple);

		/*
		 * Must have the right number of columns; must be unique and not a
		 * partial index; forget it if there are any expressions, too. Invalid
		 * indexes are out as well.
		 *
		 * TODO: in postgres 10 there is indnkeyatts attribute we must check
		 * instead of indnatts. We should keep this in mind when we adapt
		 * to the newer version.
		 */
		if (indexStruct->indnatts == 1 &&
			indexStruct->indisunique &&
			IndexIsValid(indexStruct) &&
			heap_attisnull(indexTuple, Anum_pg_index_indpred) &&
			heap_attisnull(indexTuple, Anum_pg_index_indexprs))
		{
			if (attnum == indexStruct->indkey.values[0])
			{
				list_free(indexoidlist);
				if (index_id != NULL)
					*index_id = indexoid;

				return indexTuple;
			}

		}
		ReleaseSysCache(indexTuple);
	}
	list_free(indexoidlist);

	return NULL;
}

/*
 * On DROP partition we must ensure that no FK row references this partition
 */
void
ri_checkReferences(Relation partition, Oid constraintOid)
{
	const ConstraintInfo *riinfo;
	StringInfoData querybuf;
	AttrNumber	pkattnum;
	char		pkrelname[MAX_QUOTED_REL_NAME_LEN];
	char		fkrelname[MAX_QUOTED_REL_NAME_LEN];
	char		pkattname[MAX_QUOTED_NAME_LEN + 3];
	char		fkattname[MAX_QUOTED_NAME_LEN + 3];
	Oid			pkattype,
				fkattype;
	int			spi_result;
	SPIPlanPtr	qplan;

	Assert(OidIsValid(constraintOid));
	riinfo = ri_LoadConstraintInfo(constraintOid);

	/* Get partition attribute number */
	pkattnum = get_attnum(partition->rd_id,
						  get_attname(riinfo->pk_relid, riinfo->pk_attnum));

	/*----------
	 * The query string built is:
	 *	SELECT pk.keycol FROM ONLY partition pk
	 *	 INNER JOIN ONLY fkrelname fk
	 *	 ON pk.keycol = fk.keycol;
	 *----------
	 */
	quoteRelationName(pkrelname, partition->rd_id);
	quoteRelationName(fkrelname, riinfo->fk_relid);
	pkattype = get_atttype(partition->rd_id, pkattnum);
	fkattype = get_atttype(riinfo->fk_relid, riinfo->fk_attnum);

	strcpy(pkattname, "pk.");
	strcpy(fkattname, "fk.");
	quoteOneName(pkattname + 3,
				 get_attname(partition->rd_id, pkattnum));
	quoteOneName(fkattname + 3,
				 get_attname(riinfo->fk_relid, riinfo->fk_attnum));

	initStringInfo(&querybuf);
	appendStringInfo(&querybuf, "SELECT %s", pkattname);
	appendStringInfo(&querybuf,
					 " FROM ONLY %s pk INNER JOIN ONLY %s fk ON ",
					 pkrelname, fkrelname);
	ri_GenerateQual(&querybuf, "",
					pkattname, pkattype,
					riinfo->pf_eq_opr,
					fkattname, fkattype);

	if (SPI_connect() != SPI_OK_CONNECT)
		elog(ERROR, "SPI_connect failed");

	/*
	 * Generate the plan.  We don't need to cache it, and there are no
	 * arguments to the plan.
	 */
	qplan = SPI_prepare(querybuf.data, 0, NULL);

	if (qplan == NULL)
		elog(ERROR, "SPI_prepare returned %d for %s",
			 SPI_result, querybuf.data);

	/*
	 * Run the plan.  For safety we force a current snapshot to be used. (In
	 * transaction-snapshot mode, this arguably violates transaction isolation
	 * rules, but we really haven't got much choice.) We don't need to
	 * register the snapshot, because SPI_execute_snapshot will see to it. We
	 * need at most one tuple returned, so pass limit = 1.
	 */
	spi_result = SPI_execute_snapshot(qplan,
									  NULL, NULL,
									  GetLatestSnapshot(),
									  InvalidSnapshot,
									  true, false, 1);

	/* Check result */
	if (spi_result != SPI_OK_SELECT)
		elog(ERROR, "SPI_execute_snapshot returned %d", spi_result);

	/* Did we find a tuple violating the constraint? */
	if (SPI_processed > 0)
	{
		Datum		value;
		HeapTuple	tuple = SPI_tuptable->vals[0];
		TupleDesc	tupdesc = SPI_tuptable->tupdesc;
		bool		isnull;

		value = heap_getattr(tuple, 1, tupdesc, &isnull);
		Assert(!isnull);

		ri_ReportViolation(riinfo, partition->rd_id, riinfo->fk_relid,
						   value, pkattype, false);
	}

	if (SPI_finish() != SPI_OK_FINISH)
		elog(ERROR, "SPI_finish failed");
}

/*
 * Returns oids list of RI triggers for specified partition and FK constraint
 */
List *
get_ri_triggers_list(Oid relid, Oid constr)
{
	Relation	tgrel;
	SysScanDesc tgscan;
	ScanKeyData key;
	HeapTuple	tuple;
	bool		isnull;
	List	   *triggers = NIL;

	tgrel = heap_open(TriggerRelationId, RowExclusiveLock);

	ScanKeyInit(&key,
				Anum_pg_trigger_tgconstraint,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(constr));
	tgscan = systable_beginscan(tgrel, TriggerConstraintIndexId, true,
								NULL, 1, &key);
	while (HeapTupleIsValid(tuple = systable_getnext(tgscan)))
	{
		Form_pg_trigger trigform = (Form_pg_trigger) GETSTRUCT(tuple);

		if (trigform->tgrelid == relid)
		{
			Datum trigoid = heap_getsysattr(tuple, ObjectIdAttributeNumber,
											RelationGetDescr(tgrel),
											&isnull);
			if (!isnull)
				triggers = lappend_oid(triggers, DatumGetObjectId(trigoid));
		}
	}
	systable_endscan(tgscan);
	heap_close(tgrel, RowExclusiveLock);

	return triggers;
}

/*
 * Check for referencies from FK tables and remove dependencies that prevent
 * partition from DROP
 */
void
ri_preparePartitionDrop(Oid parent,
						Relation partition,
						bool check_references)
{
	List	   *ri_constr;
	List	   *ri_refrelids;
	ListCell   *lc1,
			   *lc2;
	const PartRelationInfo *prel;

	const char *attname;
	AttrNumber	attnum;
	HeapTuple	indexTuple;
	Oid			index;

	pathman_get_fkeys(parent, &ri_constr, &ri_refrelids);

	foreach(lc1, ri_constr)
	{
		Oid		constr = lfirst_oid(lc1);
		List   *triggers;

		prel = get_pathman_relation_info(parent);
		shout_if_prel_is_invalid(parent, prel, PT_ANY);

		/* Check if there are references in FK table */
		if (check_references)
			ri_checkReferences(partition, constr);

		/*
		 * Remove index dependency on FK constraint
		 */
		attnum = var_get_attnum(prel->expr); /* parent's attnum */
		Assert(attnum != InvalidAttrNumber);
		attname = get_attname(parent, attnum);
		attnum = get_attnum(partition->rd_id, attname);	/* partition's attnum */

		indexTuple = get_index_for_key(partition, attnum, &index);
		if (HeapTupleIsValid(indexTuple))
		{
			deleteDependencyRecords(ConstraintRelationId, constr,
									RelationRelationId, index);
			ReleaseSysCache(indexTuple);
		}
		else
			elog(WARNING, "Partition %s doesn't have unique index",
				 RelationGetRelationName(partition));

		/*
		 * RI triggers implicitly depend on FK constraint, and the former one
		 * wouldn't let you drop those triggers unless you drop the constraint
		 * itself. To overcome this hurdle we need to remove dependency of
		 * triggers on FK constraint
		 */
		triggers = get_ri_triggers_list(partition->rd_id, constr);
		foreach(lc2, triggers)
		{
			Oid trig = lfirst_oid(lc2);

			deleteDependencyRecords(TriggerRelationId, trig,
									ConstraintRelationId, constr);
		}
		pfree(triggers);
	}

	if (ri_constr)
	{
		pfree(ri_constr);
		pfree(ri_refrelids);
	}
}

void
enable_ri_triggers(void)
{
	/* Check if they have been disabled */
	Assert(ri_triggers_enabled == false);
	ri_triggers_enabled = true;
}

void
disable_ri_triggers(void)
{
	/* Check if they have been enabled */
	Assert(ri_triggers_enabled == true);
	ri_triggers_enabled = false;
}
