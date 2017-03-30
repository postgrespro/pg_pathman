#include "c.h"

#include "postgres.h"
#include "miscadmin.h"

#include "access/htup_details.h"
#include "access/xact.h"
#include "catalog/pg_am.h"
#include "catalog/pg_constraint.h"
#include "catalog/pg_constraint_fn.h"
#include "catalog/pg_type.h"
#include "catalog/pg_opclass.h"
#include "catalog/pg_operator.h"
#include "catalog/pg_trigger.h"
#include "commands/trigger.h"
#include "executor/spi.h"
#include "parser/parse_relation.h"
#include "storage/bufmgr.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/tqual.h"
#include "utils/syscache.h"

#include "partition_filter.h"
#include "relation_info.h"


#define MAX_QUOTED_NAME_LEN				(NAMEDATALEN*2+3)
#define MAX_QUOTED_REL_NAME_LEN			(MAX_QUOTED_NAME_LEN*2)

#define RI_INIT_CONSTRAINTHASHSIZE		64
#define RI_INIT_QUERYHASHSIZE 			(RI_INIT_CONSTRAINTHASHSIZE * 4)

#define RIAttName(rel, attnum)	NameStr(*attnumAttName(rel, attnum))

#define RI_TRIGTYPE_INSERT 1
#define RI_TRIGTYPE_UPDATE 2
#define RI_TRIGTYPE_DELETE 3


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
	Oid			pf_eq_opr;		/* equality operators (PK = FK) */
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
	/* XXX At this point there is only one query type */
	// int32		constr_queryno; /* query type ID, see RI_PLAN_XXX above */
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
 * Local functions
 * ----------
 */
static const ConstraintInfo *ri_LoadConstraintInfo(Oid constraintOid);
static void ri_GenerateQual(StringInfo buf,
				const char *sep,
				const char *leftop, Oid leftoptype,
				Oid opoid,
				const char *rightop, Oid rightoptype);
static void ri_add_cast_to(StringInfo buf, Oid typid);
static const ConstraintInfo *ri_FetchConstraintInfo(Trigger *trigger, Relation trig_rel, bool rel_is_pk);
static void ri_InitHashTables();
static bool ri_PerformCheck(const ConstraintInfo *riinfo,
				SPIPlanPtr qplan,
				Relation fk_rel, Relation pk_rel,
				bool source_is_pk,
				HeapTuple tuple,
				bool detectNewRows, int expect_OK);
static SPIPlanPtr ri_PlanCheck(const char *querystr,
			 Oid argtype,
			 QueryKey *qkey,
			 Relation pk_rel,
			 bool cache_plan);

static Datum RI_FKey_check(TriggerData *trigdata);
static Datum ri_restrict_del(TriggerData *trigdata);

static void quoteOneName(char *buffer, const char *name);
static void quoteRelationName(char *buffer, Relation rel);
static void ri_ReportViolation(const ConstraintInfo *riinfo, Relation fk_rel);
static SPIPlanPtr FetchPreparedPlan(QueryKey *key);
static void BuildQueryKey(QueryKey *key, Oid relid, Oid partid);
static void SavePreparedPlan(QueryKey *key, SPIPlanPtr plan);
static Oid transformFkeyCheckAttrs(Relation pkrel, int16 attnum, Oid *opclass);
static void create_fk_constraint_internal(Oid fk_table, AttrNumber fk_attnum, Oid pk_table, AttrNumber pk_attnum);
static void createForeignKeyTriggers(Relation rel, Oid refRelOid,
						 Oid constraintOid, Oid indexOid);
static void createSingleForeignKeyTrigger(Oid relOid, Oid refRelOid, List *funcname,
							  char *trigname, int16 events, Oid constraintOid,
							  Oid indexOid);


PG_FUNCTION_INFO_V1(pathman_fkey_check_ins);
PG_FUNCTION_INFO_V1(pathman_fkey_check_upd);
PG_FUNCTION_INFO_V1(pathman_fkey_restrict_del);
PG_FUNCTION_INFO_V1(create_fk_constraint);

/* ----------
 * Local data
 * ----------
 */
static HTAB *ri_constraint_cache = NULL;
static HTAB *ri_query_cache = NULL;



Datum
pathman_fkey_check_ins(PG_FUNCTION_ARGS)
{
	/*
	 * Check that this is a valid trigger call on the right time and event.
	 */
	// ri_CheckTrigger(fcinfo, "pathman_fkey_check_ins", RI_TRIGTYPE_INSERT);

	/*
	 * Share code with UPDATE case.
	 */
	return RI_FKey_check((TriggerData *) fcinfo->context);
}

Datum
pathman_fkey_check_upd(PG_FUNCTION_ARGS)
{
	/*
	 * Check that this is a valid trigger call on the right time and event.
	 */
	// ri_CheckTrigger(fcinfo, "RI_FKey_check_upd", RI_TRIGTYPE_UPDATE);

	/*
	 * Share code with INSERT case.
	 */
	return RI_FKey_check((TriggerData *) fcinfo->context);
}

Datum
pathman_fkey_restrict_del(PG_FUNCTION_ARGS)
{
	/*
	 * Check that this is a valid trigger call on the right time and event.
	 */
	// ri_CheckTrigger(fcinfo, "RI_FKey_restrict_del", RI_TRIGTYPE_DELETE);

	return ri_restrict_del((TriggerData *) fcinfo->context);
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

	/* There is only one option now, but it is about to get better :) */
	switch (tgkind)
	{
		case RI_TRIGTYPE_INSERT:
			if (!TRIGGER_FIRED_BY_INSERT(trigdata->tg_event))
				ereport(ERROR,
						(errcode(ERRCODE_E_R_I_E_TRIGGER_PROTOCOL_VIOLATED),
						 errmsg("function \"%s\" must be fired for INSERT", funcname)));
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
RI_FKey_check(TriggerData *trigdata)
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
	/* TODO: it should probably be another lock level */
	pk_rel = heap_open(riinfo->pk_relid, RowShareLock);

	/* Get partitioning info */
	prel = get_pathman_relation_info(pk_rel->rd_id);
	if (!prel)
		elog(ERROR,
			 "table %s isn't partitioned by pg_pathman",
			 get_rel_name(pk_rel->rd_id));


	if (SPI_connect() != SPI_OK_CONNECT)
		elog(ERROR, "SPI_connect failed");

	pk_type = attnumTypeId(pk_rel, riinfo->pk_attnum);
	fk_type = attnumTypeId(fk_rel, riinfo->fk_attnum);

	value = heap_getattr(new_row, riinfo->fk_attnum, fk_rel->rd_att, &isnull);

	/* If foreign key value is NULL then just quit */
	if (isnull)
		return PointerGetDatum(NULL);

	partid = find_partition_for_value(value, fk_type, prel);
	part_rel = heap_open(partid, RowShareLock);

	/* It seems that we got a partition! */
	if (partid == InvalidOid)
		/* TODO: Write a decent error message as in ri_triggers.c */
		elog(ERROR, "can't find suitable partition for foreign key");

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
		quoteRelationName(pkrelname, part_rel);
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

	heap_close(pk_rel, RowShareLock);
	heap_close(part_rel, RowShareLock);

	return PointerGetDatum(NULL);
}

static Datum
ri_restrict_del(TriggerData *trigdata)
{
	const ConstraintInfo *riinfo;
	Relation	fk_rel;
	Relation	pk_rel;
	HeapTuple	old_row;
	QueryKey	qkey;
	SPIPlanPtr	qplan;
	Oid			pk_type;
	Oid			fk_type;

	/*
	 * Get arguments.
	 */
	riinfo = ri_FetchConstraintInfo(trigdata->tg_trigger,
									trigdata->tg_relation, true);

	fk_rel = heap_open(riinfo->fk_relid, RowShareLock);
	pk_rel = trigdata->tg_relation;
	// new_row = trigdata->tg_newtuple;
	old_row = trigdata->tg_trigtuple;

	pk_type = attnumTypeId(pk_rel, riinfo->pk_attnum);
	fk_type = attnumTypeId(fk_rel, riinfo->fk_attnum);

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
		quoteRelationName(fkrelname, fk_rel);
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
	// int			numkeys;

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
	// riinfo->confupdtype = conForm->confupdtype;
	// riinfo->confdeltype = conForm->confdeltype;
	// riinfo->confmatchtype = conForm->confmatchtype;

	read_constr_attribute(fk_attnum, conkey, INT2OID);
	read_constr_attribute(pk_attnum, confkey, INT2OID);
	read_constr_attribute(pf_eq_opr, conpfeqop, OIDOID);

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
		// if (riinfo->fk_relid != trigger->tgconstrrelid ||
		// 	riinfo->pk_relid != RelationGetRelid(trig_rel))
		// 	elog(ERROR, "wrong pg_constraint entry for trigger \"%s\" on table \"%s\"",
		// 		 trigger->tgname, RelationGetRelationName(trig_rel));
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
ri_InitHashTables()
{
	HASHCTL		ctl;

	memset(&ctl, 0, sizeof(ctl));
	ctl.keysize = sizeof(Oid);
	ctl.entrysize = sizeof(ConstraintInfo);
	ri_constraint_cache = hash_create("pathman ri_constraint cache",
									  RI_INIT_CONSTRAINTHASHSIZE,
									  &ctl, HASH_ELEM | HASH_BLOBS);

	/* Arrange to flush cache on pg_constraint changes */
	// CacheRegisterSyscacheCallback(CONSTROID,
	// 							  InvalidateConstraintCacheCallBack,
	// 							  (Datum) 0);

	memset(&ctl, 0, sizeof(ctl));
	ctl.keysize = sizeof(QueryKey);
	ctl.entrysize = sizeof(QueryHashEntry);
	ri_query_cache = hash_create("pathman_ri query cache",
								 RI_INIT_QUERYHASHSIZE,
								 &ctl, HASH_ELEM | HASH_BLOBS);

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
// static SPIPlanPtr
// ri_PlanCheck(const char *querystr, Oid argtype,
// 			 RI_QueryKey *qkey, Relation fk_rel, Relation pk_rel,
// 			 bool cache_plan)
static SPIPlanPtr
ri_PlanCheck(const char *querystr,
			 Oid argtype,
			 QueryKey *qkey,
			 Relation query_rel,
			 bool cache_plan)
{
	SPIPlanPtr	qplan;
	// Relation	query_rel;
	Oid			save_userid;
	int			save_sec_context;
	Oid			argtypes[1] = {argtype};

	/*
	 * Use the query type code to determine whether the query is run against
	 * the PK or FK table; we'll do the check as that table's owner
	 */
	// if (qkey->constr_queryno <= RI_PLAN_LAST_ON_PK)
	// 	query_rel = pk_rel;
	// else
	// 	query_rel = fk_rel;
	// query_rel = pk_rel;

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
// static bool
// ri_PerformCheck(const RI_ConstraintInfo *riinfo,
// 				RI_QueryKey *qkey, SPIPlanPtr qplan,
// 				Relation fk_rel, Relation pk_rel,
// 				HeapTuple old_tuple, HeapTuple new_tuple,
// 				bool detectNewRows, int expect_OK)
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
	// Datum		vals[RI_MAX_NUMKEYS * 2];
	// char		nulls[RI_MAX_NUMKEYS * 2];
	Datum		vals[1];
	char		nulls[1];
	bool		isnull = false;
	AttrNumber	attnum;


	if (source_is_pk)
	{
		query_rel = pk_rel;
		source_rel = fk_rel;
		attnum = riinfo->pk_attnum;
	}
	else
	{
		query_rel = pk_rel;
		source_rel = fk_rel;
		attnum = riinfo->fk_attnum;
	}

	vals[0] = heap_getattr(tuple, attnum, source_rel->rd_att, &isnull);
	nulls[0] = isnull ? 'n' : ' ';

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
									  vals, nulls,
									  test_snapshot, crosscheck_snapshot,
									  false, false, limit);

	/* Restore UID and security context */
	SetUserIdAndSecContext(save_userid, save_sec_context);

	/* Check result */
	if (spi_result < 0)
		elog(ERROR, "SPI_execute_snapshot returned %d", spi_result);

	if (expect_OK >= 0 && spi_result != expect_OK)
		/* TODO */
		elog(ERROR, "Error 1");

	/* XXX wouldn't it be clearer to do this part at the caller? */
	if ((SPI_processed == 0) != source_is_pk)
		/* TODO: write a correct table name on DELETE from PK table */
		ri_ReportViolation(riinfo, fk_rel);

	return SPI_processed != 0;
}

/* TODO: Aren't there already such functions somewhere? */
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
quoteRelationName(char *buffer, Relation rel)
{
	quoteOneName(buffer, get_namespace_name(RelationGetNamespace(rel)));
	buffer += strlen(buffer);
	*buffer++ = '.';
	quoteOneName(buffer, RelationGetRelationName(rel));
}

static void
ri_ReportViolation(const ConstraintInfo *riinfo, Relation fk_rel)
{
	ereport(ERROR,
		(errcode(ERRCODE_FOREIGN_KEY_VIOLATION),
		 errmsg("insert or update on table \"%s\" violates foreign key constraint \"%s\"",
				RelationGetRelationName(fk_rel),
				NameStr(riinfo->conname))));
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
 * ----------------------------------------------------------
 */

Datum
create_fk_constraint(PG_FUNCTION_ARGS)
{
	Oid fk_table = PG_GETARG_OID(0);
	Oid pk_table = PG_GETARG_OID(2);
	char *fk_attr = TextDatumGetCString(PG_GETARG_TEXT_P(1));
	char *pk_attr = TextDatumGetCString(PG_GETARG_TEXT_P(3));
	AttrNumber fk_attnum = get_attnum(fk_table, fk_attr);
	AttrNumber pk_attnum = get_attnum(pk_table, pk_attr);

	create_fk_constraint_internal(fk_table, fk_attnum, pk_table, pk_attnum);

	PG_RETURN_VOID();
}

static void
create_fk_constraint_internal(Oid fk_table, AttrNumber fk_attnum, Oid pk_table, AttrNumber pk_attnum)
{
	Oid			indexOid;
	Oid			opclass;
	Oid			pktypoid = get_atttype(pk_table, pk_attnum);
	Oid			fktypoid = get_atttype(fk_table, fk_attnum);
	Oid			fktype;
	Oid			fktyped;
	char	   *fkname;
	Oid			constrOid;

	Relation fkrel;
	Relation pkrel;

	HeapTuple	cla_ht;
	Form_pg_opclass cla_tup;
	Oid			amid;
	Oid			pfeqop;
	Oid			ppeqop;
	Oid			ffeqop;

	Oid opfamily;
	Oid opcintype;


	fkrel = heap_open(fk_table, ShareRowExclusiveLock);
	pkrel = heap_open(pk_table, ShareRowExclusiveLock);

	fkname = ChooseConstraintName(RelationGetRelationName(fkrel),
								  get_attname(fk_table, fk_attnum),
								  "fkey",
								  RelationGetNamespace(fkrel),
								  NIL);

	indexOid = transformFkeyCheckAttrs(pkrel, pk_attnum, &opclass);

	/* TODO: check permissions */

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
	// ObjectAddressSet(address, ConstraintRelationId, constrOid);

	/* Make changes-so-far visible */
	CommandCounterIncrement();

	createForeignKeyTriggers(fkrel, pk_table, constrOid, indexOid);

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
	Oid			myRelOid;
	Oid			pathman_schema = get_pathman_schema();
	char	   *pathman_schema_name = get_namespace_name(pathman_schema);
	List	   *funcname;
	Oid		   *children;
	uint32		nchildren;
	int			i;

	myRelOid = RelationGetRelid(rel);

	prel = get_pathman_relation_info(refRelOid);
	if (!prel)
		elog(ERROR,
			 "table %s isn't partitioned by pg_pathman",
			 get_rel_name(refRelOid));

	/*
	 * Build and execute a CREATE CONSTRAINT TRIGGER statement for the ON
	 * INSERT action on the FK table.
	 */
	createSingleForeignKeyTrigger(myRelOid, refRelOid,
								  pathman_funcname("pathman_fkey_check_ins"),
								  "RI_ConstraintTrigger_c",
								  TRIGGER_TYPE_INSERT,
								  constraintOid,
								  indexOid);

	/*
	 * Build and execute a CREATE CONSTRAINT TRIGGER statement for the ON
	 * INSERT action on the FK table.
	 */
	createSingleForeignKeyTrigger(myRelOid, refRelOid,
								  pathman_funcname("pathman_fkey_check_upd"),
								  "RI_ConstraintTrigger_c",
								  TRIGGER_TYPE_UPDATE,
								  constraintOid,
								  indexOid);

	nchildren = PrelChildrenCount(prel);
	children = PrelGetChildrenArray(prel);

	/*
	 * Create ON DELETE triggers on each partition
	 */
	funcname = pathman_funcname("pathman_fkey_restrict_del");
	for (i = 0; i < nchildren; i++)
	{
		createSingleForeignKeyTrigger(children[i], myRelOid, funcname,
									  "RI_ConstraintTrigger_a",
									  TRIGGER_TYPE_DELETE,
									  constraintOid,
									  indexOid);
	}
}

static void
createSingleForeignKeyTrigger(Oid relOid, Oid refRelOid, List *funcname,
							  char *trigname, int16 events, Oid constraintOid,
							  Oid indexOid)
{
	CreateTrigStmt *fk_trigger;

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

	(void) CreateTrigger(fk_trigger, NULL, relOid, refRelOid, constraintOid,
						 indexOid, true);

	/* Make changes-so-far visible */
	CommandCounterIncrement();
}

static Oid
transformFkeyCheckAttrs(Relation pkrel, int16 attnum,
						Oid *opclass) /* output parameter */
{
	Oid			indexoid = InvalidOid;
	bool		found = false;
	List	   *indexoidlist;
	ListCell   *indexoidscan;

	/*
	 * Get the list of index OIDs for the table from the relcache, and look up
	 * each one in the pg_index syscache, and match unique indexes to the list
	 * of attnums we are given.
	 */
	indexoidlist = RelationGetIndexList(pkrel);

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
		 */
		if (indexStruct->indnkeyatts == 1 &&
			indexStruct->indisunique &&
			IndexIsValid(indexStruct) &&
			heap_attisnull(indexTuple, Anum_pg_index_indpred) &&
			heap_attisnull(indexTuple, Anum_pg_index_indexprs))
		{
			Datum		indclassDatum;
			bool		isnull;
			oidvector  *indclass;

			/* Must get indclass the hard way */
			indclassDatum = SysCacheGetAttr(INDEXRELID, indexTuple,
											Anum_pg_index_indclass, &isnull);
			Assert(!isnull);
			indclass = (oidvector *) DatumGetPointer(indclassDatum);

			*opclass = InvalidOid;
			if (attnum == indexStruct->indkey.values[0])
			{
				*opclass = indclass->values[0];
				break;
			}

		}
		ReleaseSysCache(indexTuple);
		if (found)
			break;
	}

	if (!OidIsValid(opclass))
	{
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_FOREIGN_KEY),
				 errmsg("there is no unique constraint matching given keys for referenced table \"%s\"",
						RelationGetRelationName(pkrel))));
	}

	list_free(indexoidlist);

	return indexoid;
}
