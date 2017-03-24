#include "c.h"

#include "postgres.h"
#include "miscadmin.h"

#include "access/htup_details.h"
#include "access/xact.h"
#include "catalog/pg_constraint.h"
#include "catalog/pg_type.h"
#include "catalog/pg_operator.h"
#include "commands/trigger.h"
#include "executor/spi.h"
#include "parser/parse_relation.h"
#include "storage/bufmgr.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/tqual.h"
#include "utils/syscache.h"

#include "relation_info.h"
#include "partition_filter.h"


#define MAX_QUOTED_NAME_LEN				(NAMEDATALEN*2+3)
#define MAX_QUOTED_REL_NAME_LEN			(MAX_QUOTED_NAME_LEN*2)

#define RI_INIT_CONSTRAINTHASHSIZE		64
#define RI_INIT_QUERYHASHSIZE 			(RI_INIT_CONSTRAINTHASHSIZE * 4)

#define RIAttName(rel, attnum)	NameStr(*attnumAttName(rel, attnum))

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
				HeapTuple new_tuple,
				bool detectNewRows, int expect_OK);
static SPIPlanPtr ri_PlanCheck(const char *querystr,
			 Oid argtype,
			 QueryKey *qkey,
			 Relation pk_rel,
			 bool cache_plan);
static Datum RI_FKey_check(TriggerData *trigdata);

static void quoteOneName(char *buffer, const char *name);
static void quoteRelationName(char *buffer, Relation rel);
static SPIPlanPtr FetchPreparedPlan(QueryKey *key);
static void BuildQueryKey(QueryKey *key, Oid relid, Oid partid);
static void SavePreparedPlan(QueryKey *key, SPIPlanPtr plan);


PG_FUNCTION_INFO_V1(pathman_fkey_check_ins);

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
	// ri_CheckTrigger(fcinfo, "RI_FKey_check_ins", RI_TRIGTYPE_INSERT);

	/*
	 * Share code with UPDATE case.
	 */
	return RI_FKey_check((TriggerData *) fcinfo->context);
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
	int			i;
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

	// if (riinfo->confmatchtype == FKCONSTR_MATCH_PARTIAL)
	// 	ereport(ERROR,
	// 			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
	// 			 errmsg("MATCH PARTIAL not yet implemented")));

	/* TODO: Null check */

	prel = get_pathman_relation_info(pk_rel->rd_id);

	/* If there's no prel, return TRUE (overlap is not possible) */
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
	// ri_BuildQueryKey(&qkey, riinfo, RI_PLAN_CHECK_LOOKUPPK);
	BuildQueryKey(&qkey, riinfo->pk_relid, partid);

	if ((qplan = FetchPreparedPlan(&qkey)) == NULL)
	{
		StringInfoData querybuf;
		char		pkrelname[MAX_QUOTED_REL_NAME_LEN];
		char		attname[MAX_QUOTED_NAME_LEN];
		char		paramname[16];

		/* TODO */
		// Oid			queryoids[123];

		/* ----------
		 * The query string built is
		 *	SELECT 1 FROM ONLY <pktable> x WHERE pkatt1 = $1 [AND ...]
		 *		   FOR KEY SHARE OF x
		 * The type id's for the $ parameters are those of the
		 * corresponding FK attributes.
		 * ----------
		 */
		initStringInfo(&querybuf);
		quoteRelationName(pkrelname, part_rel);
		appendStringInfo(&querybuf, "SELECT 1 FROM ONLY %s x", pkrelname);

		quoteOneName(attname, RIAttName(pk_rel, riinfo->pk_attnum));
		// sprintf(paramname, "$%d", i + 1);
		sprintf(paramname, "$1");
		ri_GenerateQual(&querybuf, "WHERE",
						attname, pk_type,
						riinfo->pf_eq_opr,
						paramname, fk_type);
		// queryoids[i] = fk_type;


		appendStringInfoString(&querybuf, " FOR KEY SHARE OF x");

		/* Prepare and save the plan */
		qplan = ri_PlanCheck(querybuf.data, fk_type, &qkey, part_rel, true);
	}

	/*
	 * Now check that foreign key exists in PK table
	 */
	ri_PerformCheck(riinfo, qplan,
					fk_rel, part_rel,
					new_row,
					false,
					SPI_OK_SELECT);

	if (SPI_finish() != SPI_OK_FINISH)
		elog(ERROR, "SPI_finish failed");

	heap_close(pk_rel, RowShareLock);
	heap_close(part_rel, RowShareLock);

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
		if (riinfo->fk_relid != trigger->tgconstrrelid ||
			riinfo->pk_relid != RelationGetRelid(trig_rel))
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
			 Relation pk_rel,
			 bool cache_plan)
{
	SPIPlanPtr	qplan;
	Relation	query_rel;
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
	query_rel = pk_rel;

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
				HeapTuple new_tuple,
				bool detectNewRows, int expect_OK)
{
	Relation	query_rel,
				source_rel;
	bool		source_is_pk;
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

	/*
	 * Use the query type code to determine whether the query is run against
	 * the PK or FK table; we'll do the check as that table's owner
	 */
	// if (qkey->constr_queryno <= RI_PLAN_LAST_ON_PK)
	// 	query_rel = pk_rel;
	// else
	// 	query_rel = fk_rel;
	query_rel = pk_rel;

	/*
	 * The values for the query are taken from the table on which the trigger
	 * is called - it is normally the other one with respect to query_rel. An
	 * exception is ri_Check_Pk_Match(), which uses the PK table for both (and
	 * sets queryno to RI_PLAN_CHECK_LOOKUPPK_FROM_PK).  We might eventually
	 * need some less klugy way to determine this.
	 */
	// if (qkey->constr_queryno == RI_PLAN_CHECK_LOOKUPPK)
	// {
		source_rel = fk_rel;
		source_is_pk = false;
	// }
	// else
	// {
	// 	source_rel = pk_rel;
	// 	source_is_pk = true;
	// }

	/* Extract the parameters to be passed into the query */
	// if (new_tuple)
	// {
	// 	ri_ExtractValues(source_rel, new_tuple, riinfo, source_is_pk,
	// 					 vals, nulls);
	// 	if (old_tuple)
	// 		ri_ExtractValues(source_rel, old_tuple, riinfo, source_is_pk,
	// 						 vals + riinfo->nkeys, nulls + riinfo->nkeys);
	// }
	// else
	// {
	// 	ri_ExtractValues(source_rel, old_tuple, riinfo, source_is_pk,
	// 					 vals, nulls);
	// }

	vals[0] = heap_getattr(new_tuple, riinfo->fk_attnum, source_rel->rd_att, &isnull);
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
		// ri_ReportViolation(riinfo,
		// 				   pk_rel, fk_rel,
		// 				   new_tuple ? new_tuple : old_tuple,
		// 				   NULL,
		// 				   qkey->constr_queryno, true);
		elog(ERROR, "Error 1");

	/* XXX wouldn't it be clearer to do this part at the caller? */
	if (SPI_processed == 0)
		elog(ERROR, "Error 2");
	// if (qkey->constr_queryno != RI_PLAN_CHECK_LOOKUPPK_FROM_PK &&
	// 	expect_OK == SPI_OK_SELECT &&
	// (SPI_processed == 0) == (qkey->constr_queryno == RI_PLAN_CHECK_LOOKUPPK))
	// 	elog(ERROR, "Error 2");
		// ri_ReportViolation(riinfo,
		// 				   pk_rel, fk_rel,
		// 				   new_tuple ? new_tuple : old_tuple,
		// 				   NULL,
		// 				   qkey->constr_queryno, false);

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
