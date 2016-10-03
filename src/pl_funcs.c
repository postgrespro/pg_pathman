/* ------------------------------------------------------------------------
 *
 * pl_funcs.c
 *		Utility C functions for stored procedures
 *
 * Copyright (c) 2015-2016, Postgres Professional
 *
 * ------------------------------------------------------------------------
 */

#include "init.h"
#include "utils.h"
#include "pathman.h"
#include "relation_info.h"
#include "xact_handling.h"

#include "access/htup_details.h"
#include "access/nbtree.h"
#include "catalog/indexing.h"
#include "catalog/pg_type.h"
#include "commands/tablespace.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "utils/builtins.h"
#include "utils/inval.h"
#include "utils/jsonb.h"
#include "utils/snapmgr.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"


/* Function declarations */

PG_FUNCTION_INFO_V1( on_partitions_created );
PG_FUNCTION_INFO_V1( on_partitions_updated );
PG_FUNCTION_INFO_V1( on_partitions_removed );

PG_FUNCTION_INFO_V1( get_parent_of_partition_pl );
PG_FUNCTION_INFO_V1( get_base_type_pl );
PG_FUNCTION_INFO_V1( get_attribute_type_pl );
PG_FUNCTION_INFO_V1( get_rel_tablespace_name );

PG_FUNCTION_INFO_V1( show_partition_list_internal );

PG_FUNCTION_INFO_V1( build_update_trigger_func_name );
PG_FUNCTION_INFO_V1( build_update_trigger_name );
PG_FUNCTION_INFO_V1( build_check_constraint_name_attnum );
PG_FUNCTION_INFO_V1( build_check_constraint_name_attname );

PG_FUNCTION_INFO_V1( is_date_type );
PG_FUNCTION_INFO_V1( is_attribute_nullable );

PG_FUNCTION_INFO_V1( add_to_pathman_config );
PG_FUNCTION_INFO_V1( invalidate_relcache );

PG_FUNCTION_INFO_V1( lock_partitioned_relation );
PG_FUNCTION_INFO_V1( prevent_relation_modification );

PG_FUNCTION_INFO_V1( validate_on_part_init_callback_pl );
PG_FUNCTION_INFO_V1( invoke_on_partition_created_callback );

PG_FUNCTION_INFO_V1( debug_capture );


typedef struct
{
	Relation				pathman_config;
	HeapScanDesc			pathman_config_scan;
	Snapshot				snapshot;

	const PartRelationInfo *current_prel;

	uint32					child_number;
} show_partition_list_cxt;


static void on_partitions_created_internal(Oid partitioned_table, bool add_callbacks);
static void on_partitions_updated_internal(Oid partitioned_table, bool add_callbacks);
static void on_partitions_removed_internal(Oid partitioned_table, bool add_callbacks);


/*
 * Extracted common check.
 */
static bool
check_relation_exists(Oid relid)
{
	return get_rel_type_id(relid) != InvalidOid;
}


/*
 * ----------------------------
 *  Partition events callbacks
 * ----------------------------
 */

static void
on_partitions_created_internal(Oid partitioned_table, bool add_callbacks)
{
	elog(DEBUG2, "on_partitions_created() [add_callbacks = %s] "
				 "triggered for relation %u",
		 (add_callbacks ? "true" : "false"), partitioned_table);
}

static void
on_partitions_updated_internal(Oid partitioned_table, bool add_callbacks)
{
	bool found;

	elog(DEBUG2, "on_partitions_updated() [add_callbacks = %s] "
				 "triggered for relation %u",
		 (add_callbacks ? "true" : "false"), partitioned_table);

	invalidate_pathman_relation_info(partitioned_table, &found);
}

static void
on_partitions_removed_internal(Oid partitioned_table, bool add_callbacks)
{
	elog(DEBUG2, "on_partitions_removed() [add_callbacks = %s] "
				 "triggered for relation %u",
		 (add_callbacks ? "true" : "false"), partitioned_table);
}


Datum
on_partitions_created(PG_FUNCTION_ARGS)
{
	on_partitions_created_internal(PG_GETARG_OID(0), true);
	PG_RETURN_NULL();
}

Datum
on_partitions_updated(PG_FUNCTION_ARGS)
{
	on_partitions_updated_internal(PG_GETARG_OID(0), true);
	PG_RETURN_NULL();
}

Datum
on_partitions_removed(PG_FUNCTION_ARGS)
{
	on_partitions_removed_internal(PG_GETARG_OID(0), true);
	PG_RETURN_NULL();
}


/*
 * ------------------------
 *  Various useful getters
 * ------------------------
 */

/*
 * Get parent of a specified partition.
 */
Datum
get_parent_of_partition_pl(PG_FUNCTION_ARGS)
{
	Oid					partition = PG_GETARG_OID(0);
	PartParentSearch	parent_search;
	Oid					parent;

	/* Fetch parent & write down search status */
	parent = get_parent_of_partition(partition, &parent_search);

	/* We MUST be sure :) */
	Assert(parent_search != PPS_NOT_SURE);

	/* It must be parent known by pg_pathman */
	if (parent_search == PPS_ENTRY_PART_PARENT)
		PG_RETURN_OID(parent);
	else
	{
		elog(ERROR, "\%s\" is not pg_pathman's partition",
			 get_rel_name_or_relid(partition));

		PG_RETURN_NULL();
	}
}

/*
 * Extract basic type of a domain.
 */
Datum
get_base_type_pl(PG_FUNCTION_ARGS)
{
	PG_RETURN_OID(getBaseType(PG_GETARG_OID(0)));
}

/*
 * Get type (as REGTYPE) of a given attribute.
 */
Datum
get_attribute_type_pl(PG_FUNCTION_ARGS)
{
	Oid			relid = PG_GETARG_OID(0);
	text	   *attname = PG_GETARG_TEXT_P(1);
	Oid			result;
	HeapTuple	tp;

	/* NOTE: for now it's the most efficient way */
	tp = SearchSysCacheAttName(relid, text_to_cstring(attname));
	if (HeapTupleIsValid(tp))
	{
		Form_pg_attribute att_tup = (Form_pg_attribute) GETSTRUCT(tp);
		result = att_tup->atttypid;
		ReleaseSysCache(tp);

		PG_RETURN_OID(result);
	}
	else
		elog(ERROR, "Cannot find type name for attribute \"%s\" "
					"of relation \"%s\"",
			 text_to_cstring(attname), get_rel_name_or_relid(relid));

	PG_RETURN_NULL(); /* keep compiler happy */
}

/*
 * Return tablespace name for specified relation
 */
Datum
get_rel_tablespace_name(PG_FUNCTION_ARGS)
{
	Oid			relid = PG_GETARG_OID(0);
	Oid			tablespace_id;
	char	   *result;

	tablespace_id = get_rel_tablespace(relid);

	/* If tablespace id is InvalidOid then use the default tablespace */
	if (!OidIsValid(tablespace_id))
	{
		tablespace_id = GetDefaultTablespace(get_rel_persistence(relid));

		/* If tablespace is still invalid then use database's default */
		if (!OidIsValid(tablespace_id))
			tablespace_id = MyDatabaseTableSpace;
	}

	result = get_tablespace_name(tablespace_id);
	PG_RETURN_TEXT_P(cstring_to_text(result));
}


/*
 * ----------------------
 *  Common purpose VIEWs
 * ----------------------
 */

/*
 * List all existing partitions and their parents.
 */
Datum
show_partition_list_internal(PG_FUNCTION_ARGS)
{
	show_partition_list_cxt	   *usercxt;
	FuncCallContext			   *funccxt;

	/*
	 * Initialize tuple descriptor & function call context.
	 */
	if (SRF_IS_FIRSTCALL())
	{
		TupleDesc		tupdesc;
		MemoryContext	old_mcxt;

		funccxt = SRF_FIRSTCALL_INIT();

		old_mcxt = MemoryContextSwitchTo(funccxt->multi_call_memory_ctx);

		usercxt = (show_partition_list_cxt *) palloc(sizeof(show_partition_list_cxt));

		/* Open PATHMAN_CONFIG with latest snapshot available */
		usercxt->pathman_config = heap_open(get_pathman_config_relid(),
											AccessShareLock);
		usercxt->snapshot = RegisterSnapshot(GetLatestSnapshot());
		usercxt->pathman_config_scan = heap_beginscan(usercxt->pathman_config,
													  usercxt->snapshot, 0, NULL);

		usercxt->current_prel = NULL;

		/* Create tuple descriptor */
		tupdesc = CreateTemplateTupleDesc(Natts_pathman_partition_list, false);

		TupleDescInitEntry(tupdesc, Anum_pathman_pl_parent,
						   "parent", REGCLASSOID, -1, 0);
		TupleDescInitEntry(tupdesc, Anum_pathman_pl_partition,
						   "partition", REGCLASSOID, -1, 0);
		TupleDescInitEntry(tupdesc, Anum_pathman_pl_parttype,
						   "parttype", INT4OID, -1, 0);
		TupleDescInitEntry(tupdesc, Anum_pathman_pl_partattr,
						   "partattr", TEXTOID, -1, 0);
		TupleDescInitEntry(tupdesc, Anum_pathman_pl_range_min,
						   "range_min", TEXTOID, -1, 0);
		TupleDescInitEntry(tupdesc, Anum_pathman_pl_range_max,
						   "range_max", TEXTOID, -1, 0);

		funccxt->tuple_desc = BlessTupleDesc(tupdesc);
		funccxt->user_fctx = (void *) usercxt;

		MemoryContextSwitchTo(old_mcxt);
	}

	funccxt = SRF_PERCALL_SETUP();
	usercxt = (show_partition_list_cxt *) funccxt->user_fctx;

	/* Iterate through pathman cache */
	for(;;)
	{
		const PartRelationInfo *prel;
		HeapTuple				htup;
		Datum					values[Natts_pathman_partition_list];
		bool					isnull[Natts_pathman_partition_list] = { 0 };
		char				   *partattr_cstr;

		/* Fetch next PartRelationInfo if needed */
		if (usercxt->current_prel == NULL)
		{
			HeapTuple	pathman_config_htup;
			Datum		parent_table;
			bool		parent_table_isnull;
			Oid			parent_table_oid;

			pathman_config_htup = heap_getnext(usercxt->pathman_config_scan,
											   ForwardScanDirection);
			if (!HeapTupleIsValid(pathman_config_htup))
				break;

			parent_table = heap_getattr(pathman_config_htup,
										Anum_pathman_config_partrel,
										RelationGetDescr(usercxt->pathman_config),
										&parent_table_isnull);

			Assert(parent_table_isnull == false);
			parent_table_oid = DatumGetObjectId(parent_table);

			usercxt->current_prel = get_pathman_relation_info(parent_table_oid);
			if (usercxt->current_prel == NULL)
				continue;

			usercxt->child_number = 0;
		}

		/* Alias to 'usercxt->current_prel' */
		prel = usercxt->current_prel;

		if (usercxt->child_number >= PrelChildrenCount(prel))
		{
			usercxt->current_prel = NULL;
			usercxt->child_number = 0;

			continue;
		}

		partattr_cstr = get_attname(PrelParentRelid(prel), prel->attnum);
		if (!partattr_cstr)
		{
			usercxt->current_prel = NULL;
			continue;
		}

		values[Anum_pathman_pl_parent - 1]		= PrelParentRelid(prel);
		values[Anum_pathman_pl_parttype - 1]	= prel->parttype;
		values[Anum_pathman_pl_partattr - 1]	= CStringGetTextDatum(partattr_cstr);

		switch (prel->parttype)
		{
			case PT_HASH:
				{
					Oid	 *children = PrelGetChildrenArray(prel),
						  child_oid = children[usercxt->child_number];

					values[Anum_pathman_pl_partition - 1] = child_oid;
					isnull[Anum_pathman_pl_range_min - 1] = true;
					isnull[Anum_pathman_pl_range_max - 1] = true;
				}
				break;

			case PT_RANGE:
				{
					RangeEntry *re;
					Datum		rmin,
								rmax;

					re = &PrelGetRangesArray(prel)[usercxt->child_number];

					rmin = CStringGetTextDatum(datum_to_cstring(re->min,
																prel->atttype));
					rmax = CStringGetTextDatum(datum_to_cstring(re->max,
																prel->atttype));

					values[Anum_pathman_pl_partition - 1] = re->child_oid;
					values[Anum_pathman_pl_range_min - 1] = rmin;
					values[Anum_pathman_pl_range_max - 1] = rmax;
				}
				break;

			default:
				elog(ERROR, "Unknown partitioning type %u", prel->parttype);
		}

		/* Switch to the next child */
		usercxt->child_number++;

		/* Form output tuple */
		htup = heap_form_tuple(funccxt->tuple_desc, values, isnull);

		SRF_RETURN_NEXT(funccxt, HeapTupleGetDatum(htup));
	}

	/* Clean resources */
	heap_endscan(usercxt->pathman_config_scan);
	UnregisterSnapshot(usercxt->snapshot);
	heap_close(usercxt->pathman_config, AccessShareLock);

	SRF_RETURN_DONE(funccxt);
}


/*
 * --------
 *  Traits
 * --------
 */

Datum
is_date_type(PG_FUNCTION_ARGS)
{
	PG_RETURN_BOOL(is_date_type_internal(PG_GETARG_OID(0)));
}

Datum
is_attribute_nullable(PG_FUNCTION_ARGS)
{
	Oid			relid = PG_GETARG_OID(0);
	text	   *attname = PG_GETARG_TEXT_P(1);
	bool		result = true;
	HeapTuple	tp;

	tp = SearchSysCacheAttName(relid, text_to_cstring(attname));
	if (HeapTupleIsValid(tp))
	{
		Form_pg_attribute att_tup = (Form_pg_attribute) GETSTRUCT(tp);
		result = !att_tup->attnotnull;
		ReleaseSysCache(tp);
	}
	else
		elog(ERROR, "Cannot find type name for attribute \"%s\" "
					"of relation \"%s\"",
			 text_to_cstring(attname), get_rel_name_or_relid(relid));

	PG_RETURN_BOOL(result); /* keep compiler happy */
}


/*
 * ------------------------
 *  Useful string builders
 * ------------------------
 */

Datum
build_update_trigger_func_name(PG_FUNCTION_ARGS)
{
	Oid			relid = PG_GETARG_OID(0),
				nspid;
	const char *result;

	/* Check that relation exists */
	if (!check_relation_exists(relid))
		elog(ERROR, "Invalid relation %u", relid);

	nspid = get_rel_namespace(relid);
	result = psprintf("%s.%s",
					  quote_identifier(get_namespace_name(nspid)),
					  quote_identifier(psprintf("%s_upd_trig_func",
												get_rel_name(relid))));

	PG_RETURN_TEXT_P(cstring_to_text(result));
}

Datum
build_update_trigger_name(PG_FUNCTION_ARGS)
{
	Oid			relid = PG_GETARG_OID(0);
	const char *result; /* trigger's name can't be qualified */

	/* Check that relation exists */
	if (!check_relation_exists(relid))
		elog(ERROR, "Invalid relation %u", relid);

	result = quote_identifier(psprintf("%s_upd_trig", get_rel_name(relid)));

	PG_RETURN_TEXT_P(cstring_to_text(result));
}

Datum
build_check_constraint_name_attnum(PG_FUNCTION_ARGS)
{
	Oid			relid = PG_GETARG_OID(0);
	AttrNumber	attnum = PG_GETARG_INT16(1);
	const char *result;

	if (!check_relation_exists(relid))
		elog(ERROR, "Invalid relation %u", relid);

	/* We explicitly do not support system attributes */
	if (attnum == InvalidAttrNumber || attnum < 0)
		elog(ERROR, "Cannot build check constraint name: "
					"invalid attribute number %i", attnum);

	result = build_check_constraint_name_internal(relid, attnum);

	PG_RETURN_TEXT_P(cstring_to_text(quote_identifier(result)));
}

Datum
build_check_constraint_name_attname(PG_FUNCTION_ARGS)
{
	Oid			relid = PG_GETARG_OID(0);
	text	   *attname = PG_GETARG_TEXT_P(1);
	AttrNumber	attnum = get_attnum(relid, text_to_cstring(attname));
	const char *result;

	if (!check_relation_exists(relid))
		elog(ERROR, "Invalid relation %u", relid);

	if (attnum == InvalidAttrNumber)
		elog(ERROR, "Relation \"%s\" has no column '%s'",
			 get_rel_name_or_relid(relid), text_to_cstring(attname));

	result = build_check_constraint_name_internal(relid, attnum);

	PG_RETURN_TEXT_P(cstring_to_text(quote_identifier(result)));
}


/*
 * ------------------------
 *  Cache & config updates
 * ------------------------
 */

/*
 * Try to add previously partitioned table to PATHMAN_CONFIG.
 */
Datum
add_to_pathman_config(PG_FUNCTION_ARGS)
{
	Oid					relid;
	text			   *attname;
	PartType			parttype;

	Relation			pathman_config;
	Datum				values[Natts_pathman_config];
	bool				isnull[Natts_pathman_config];
	HeapTuple			htup;
	CatalogIndexState	indstate;

	PathmanInitState	init_state;
	MemoryContext		old_mcxt = CurrentMemoryContext;

	if (PG_ARGISNULL(0))
		elog(ERROR, "parent_relid should not be null");

	if (PG_ARGISNULL(1))
		elog(ERROR, "attname should not be null");

	/* Read parameters */
	relid = PG_GETARG_OID(0);
	attname = PG_GETARG_TEXT_P(1);

	/* Check that relation exists */
	if (!check_relation_exists(relid))
		elog(ERROR, "Invalid relation %u", relid);

	if (get_attnum(relid, text_to_cstring(attname)) == InvalidAttrNumber)
		elog(ERROR, "Relation \"%s\" has no column '%s'",
			 get_rel_name_or_relid(relid), text_to_cstring(attname));

	/* Select partitioning type using 'range_interval' */
	parttype = PG_ARGISNULL(2) ? PT_HASH : PT_RANGE;

	/*
	 * Initialize columns (partrel, attname, parttype, range_interval).
	 */
	values[Anum_pathman_config_partrel - 1]			= ObjectIdGetDatum(relid);
	isnull[Anum_pathman_config_partrel - 1]			= false;

	values[Anum_pathman_config_attname - 1]			= PointerGetDatum(attname);
	isnull[Anum_pathman_config_attname - 1]			= false;

	values[Anum_pathman_config_parttype - 1]		= Int32GetDatum(parttype);
	isnull[Anum_pathman_config_parttype - 1]		= false;

	values[Anum_pathman_config_range_interval - 1]	= PG_GETARG_DATUM(2);
	isnull[Anum_pathman_config_range_interval - 1]	= PG_ARGISNULL(2);

	/* Insert new row into PATHMAN_CONFIG */
	pathman_config = heap_open(get_pathman_config_relid(), RowExclusiveLock);
	htup = heap_form_tuple(RelationGetDescr(pathman_config), values, isnull);
	simple_heap_insert(pathman_config, htup);
	indstate = CatalogOpenIndexes(pathman_config);
	CatalogIndexInsert(indstate, htup);
	CatalogCloseIndexes(indstate);
	heap_close(pathman_config, RowExclusiveLock);

	/* Now try to create a PartRelationInfo */
	PG_TRY();
	{
		/* Some flags might change during refresh attempt */
		save_pathman_init_state(&init_state);

		refresh_pathman_relation_info(relid, parttype, text_to_cstring(attname));
	}
	PG_CATCH();
	{
		ErrorData *edata;

		/* Switch to the original context & copy edata */
		MemoryContextSwitchTo(old_mcxt);
		edata = CopyErrorData();
		FlushErrorState();

		/* We have to restore all changed flags */
		restore_pathman_init_state(&init_state);

		/* Show error message */
		elog(ERROR, "%s", edata->message);

		FreeErrorData(edata);
	}
	PG_END_TRY();

	PG_RETURN_BOOL(true);
}


/*
 * Invalidate relcache for a specified relation.
 */
Datum
invalidate_relcache(PG_FUNCTION_ARGS)
{
	Oid		relid = PG_GETARG_OID(0);

	if (check_relation_exists(relid))
		CacheInvalidateRelcacheByRelid(relid);

	PG_RETURN_VOID();
}


/*
 * --------------------------
 *  Special locking routines
 * --------------------------
 */

/*
 * Acquire appropriate lock on a partitioned relation.
 */
Datum
lock_partitioned_relation(PG_FUNCTION_ARGS)
{
	Oid			relid = PG_GETARG_OID(0);

	/* Lock partitioned relation till transaction's end */
	xact_lock_partitioned_rel(relid, false);

	PG_RETURN_VOID();
}

/*
 * Lock relation exclusively & check for current isolation level.
 */
Datum
prevent_relation_modification(PG_FUNCTION_ARGS)
{
	Oid			relid = PG_GETARG_OID(0);

	/*
	 * Check that isolation level is READ COMMITTED.
	 * Else we won't be able to see new rows
	 * which could slip through locks.
	 */
	if (!xact_is_level_read_committed())
		ereport(ERROR,
				(errmsg("Cannot perform blocking partitioning operation"),
				 errdetail("Expected READ COMMITTED isolation level")));

	/*
	 * Check if table is being modified
	 * concurrently in a separate transaction.
	 */
	if (!xact_lock_rel_exclusive(relid, true))
		ereport(ERROR,
				(errmsg("Cannot perform blocking partitioning operation"),
				 errdetail("Table \"%s\" is being modified concurrently",
						   get_rel_name_or_relid(relid))));

	PG_RETURN_VOID();
}


/*
 * -------------------------------------------
 *  User-defined partition creation callbacks
 * -------------------------------------------
 */

/*
 * Checks that callback function meets specific requirements.
 * It must have the only JSONB argument and BOOL return type.
 */
Datum
validate_on_part_init_callback_pl(PG_FUNCTION_ARGS)
{
	validate_on_part_init_cb(PG_GETARG_OID(0), true);

	PG_RETURN_VOID();
}

/*
 * Builds JSONB object containing new partition parameters
 * and invokes the callback.
 */
Datum
invoke_on_partition_created_callback(PG_FUNCTION_ARGS)
{
#define JSB_INIT_VAL(value, val_type, val_cstring) \
	do { \
		(value)->type = jbvString; \
		(value)->val.string.len = strlen(val_cstring); \
		(value)->val.string.val = val_cstring; \
		pushJsonbValue(&jsonb_state, val_type, (value)); \
	} while (0)

#define ARG_PARENT			0	/* parent table */
#define ARG_CHILD			1	/* partition */
#define ARG_CALLBACK		2	/* callback to be invoked */
#define ARG_RANGE_START		3	/* start_value */
#define ARG_RANGE_END		4	/* end_value */

	Oid						parent_oid		= PG_GETARG_OID(ARG_PARENT),
							partition_oid	= PG_GETARG_OID(ARG_CHILD);
	PartType				part_type;

	Oid						cb_oid			= PG_GETARG_OID(ARG_CALLBACK);
	FmgrInfo				cb_flinfo;
	FunctionCallInfoData	cb_fcinfo;

	JsonbParseState		   *jsonb_state = NULL;
	JsonbValue			   *result,
							key,
							val;

	/* If there's no callback function specified, we're done */
	if (cb_oid == InvalidOid)
		PG_RETURN_VOID();

	if (PG_ARGISNULL(ARG_PARENT))
		elog(ERROR, "parent_relid should not be null");

	if (PG_ARGISNULL(ARG_CHILD))
		elog(ERROR, "partition should not be null");

	/* Both RANGE_START & RANGE_END are not available (HASH) */
	if (PG_ARGISNULL(ARG_RANGE_START) && PG_ARGISNULL(ARG_RANGE_START))
		part_type = PT_HASH;

	/* Either RANGE_START or RANGE_END is missing */
	else if (PG_ARGISNULL(ARG_RANGE_START) || PG_ARGISNULL(ARG_RANGE_START))
		elog(ERROR, "both boundaries must be provided for RANGE partition");

	/* Both RANGE_START & RANGE_END are provided */
	else part_type = PT_RANGE;

	/* Build JSONB according to partitioning type */
	switch (part_type)
	{
		case PT_HASH:
			{
				pushJsonbValue(&jsonb_state, WJB_BEGIN_OBJECT, NULL);

				JSB_INIT_VAL(&key, WJB_KEY, "parent");
				JSB_INIT_VAL(&val, WJB_VALUE, get_rel_name_or_relid(parent_oid));
				JSB_INIT_VAL(&key, WJB_KEY, "partition");
				JSB_INIT_VAL(&val, WJB_VALUE, get_rel_name_or_relid(partition_oid));
				JSB_INIT_VAL(&key, WJB_KEY, "part_type");
				JSB_INIT_VAL(&val, WJB_VALUE, "HASH");

				result = pushJsonbValue(&jsonb_state, WJB_END_OBJECT, NULL);
			}
			break;

		case PT_RANGE:
			{
				char   *start_value,
					   *end_value;
				Oid		type = get_fn_expr_argtype(fcinfo->flinfo, ARG_RANGE_START);

				/* Convert min & max to CSTRING */
				start_value = datum_to_cstring(PG_GETARG_DATUM(ARG_RANGE_START), type);
				end_value = datum_to_cstring(PG_GETARG_DATUM(ARG_RANGE_END), type);

				pushJsonbValue(&jsonb_state, WJB_BEGIN_OBJECT, NULL);

				JSB_INIT_VAL(&key, WJB_KEY, "parent");
				JSB_INIT_VAL(&val, WJB_VALUE, get_rel_name_or_relid(parent_oid));
				JSB_INIT_VAL(&key, WJB_KEY, "partition");
				JSB_INIT_VAL(&val, WJB_VALUE, get_rel_name_or_relid(partition_oid));
				JSB_INIT_VAL(&key, WJB_KEY, "part_type");
				JSB_INIT_VAL(&val, WJB_VALUE, "RANGE");
				JSB_INIT_VAL(&key, WJB_KEY, "start");
				JSB_INIT_VAL(&val, WJB_VALUE, start_value);
				JSB_INIT_VAL(&key, WJB_KEY, "end");
				JSB_INIT_VAL(&val, WJB_VALUE, end_value);

				result = pushJsonbValue(&jsonb_state, WJB_END_OBJECT, NULL);
			}
			break;

		default:
			elog(ERROR, "Unknown partitioning type %u", part_type);
			break;
	}

	/* Validate the callback's signature */
	validate_on_part_init_cb(cb_oid, true);

	fmgr_info(cb_oid, &cb_flinfo);

	InitFunctionCallInfoData(cb_fcinfo, &cb_flinfo, 1, InvalidOid, NULL, NULL);
	cb_fcinfo.arg[0] = PointerGetDatum(JsonbValueToJsonb(result));
	cb_fcinfo.argnull[0] = false;

	/* Invoke the callback */
	FunctionCallInvoke(&cb_fcinfo);

	PG_RETURN_VOID();
}


/*
 * -------
 *  DEBUG
 * -------
 */

/*
 * NOTE: used for DEBUG, set breakpoint here.
 */
Datum
debug_capture(PG_FUNCTION_ARGS)
{
	static float8 sleep_time = 0;
	DirectFunctionCall1(pg_sleep, Float8GetDatum(sleep_time));

	/* Write something (doesn't really matter) */
	elog(WARNING, "debug_capture [%u]", MyProcPid);

	PG_RETURN_VOID();
}
