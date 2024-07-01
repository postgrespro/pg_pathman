/* ------------------------------------------------------------------------
 *
 * utility_stmt_hooking.c
 *		Override COPY TO/FROM and ALTER TABLE ... RENAME statements
 *		for partitioned tables
 *
 * Copyright (c) 2016-2020, Postgres Professional
 * Portions Copyright (c) 1996-2015, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * ------------------------------------------------------------------------
 */

#include "compat/debug_compat_features.h"
#include "compat/pg_compat.h"
#include "init.h"
#include "utility_stmt_hooking.h"
#include "partition_filter.h"

#include "access/htup_details.h"
#if PG_VERSION_NUM >= 120000
#include "access/heapam.h"
#include "access/table.h"
#endif
#include "access/sysattr.h"
#include "access/xact.h"
#include "catalog/namespace.h"
#include "commands/copy.h"
#if PG_VERSION_NUM >= 160000 /* for commit a61b1f74823c */
#include "commands/copyfrom_internal.h"
#endif
#include "commands/defrem.h"
#include "commands/trigger.h"
#include "commands/tablecmds.h"
#include "foreign/fdwapi.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#if PG_VERSION_NUM >= 160000 /* for commit a61b1f74823c */
#include "parser/parse_relation.h"
#endif
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/rls.h"

/* we avoid includig libpq.h because it requires openssl.h */
#include "libpq/pqcomm.h"
extern PGDLLIMPORT ProtocolVersion FrontendProtocol;
extern void pq_endmsgread(void);

/* Determine whether we should enable COPY or not (PostgresPro has a fix) */
#if defined(WIN32) && \
		(!defined(ENABLE_PGPRO_PATCHES) || \
		 !defined(ENABLE_PATHMAN_AWARE_COPY_WIN32) || \
		 !defined(PGPRO_PATHMAN_AWARE_COPY))
#define DISABLE_PATHMAN_COPY
#endif

/*
 * While building PostgreSQL on Windows the msvc compiler produces .def file
 * which contains all the symbols that were declared as external except the ones
 * that were declared but not defined. We redefine variables below to prevent
 * 'unresolved symbol' errors on Windows. But we have to disable COPY feature
 * on Windows.
 */
#ifdef DISABLE_PATHMAN_COPY
bool				XactReadOnly = false;
ProtocolVersion		FrontendProtocol = (ProtocolVersion) 0;
#endif


#define PATHMAN_COPY_READ_LOCK		AccessShareLock
#define PATHMAN_COPY_WRITE_LOCK		RowExclusiveLock


static uint64 PathmanCopyFrom(
#if PG_VERSION_NUM >= 140000 /* Structure changed in c532d15dddff */
							  CopyFromState cstate,
#else
							  CopyState cstate,
#endif
							  Relation parent_rel,
							  List *range_table,
							  bool old_protocol);

static void prepare_rri_for_copy(ResultRelInfoHolder *rri_holder,
								 const ResultPartsStorage *rps_storage);

static void finish_rri_for_copy(ResultRelInfoHolder *rri_holder,
								const ResultPartsStorage *rps_storage);


/*
 * Is pg_pathman supposed to handle this COPY stmt?
 */
bool
is_pathman_related_copy(Node *parsetree)
{
	CopyStmt   *copy_stmt = (CopyStmt *) parsetree;
	Oid			parent_relid;

	Assert(IsPathmanReady());

	if (!IsOverrideCopyEnabled())
	{
		elog(DEBUG1, "COPY statement hooking is disabled");
		return false;
	}

	/* Check that it's a CopyStmt */
	if (!IsA(parsetree, CopyStmt))
		return false;

	/* Also check that stmt->relation exists */
	if (!copy_stmt->relation)
		return false;

	/* Get partition's Oid while locking it */
	parent_relid = RangeVarGetRelid(copy_stmt->relation,
									(copy_stmt->is_from ?
										PATHMAN_COPY_WRITE_LOCK :
										PATHMAN_COPY_READ_LOCK),
									true);

	/* Skip relation if it does not exist (for Citus compatibility) */
	if (!OidIsValid(parent_relid))
		return false;

	/* Check that relation is partitioned */
	if (has_pathman_relation_info(parent_relid))
	{
		ListCell *lc;

		/* Analyze options list */
		foreach (lc, copy_stmt->options)
		{
			DefElem *defel = (DefElem *) lfirst(lc);

			/* We do not support freeze */
			/*
			 * It would be great to allow copy.c extract option value and
			 * check it ready. However, there is no possibility (hooks) to do
			 * that before messaging 'ok, begin streaming data' to the client,
			 * which is ugly and confusing: e.g. it would require us to
			 * actually send something in regression tests before we notice
			 * the error.
			 */
			if (strcmp(defel->defname, "freeze") == 0 && defGetBoolean(defel))
				elog(ERROR, "freeze is not supported for partitioned tables");
		}

		/* Emit ERROR if we can't see the necessary symbols */
		#ifdef DISABLE_PATHMAN_COPY
			elog(ERROR, "COPY is not supported for partitioned tables on Windows");
		#else
			elog(DEBUG1, "Overriding default behavior for COPY [%u]",
				 parent_relid);
		#endif

		return true;
	}

	return false;
}

/*
 * Is pg_pathman supposed to handle this table rename stmt?
 */
bool
is_pathman_related_table_rename(Node *parsetree,
								Oid *relation_oid_out,	/* ret value #1 */
								bool *is_parent_out)	/* ret value #2 */
{
	RenameStmt	   *rename_stmt = (RenameStmt *) parsetree;
	Oid				relation_oid,
					parent_relid;

	Assert(IsPathmanReady());

	/* Set default values */
	if (relation_oid_out) *relation_oid_out = InvalidOid;

	if (!IsA(parsetree, RenameStmt))
		return false;

	/* Are we going to rename some table? */
	if (rename_stmt->renameType != OBJECT_TABLE)
		return false;

	/* Fetch Oid of this relation */
	relation_oid = RangeVarGetRelid(rename_stmt->relation,
									AccessShareLock,
									rename_stmt->missing_ok);

	/* Check ALTER TABLE ... IF EXISTS of nonexistent table */
	if (rename_stmt->missing_ok && relation_oid == InvalidOid)
		return false;

	/* Assume it's a parent */
	if (has_pathman_relation_info(relation_oid))
	{
		if (relation_oid_out)
			*relation_oid_out = relation_oid;
		if (is_parent_out)
			*is_parent_out = true;
		return true;
	}

	/* Assume it's a partition, fetch its parent */
	parent_relid = get_parent_of_partition(relation_oid);
	if (!OidIsValid(parent_relid))
		return false;

	/* Is parent partitioned? */
	if (has_pathman_relation_info(parent_relid))
	{
		if (relation_oid_out)
			*relation_oid_out = relation_oid;
		if (is_parent_out)
			*is_parent_out = false;
		return true;
	}

	return false;
}

/*
 * Is pg_pathman supposed to handle this ALTER COLUMN TYPE stmt?
 */
bool
is_pathman_related_alter_column_type(Node *parsetree,
									 Oid *parent_relid_out,
									 AttrNumber *attr_number_out,
									 PartType *part_type_out)
{
	AlterTableStmt	   *alter_table_stmt = (AlterTableStmt *) parsetree;
	ListCell		   *lc;
	Oid					parent_relid;
	bool				result = false;
	PartRelationInfo   *prel;

	Assert(IsPathmanReady());

	if (!IsA(alter_table_stmt, AlterTableStmt))
		return false;

	/* Are we going to modify some table? */
#if PG_VERSION_NUM >= 140000
	if (alter_table_stmt->objtype != OBJECT_TABLE)
#else
	if (alter_table_stmt->relkind != OBJECT_TABLE)
#endif
		return false;

	/* Assume it's a parent, fetch its Oid */
	parent_relid = RangeVarGetRelid(alter_table_stmt->relation,
									AccessShareLock,
									alter_table_stmt->missing_ok);

	/* Check ALTER TABLE ... IF EXISTS of nonexistent table */
	if (alter_table_stmt->missing_ok && parent_relid == InvalidOid)
		return false;

	/* Is parent partitioned? */
	if ((prel = get_pathman_relation_info(parent_relid)) != NULL)
	{
		/* Return 'parent_relid' and 'prel->parttype' */
		if (parent_relid_out) *parent_relid_out = parent_relid;
		if (part_type_out) *part_type_out = prel->parttype;
	}
	else return false;

	/* Examine command list */
	foreach (lc, alter_table_stmt->cmds)
	{
		AlterTableCmd  *alter_table_cmd = (AlterTableCmd *) lfirst(lc);
		AttrNumber		attnum;
		int				adjusted_attnum;

		if (!IsA(alter_table_cmd, AlterTableCmd))
			continue;

		/* Is it an ALTER COLUMN TYPE statement? */
		if (alter_table_cmd->subtype != AT_AlterColumnType)
			continue;

		/* Is it a column that used in expression? */
		attnum = get_attnum(parent_relid, alter_table_cmd->name);
		adjusted_attnum = attnum - FirstLowInvalidHeapAttributeNumber;
		if (!bms_is_member(adjusted_attnum, prel->expr_atts))
			continue;

		/* Return 'attr_number_out' if asked to */
		if (attr_number_out) *attr_number_out = attnum;

		/* Success! */
		result = true;
	}

	close_pathman_relation_info(prel);

	return result;
}

/*
 * PathmanCopyGetAttnums - build an integer list of attnums to be copied
 *
 * The input attnamelist is either the user-specified column list,
 * or NIL if there was none (in which case we want all the non-dropped
 * columns).
 *
 * rel can be NULL ... it's only used for error reports.
 */
static List *
PathmanCopyGetAttnums(TupleDesc tupDesc, Relation rel, List *attnamelist)
{
	List	   *attnums = NIL;

	if (attnamelist == NIL)
	{
		/* Generate default column list */
		int			attr_count = tupDesc->natts;
		int			i;

		for (i = 0; i < attr_count; i++)
		{
			if (TupleDescAttr(tupDesc, i)->attisdropped)
				continue;
			attnums = lappend_int(attnums, i + 1);
		}
	}
	else
	{
		/* Validate the user-supplied list and extract attnums */
		ListCell   *l;

		foreach(l, attnamelist)
		{
			char	   *name = strVal(lfirst(l));
			int			attnum;
			int			i;

			/* Lookup column name */
			attnum = InvalidAttrNumber;
			for (i = 0; i < tupDesc->natts; i++)
			{
				Form_pg_attribute att = TupleDescAttr(tupDesc, i);

				if (att->attisdropped)
					continue;
				if (namestrcmp(&(att->attname), name) == 0)
				{
					attnum = att->attnum;
					break;
				}
			}
			if (attnum == InvalidAttrNumber)
			{
				if (rel != NULL)
					ereport(ERROR,
							(errcode(ERRCODE_UNDEFINED_COLUMN),
					errmsg("column \"%s\" of relation \"%s\" does not exist",
						   name, RelationGetRelationName(rel))));
				else
					ereport(ERROR,
							(errcode(ERRCODE_UNDEFINED_COLUMN),
							 errmsg("column \"%s\" does not exist",
									name)));
			}
			/* Check for duplicates */
			if (list_member_int(attnums, attnum))
				ereport(ERROR,
						(errcode(ERRCODE_DUPLICATE_COLUMN),
						 errmsg("column \"%s\" specified more than once",
								name)));
			attnums = lappend_int(attnums, attnum);
		}
	}

	return attnums;
}

/*
 * Execute COPY TO/FROM statement for a partitioned table.
 * NOTE: based on DoCopy() (see copy.c).
 */
void
PathmanDoCopy(const CopyStmt *stmt,
			  const char *queryString,
			  int stmt_location,
			  int stmt_len,
			  uint64 *processed)
{
#if PG_VERSION_NUM >= 140000 /* Structure changed in c532d15dddff */
	CopyFromState cstate;
#else
	CopyState	cstate;
#endif
	ParseState *pstate;
	Relation	rel;
	List	   *range_table = NIL;
	bool		is_from = stmt->is_from,
				pipe = (stmt->filename == NULL),
				is_old_protocol = PG_PROTOCOL_MAJOR(FrontendProtocol) < 3 && pipe;

	/* Disallow COPY TO/FROM file or program except to superusers. */
	if (!pipe && !superuser())
	{
		if (stmt->is_program)
			ereport(ERROR,
					(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
					 errmsg("must be superuser to COPY to or from an external program"),
					 errhint("Anyone can COPY to stdout or from stdin. "
						   "psql's \\copy command also works for anyone.")));
		else
			ereport(ERROR,
					(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
					 errmsg("must be superuser to COPY to or from a file"),
					 errhint("Anyone can COPY to stdout or from stdin. "
						   "psql's \\copy command also works for anyone.")));
	}

	pstate = make_parsestate(NULL);
	pstate->p_sourcetext = queryString;

	/* Check that we have a relation */
	if (stmt->relation)
	{
		TupleDesc		tupDesc;
		AclMode			required_access = (is_from ? ACL_INSERT : ACL_SELECT);
		List		   *attnums;
		ListCell	   *cur;
		RangeTblEntry  *rte;
#if PG_VERSION_NUM >= 160000 /* for commit a61b1f74823c */
		RTEPermissionInfo *perminfo;
#endif

		Assert(!stmt->query);

		/* Open the relation (we've locked it in is_pathman_related_copy()) */
		rel = heap_openrv_compat(stmt->relation, NoLock);

		rte = makeNode(RangeTblEntry);
		rte->rtekind = RTE_RELATION;
		rte->relid = RelationGetRelid(rel);
		rte->relkind = rel->rd_rel->relkind;
#if PG_VERSION_NUM >= 160000 /* for commit a61b1f74823c */
		pstate->p_rtable = lappend(pstate->p_rtable, rte);
		perminfo = addRTEPermissionInfo(&pstate->p_rteperminfos, rte);
		perminfo->requiredPerms = required_access;
#else
		rte->requiredPerms = required_access;
#endif
		range_table = list_make1(rte);

		tupDesc = RelationGetDescr(rel);
		attnums = PathmanCopyGetAttnums(tupDesc, rel, stmt->attlist);
#if PG_VERSION_NUM >= 160000 /* for commit a61b1f74823c */
		foreach(cur, attnums)
		{
			int attno;
			Bitmapset **bms;

			attno = lfirst_int(cur) - FirstLowInvalidHeapAttributeNumber;
			bms = is_from ? &perminfo->insertedCols : &perminfo->selectedCols;

			*bms = bms_add_member(*bms, attno);
		}
		ExecCheckPermissions(pstate->p_rtable, list_make1(perminfo), true);
#else
		foreach(cur, attnums)
		{
			int attnum = lfirst_int(cur) - FirstLowInvalidHeapAttributeNumber;

			if (is_from)
				rte->insertedCols = bms_add_member(rte->insertedCols, attnum);
			else
				rte->selectedCols = bms_add_member(rte->selectedCols, attnum);
		}
		ExecCheckRTPerms(range_table, true);
#endif

		/* Disable COPY FROM if table has RLS */
		if (is_from && check_enable_rls(rte->relid, InvalidOid, false) == RLS_ENABLED)
		{
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				  errmsg("COPY FROM not supported with row-level security"),
						 errhint("Use INSERT statements instead.")));
		}

		/* Disable COPY TO */
		if (!is_from)
		{
			ereport(WARNING,
					(errmsg("COPY TO will only select rows from parent table \"%s\"",
							RelationGetRelationName(rel)),
					 errhint("Consider using the COPY (SELECT ...) TO variant.")));
		}
	}

	/* This should never happen (see is_pathman_related_copy()) */
	else elog(ERROR, "error in function " CppAsString(PathmanDoCopy));

	if (is_from)
	{
		/* check read-only transaction and parallel mode */
		if (XactReadOnly && !rel->rd_islocaltemp)
			PreventCommandIfReadOnly("COPY FROM");
		PreventCommandIfParallelMode("COPY FROM");

		cstate = BeginCopyFromCompat(pstate, rel, stmt->filename,
									 stmt->is_program, NULL, stmt->attlist,
									 stmt->options);
		*processed = PathmanCopyFrom(cstate, rel, range_table, is_old_protocol);
		EndCopyFrom(cstate);
	}
	else
	{
#if PG_VERSION_NUM >= 160000 /* for commit f75cec4fff87 */
		/*
		 * Forget current RangeTblEntries and RTEPermissionInfos.
		 * Standard DoCopy will create new ones.
		 */
		pstate->p_rtable = NULL;
		pstate->p_rteperminfos = NULL;
#endif
		/* Call standard DoCopy using a new CopyStmt */
		DoCopyCompat(pstate, stmt, stmt_location, stmt_len, processed);
	}

	/* Close the relation, but keep it locked */
	heap_close_compat(rel, (is_from ? NoLock : PATHMAN_COPY_READ_LOCK));
}

/*
 * Copy FROM file to relation.
 */
static uint64
PathmanCopyFrom(
#if PG_VERSION_NUM >= 140000 /* Structure changed in c532d15dddff */
				CopyFromState cstate,
#else
				CopyState cstate,
#endif
				Relation parent_rel,
				List *range_table, bool old_protocol)
{
	HeapTuple			tuple;
	TupleDesc			tupDesc;
	Datum			   *values;
	bool			   *nulls;

	ResultPartsStorage	parts_storage;
	ResultRelInfo	   *parent_rri;
	Oid					parent_relid = RelationGetRelid(parent_rel);

	MemoryContext		query_mcxt = CurrentMemoryContext;
	EState			   *estate = CreateExecutorState(); /* for ExecConstraints() */
	TupleTableSlot	   *myslot;

	uint64				processed = 0;

	tupDesc = RelationGetDescr(parent_rel);

	parent_rri = makeNode(ResultRelInfo);
	InitResultRelInfoCompat(parent_rri,
							parent_rel,
							1,		/* dummy rangetable index */
							0);
	ExecOpenIndices(parent_rri, false);

#if PG_VERSION_NUM >= 140000  /* reworked in 1375422c7826 */
	/*
	 * Call ExecInitRangeTable() should be first because in 14+ it initializes
	 * field "estate->es_result_relations":
	 */
#if PG_VERSION_NUM >= 160000
	ExecInitRangeTable(estate, range_table, cstate->rteperminfos);
#else
	ExecInitRangeTable(estate, range_table);
#endif
	estate->es_result_relations =
		(ResultRelInfo **) palloc0(list_length(range_table) * sizeof(ResultRelInfo *));
	estate->es_result_relations[0] = parent_rri;
	/*
	 * Saving in the list allows to avoid needlessly traversing the whole
	 * array when only a few of its entries are possibly non-NULL.
	 */
	estate->es_opened_result_relations =
		lappend(estate->es_opened_result_relations, parent_rri);
	estate->es_result_relation_info = parent_rri;
#else
	estate->es_result_relations = parent_rri;
	estate->es_num_result_relations = 1;
	estate->es_result_relation_info = parent_rri;
#if PG_VERSION_NUM >= 120000
	ExecInitRangeTable(estate, range_table);
#else
	estate->es_range_table = range_table;
#endif
#endif
	/* Initialize ResultPartsStorage */
	init_result_parts_storage(&parts_storage,
							  parent_relid, parent_rri,
							  estate, CMD_INSERT,
							  RPS_CLOSE_RELATIONS,
							  RPS_DEFAULT_SPECULATIVE,
							  RPS_RRI_CB(prepare_rri_for_copy, cstate),
							  RPS_RRI_CB(finish_rri_for_copy, NULL));
#if PG_VERSION_NUM >= 160000 /* for commit a61b1f74823c */
	/* ResultRelInfo of partitioned table. */
	parts_storage.init_rri = parent_rri;

	/*
	 * Copy the RTEPermissionInfos into estate as well, so that
	 * scan_result_parts_storage() et al will work correctly.
	 */
	estate->es_rteperminfos = cstate->rteperminfos;
#endif

	/* Set up a tuple slot too */
	myslot = ExecInitExtraTupleSlotCompat(estate, NULL, &TTSOpsHeapTuple);
	/* Triggers might need a slot as well */
#if PG_VERSION_NUM < 120000
	estate->es_trig_tuple_slot = ExecInitExtraTupleSlotCompat(estate, tupDesc, nothing_here);
#endif

	/* Prepare to catch AFTER triggers. */
	AfterTriggerBeginQuery();

	/*
	 * Check BEFORE STATEMENT insertion triggers. It's debatable whether we
	 * should do this for COPY, since it's not really an "INSERT" statement as
	 * such. However, executing these triggers maintains consistency with the
	 * EACH ROW triggers that we already fire on COPY.
	 */
	ExecBSInsertTriggers(estate, parent_rri);

	values = (Datum *) palloc(tupDesc->natts * sizeof(Datum));
	nulls = (bool *) palloc(tupDesc->natts * sizeof(bool));

	for (;;)
	{
		TupleTableSlot		   *slot;
		bool					skip_tuple = false;
#if PG_VERSION_NUM < 120000
		Oid						tuple_oid = InvalidOid;
#endif
		ExprContext		 	   *econtext = GetPerTupleExprContext(estate);

		ResultRelInfoHolder	   *rri_holder;
		ResultRelInfo		   *child_rri;

		CHECK_FOR_INTERRUPTS();

		ResetPerTupleExprContext(estate);

		/* Switch into per tuple memory context */
		MemoryContextSwitchTo(GetPerTupleMemoryContext(estate));

		if (!NextCopyFromCompat(cstate, econtext, values, nulls, &tuple_oid))
			break;

		/* We can form the input tuple */
		tuple = heap_form_tuple(tupDesc, values, nulls);

#if PG_VERSION_NUM < 120000
		if (tuple_oid != InvalidOid)
			HeapTupleSetOid(tuple, tuple_oid);
#endif

		/* Place tuple in tuple slot --- but slot shouldn't free it */
		slot = myslot;
		ExecSetSlotDescriptor(slot, tupDesc);
#if PG_VERSION_NUM >= 120000
		ExecStoreHeapTuple(tuple, slot, false);
#else
		ExecStoreTuple(tuple, slot, InvalidBuffer, false);
#endif

		/* Search for a matching partition */
		rri_holder = select_partition_for_insert(estate, &parts_storage, slot);
		child_rri = rri_holder->result_rel_info;

		/* Magic: replace parent's ResultRelInfo with ours */
		estate->es_result_relation_info = child_rri;

		/*
		 * Constraints might reference the tableoid column, so initialize
		 * t_tableOid before evaluating them.
		 */
		tuple->t_tableOid = RelationGetRelid(child_rri->ri_RelationDesc);

		/* If there's a transform map, rebuild the tuple */
		if (rri_holder->tuple_map)
		{
			HeapTuple tuple_old;

			tuple_old = tuple;
#if PG_VERSION_NUM >= 120000
			tuple = execute_attr_map_tuple(tuple, rri_holder->tuple_map);
#else
			tuple = do_convert_tuple(tuple, rri_holder->tuple_map);
#endif
			heap_freetuple(tuple_old);
		}

		/* Now we can set proper tuple descriptor according to child relation */
		ExecSetSlotDescriptor(slot, RelationGetDescr(child_rri->ri_RelationDesc));
#if PG_VERSION_NUM >= 120000
		ExecStoreHeapTuple(tuple, slot, false);
#else
		ExecStoreTuple(tuple, slot, InvalidBuffer, false);
#endif

		/* Triggers and stuff need to be invoked in query context. */
		MemoryContextSwitchTo(query_mcxt);

		/* BEFORE ROW INSERT Triggers */
		if (child_rri->ri_TrigDesc &&
			child_rri->ri_TrigDesc->trig_insert_before_row)
		{
#if PG_VERSION_NUM >= 120000
			if (!ExecBRInsertTriggers(estate, child_rri, slot))
				skip_tuple = true;
			else	/* trigger might have changed tuple */
				tuple = ExecFetchSlotHeapTuple(slot, false, NULL);
#else
			slot = ExecBRInsertTriggers(estate, child_rri, slot);

			if (slot == NULL)	/* "do nothing" */
				skip_tuple = true;
			else	/* trigger might have changed tuple */
			{
				tuple = ExecMaterializeSlot(slot);
			}
#endif
		}

		/* Proceed if we still have a tuple */
		if (!skip_tuple)
		{
			List *recheckIndexes = NIL;

			/* Check the constraints of the tuple */
			if (child_rri->ri_RelationDesc->rd_att->constr)
				ExecConstraints(child_rri, slot, estate);

			/* Handle local tables */
			if (!child_rri->ri_FdwRoutine)
			{
				/* OK, now store the tuple... */
				simple_heap_insert(child_rri->ri_RelationDesc, tuple);
#if PG_VERSION_NUM >= 120000 /* since 12, tid lives directly in slot */
				ItemPointerCopy(&tuple->t_self, &slot->tts_tid);
				/* and we must stamp tableOid as we go around table_tuple_insert */
				slot->tts_tableOid = RelationGetRelid(child_rri->ri_RelationDesc);
#endif

				/* ... and create index entries for it */
				if (child_rri->ri_NumIndices > 0)
					recheckIndexes = ExecInsertIndexTuplesCompat(estate->es_result_relation_info,
										slot, &(tuple->t_self), estate, false, false, NULL, NIL, false);
			}
#ifdef PG_SHARDMAN
			/* Handle foreign tables */
			else
			{
				child_rri->ri_FdwRoutine->ForeignNextCopyFrom(estate,
															  child_rri,
															  cstate);
			}
#endif

			/* AFTER ROW INSERT Triggers (FIXME: NULL transition) */
#if PG_VERSION_NUM >= 120000
			ExecARInsertTriggersCompat(estate, child_rri, slot,
									   recheckIndexes, NULL);
#else
			ExecARInsertTriggersCompat(estate, child_rri, tuple,
									   recheckIndexes, NULL);
#endif

			list_free(recheckIndexes);

			/*
			 * We count only tuples not suppressed by a BEFORE INSERT trigger;
			 * this is the same definition used by execMain.c for counting
			 * tuples inserted by an INSERT command.
			 */
			processed++;
		}
	}

	/* Switch back to query context */
	MemoryContextSwitchTo(query_mcxt);

	/* Required for old protocol */
	if (old_protocol)
		pq_endmsgread();

	/* Execute AFTER STATEMENT insertion triggers (FIXME: NULL transition) */
	ExecASInsertTriggersCompat(estate, parent_rri, NULL);

	/* Handle queued AFTER triggers */
	AfterTriggerEndQuery(estate);

	pfree(values);
	pfree(nulls);

	/* Release resources for tuple table */
	ExecResetTupleTable(estate->es_tupleTable, false);

	/* Close partitions and destroy hash table */
	fini_result_parts_storage(&parts_storage);

	/* Close parent's indices */
	ExecCloseIndices(parent_rri);

	/* Release an EState along with all remaining working storage */
	FreeExecutorState(estate);

	return processed;
}

/*
 * Init COPY FROM, if supported.
 */
static void
prepare_rri_for_copy(ResultRelInfoHolder *rri_holder,
					 const ResultPartsStorage *rps_storage)
{
	ResultRelInfo	*rri = rri_holder->result_rel_info;
	FdwRoutine		*fdw_routine = rri->ri_FdwRoutine;

	if (fdw_routine != NULL)
	{
		/*
		 * If this PostgreSQL edition has no idea about shardman, behave as usual:
		 * vanilla Postgres doesn't support COPY FROM to foreign partitions.
		 * However, shardman patches to core extend FDW API to allow it.
		 */
#ifdef PG_SHARDMAN
		/* shardman COPY FROM requested? */
		if (*find_rendezvous_variable(
				"shardman_pathman_copy_from_rendezvous") != NULL &&
			FdwCopyFromIsSupported(fdw_routine))
		{
			CopyState		cstate = (CopyState) rps_storage->init_rri_holder_cb_arg;
			ResultRelInfo	*parent_rri = rps_storage->base_rri;
			EState			*estate = rps_storage->estate;

			fdw_routine->BeginForeignCopyFrom(estate, rri, cstate, parent_rri);
			return;
		}
#endif

		elog(ERROR, "cannot copy to foreign partition \"%s\"",
			 get_rel_name(RelationGetRelid(rri->ri_RelationDesc)));
	}
}

/*
 * Shutdown FDWs.
 */
static void
finish_rri_for_copy(ResultRelInfoHolder *rri_holder,
					const ResultPartsStorage *rps_storage)
{
#ifdef PG_SHARDMAN
	ResultRelInfo *resultRelInfo = rri_holder->result_rel_info;

	if (resultRelInfo->ri_FdwRoutine)
		resultRelInfo->ri_FdwRoutine->EndForeignCopyFrom(rps_storage->estate,
														 resultRelInfo);
#endif
}

/*
 * Rename RANGE\HASH check constraint of a partition on table rename event.
 */
void
PathmanRenameConstraint(Oid partition_relid,			/* partition Oid */
						const RenameStmt *rename_stmt)	/* partition rename stmt */
{
	char		   *old_constraint_name,
				   *new_constraint_name;
	RenameStmt		rename_con_stmt;

	/* Generate old constraint name */
	old_constraint_name =
			build_check_constraint_name_relid_internal(partition_relid);

	/* Generate new constraint name */
	new_constraint_name =
			build_check_constraint_name_relname_internal(rename_stmt->newname);

	/* Build check constraint RENAME statement */
	memset((void *) &rename_con_stmt, 0, sizeof(RenameStmt));
	NodeSetTag(&rename_con_stmt, T_RenameStmt);
	rename_con_stmt.renameType	= OBJECT_TABCONSTRAINT;
	rename_con_stmt.relation	= rename_stmt->relation;
	rename_con_stmt.subname		= old_constraint_name;
	rename_con_stmt.newname		= new_constraint_name;
	rename_con_stmt.missing_ok	= false;

	/* Finally, rename partitioning constraint */
	RenameConstraint(&rename_con_stmt);

	pfree(old_constraint_name);
	pfree(new_constraint_name);

	/* Make changes visible */
	CommandCounterIncrement();
}

/*
 * Rename auto naming sequence of a parent on table rename event.
 */
void
PathmanRenameSequence(Oid parent_relid,					/* parent Oid */
					  const RenameStmt *rename_stmt)	/* parent rename stmt */
{
	char	   *old_seq_name,
			   *new_seq_name,
			   *seq_nsp_name;
	RangeVar   *seq_rv;
	Oid			seq_relid;

	/* Produce old & new names and RangeVar */
	seq_nsp_name	= get_namespace_name(get_rel_namespace(parent_relid));
	old_seq_name	= build_sequence_name_relid_internal(parent_relid);
	new_seq_name	= build_sequence_name_relname_internal(rename_stmt->newname);
	seq_rv			= makeRangeVar(seq_nsp_name, old_seq_name, -1);

	/* Fetch Oid of sequence */
	seq_relid = RangeVarGetRelid(seq_rv, AccessExclusiveLock, true);

	/* Do nothing if there's no naming sequence */
	if (!OidIsValid(seq_relid))
		return;

	/* Finally, rename auto naming sequence */
	RenameRelationInternalCompat(seq_relid, new_seq_name, false, false);

	pfree(seq_nsp_name);
	pfree(old_seq_name);
	pfree(new_seq_name);
	pfree(seq_rv);

	/* Make changes visible */
	CommandCounterIncrement();
}
