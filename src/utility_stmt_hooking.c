/* ------------------------------------------------------------------------
 *
 * utility_stmt_hooking.c
 *		Override COPY TO/FROM and ALTER TABLE ... RENAME statements
 *		for partitioned tables
 *
 * Copyright (c) 2016, Postgres Professional
 * Portions Copyright (c) 1996-2015, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * ------------------------------------------------------------------------
 */

#include "init.h"
#include "utility_stmt_hooking.h"
#include "partition_filter.h"
#include "relation_info.h"

#include "access/htup_details.h"
#include "access/sysattr.h"
#include "access/xact.h"
#include "catalog/namespace.h"
#include "commands/copy.h"
#include "commands/trigger.h"
#include "commands/tablecmds.h"
#include "foreign/fdwapi.h"
#include "miscadmin.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/rls.h"

#include "libpq/libpq.h"


/*
 * Determine whether we should enable COPY or not (PostgresPro has a fix).
 */
#if defined(WIN32) && !defined(PGPRO_PATHMAN_AWARE_COPY)
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


static uint64 PathmanCopyFrom(CopyState cstate,
							  Relation parent_rel,
							  List *range_table,
							  bool old_protocol);

static void prepare_rri_for_copy(EState *estate,
								 ResultRelInfoHolder *rri_holder,
								 const ResultPartsStorage *rps_storage,
								 void *arg);


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
										RowExclusiveLock :
										AccessShareLock),
									false);

	/* Check that relation is partitioned */
	if (get_pathman_relation_info(parent_relid))
	{
		ListCell *lc;

		/* Analyze options list */
		foreach (lc, copy_stmt->options)
		{
			DefElem *defel = (DefElem *) lfirst(lc);

			Assert(IsA(defel, DefElem));

			/* We do not support freeze */
			if (strcmp(defel->defname, "freeze") == 0)
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
								Oid *partition_relid_out,			/* ret value */
								AttrNumber *partitioned_col_out)	/* ret value */
{
	RenameStmt			   *rename_stmt = (RenameStmt *) parsetree;
	Oid						partition_relid,
							parent_relid;
	const PartRelationInfo *prel;
	PartParentSearch		parent_search;

	Assert(IsPathmanReady());

	/* Set default values */
	if (partition_relid_out) *partition_relid_out = InvalidOid;
	if (partitioned_col_out) *partitioned_col_out = InvalidAttrNumber;

	if (!IsA(parsetree, RenameStmt))
		return false;

	/* Are we going to rename some table? */
	if (rename_stmt->renameType != OBJECT_TABLE)
		return false;

	/* Assume it's a partition, fetch its Oid */
	partition_relid = RangeVarGetRelid(rename_stmt->relation,
									   AccessShareLock,
									   false);

	/* Try fetching parent of this table */
	parent_relid = get_parent_of_partition(partition_relid, &parent_search);
	if (parent_search != PPS_ENTRY_PART_PARENT)
		return false;

	/* Is parent partitioned? */
	if ((prel = get_pathman_relation_info(parent_relid)) != NULL)
	{
		/* Return 'partition_relid' and 'prel->attnum' */
		if (partition_relid_out) *partition_relid_out = partition_relid;
		if (partitioned_col_out) *partitioned_col_out = prel->attnum;

		return true;
	}

	return false;
}


/*
 * CopyGetAttnums - build an integer list of attnums to be copied
 *
 * The input attnamelist is either the user-specified column list,
 * or NIL if there was none (in which case we want all the non-dropped
 * columns).
 *
 * rel can be NULL ... it's only used for error reports.
 */
static List *
CopyGetAttnums(TupleDesc tupDesc, Relation rel, List *attnamelist)
{
	List	   *attnums = NIL;

	if (attnamelist == NIL)
	{
		/* Generate default column list */
		Form_pg_attribute *attr = tupDesc->attrs;
		int			attr_count = tupDesc->natts;
		int			i;

		for (i = 0; i < attr_count; i++)
		{
			if (attr[i]->attisdropped)
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
				if (tupDesc->attrs[i]->attisdropped)
					continue;
				if (namestrcmp(&(tupDesc->attrs[i]->attname), name) == 0)
				{
					attnum = tupDesc->attrs[i]->attnum;
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
PathmanDoCopy(const CopyStmt *stmt, const char *queryString, uint64 *processed)
{
	CopyState	cstate;
	bool		is_from = stmt->is_from;
	bool		pipe = (stmt->filename == NULL);
	Relation	rel;
	Node	   *query = NULL;
	List	   *range_table = NIL;

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

	/* Check that we have a relation */
	if (stmt->relation)
	{
		TupleDesc		tupDesc;
		AclMode			required_access = (is_from ? ACL_INSERT : ACL_SELECT);
		List		   *attnums;
		ListCell	   *cur;
		RangeTblEntry  *rte;

		Assert(!stmt->query);

		/* Open the relation (we've locked it in is_pathman_related_copy()) */
		rel = heap_openrv(stmt->relation, NoLock);

		rte = makeNode(RangeTblEntry);
		rte->rtekind = RTE_RELATION;
		rte->relid = RelationGetRelid(rel);
		rte->relkind = rel->rd_rel->relkind;
		rte->requiredPerms = required_access;
		range_table = list_make1(rte);

		tupDesc = RelationGetDescr(rel);
		attnums = CopyGetAttnums(tupDesc, rel, stmt->attlist);
		foreach(cur, attnums)
		{
			int attno = lfirst_int(cur) - FirstLowInvalidHeapAttributeNumber;

			if (is_from)
				rte->insertedCols = bms_add_member(rte->insertedCols, attno);
			else
				rte->selectedCols = bms_add_member(rte->selectedCols, attno);
		}
		ExecCheckRTPerms(range_table, true);

		/*
		 * We should perform a query instead of low-level heap scan whenever:
		 *		a) table has a RLS policy;
		 *		b) table is partitioned & it's COPY FROM.
		 */
		if (check_enable_rls(rte->relid, InvalidOid, false) == RLS_ENABLED ||
			is_from == false) /* rewrite COPY table TO statements */
		{
			SelectStmt *select;
			RangeVar   *from;
			List	   *target_list = NIL;

			if (is_from)
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				  errmsg("COPY FROM not supported with row-level security"),
						 errhint("Use INSERT statements instead.")));

			/* Build target list */
			if (!stmt->attlist)
			{
				ColumnRef  *cr;
				ResTarget  *target;

				cr = makeNode(ColumnRef);
				cr->fields = list_make1(makeNode(A_Star));
				cr->location = -1;

				/* Build the ResTarget and add the ColumnRef to it. */
				target = makeNode(ResTarget);
				target->name = NULL;
				target->indirection = NIL;
				target->val = (Node *) cr;
				target->location = -1;

				target_list = list_make1(target);
			}
			else
			{
				ListCell   *lc;

				foreach(lc, stmt->attlist)
				{
					ColumnRef  *cr;
					ResTarget  *target;

					/*
					 * Build the ColumnRef for each column.  The ColumnRef
					 * 'fields' property is a String 'Value' node (see
					 * nodes/value.h) that corresponds to the column name
					 * respectively.
					 */
					cr = makeNode(ColumnRef);
					cr->fields = list_make1(lfirst(lc));
					cr->location = -1;

					/* Build the ResTarget and add the ColumnRef to it. */
					target = makeNode(ResTarget);
					target->name = NULL;
					target->indirection = NIL;
					target->val = (Node *) cr;
					target->location = -1;

					/* Add each column to the SELECT statements target list */
					target_list = lappend(target_list, target);
				}
			}

			/*
			 * Build RangeVar for from clause, fully qualified based on the
			 * relation which we have opened and locked.
			 */
			from = makeRangeVar(get_namespace_name(RelationGetNamespace(rel)),
								RelationGetRelationName(rel), -1);

			/* Build query */
			select = makeNode(SelectStmt);
			select->targetList = target_list;
			select->fromClause = list_make1(from);

			query = (Node *) select;

			/*
			 * Close the relation for now, but keep the lock on it to prevent
			 * changes between now and when we start the query-based COPY.
			 *
			 * We'll reopen it later as part of the query-based COPY.
			 */
			heap_close(rel, NoLock);
			rel = NULL;
		}
	}

	/* This should never happen (see is_pathman_related_copy()) */
	else elog(ERROR, "error in function \"%s\"", CppAsString(PathmanDoCopy));

	/* COPY ... FROM ... */
	if (is_from)
	{
		bool is_old_protocol = PG_PROTOCOL_MAJOR(FrontendProtocol) < 3 &&
							   stmt->filename == NULL;

		/* There should be relation */
		if (!rel) elog(FATAL, "No relation for PATHMAN COPY FROM");

		/* check read-only transaction and parallel mode */
		if (XactReadOnly && !rel->rd_islocaltemp)
			PreventCommandIfReadOnly("PATHMAN COPY FROM");
		PreventCommandIfParallelMode("PATHMAN COPY FROM");

		cstate = BeginCopyFrom(rel, stmt->filename, stmt->is_program,
							   stmt->attlist, stmt->options);
		*processed = PathmanCopyFrom(cstate, rel, range_table, is_old_protocol);
		EndCopyFrom(cstate);
	}
	/* COPY ... TO ... */
	else
	{
		CopyStmt	modified_copy_stmt;

		/* We should've created a query */
		Assert(query);

		/* Copy 'stmt' and override some of the fields */
		modified_copy_stmt = *stmt;
		modified_copy_stmt.relation = NULL;
		modified_copy_stmt.query = query;

		/* Call standard DoCopy using a new CopyStmt */
		DoCopy(&modified_copy_stmt, queryString, processed);
	}

	/*
	 * Close the relation. If reading, we can release the AccessShareLock we
	 * got; if writing, we should hold the lock until end of transaction to
	 * ensure that updates will be committed before lock is released.
	 */
	if (rel != NULL)
		heap_close(rel, (is_from ? NoLock : AccessShareLock));
}

/*
 * Copy FROM file to relation.
 */
static uint64
PathmanCopyFrom(CopyState cstate, Relation parent_rel,
				List *range_table, bool old_protocol)
{
	HeapTuple			tuple;
	TupleDesc			tupDesc;
	Datum			   *values;
	bool			   *nulls;

	ResultPartsStorage	parts_storage;
	ResultRelInfo	   *parent_result_rel;

	EState			   *estate = CreateExecutorState(); /* for ExecConstraints() */
	ExprContext		   *econtext;
	TupleTableSlot	   *myslot;
	MemoryContext		oldcontext = CurrentMemoryContext;

	uint64				processed = 0;


	tupDesc = RelationGetDescr(parent_rel);

	parent_result_rel = makeNode(ResultRelInfo);
	InitResultRelInfo(parent_result_rel,
					  parent_rel,
					  1,		/* dummy rangetable index */
					  0);
	ExecOpenIndices(parent_result_rel, false);

	estate->es_result_relations = parent_result_rel;
	estate->es_num_result_relations = 1;
	estate->es_result_relation_info = parent_result_rel;
	estate->es_range_table = range_table;

	/* Initialize ResultPartsStorage */
	init_result_parts_storage(&parts_storage, estate, false,
							  ResultPartsStorageStandard,
							  prepare_rri_for_copy, NULL);
	parts_storage.saved_rel_info = parent_result_rel;

	/* Set up a tuple slot too */
	myslot = ExecInitExtraTupleSlot(estate);
	ExecSetSlotDescriptor(myslot, tupDesc);
	/* Triggers might need a slot as well */
	estate->es_trig_tuple_slot = ExecInitExtraTupleSlot(estate);

	/* Prepare to catch AFTER triggers. */
	AfterTriggerBeginQuery();

	/*
	 * Check BEFORE STATEMENT insertion triggers. It's debatable whether we
	 * should do this for COPY, since it's not really an "INSERT" statement as
	 * such. However, executing these triggers maintains consistency with the
	 * EACH ROW triggers that we already fire on COPY.
	 */
	ExecBSInsertTriggers(estate, parent_result_rel);

	values = (Datum *) palloc(tupDesc->natts * sizeof(Datum));
	nulls = (bool *) palloc(tupDesc->natts * sizeof(bool));

	econtext = GetPerTupleExprContext(estate);

	for (;;)
	{
		TupleTableSlot		   *slot;
		bool					skip_tuple;
		Oid						tuple_oid = InvalidOid;

		const PartRelationInfo *prel;
		ResultRelInfoHolder	   *rri_holder_child;
		ResultRelInfo		   *child_result_rel;

		CHECK_FOR_INTERRUPTS();

		ResetPerTupleExprContext(estate);

		/* Fetch PartRelationInfo for parent relation */
		prel = get_pathman_relation_info(RelationGetRelid(parent_rel));

		/* Switch into per tuple memory context */
		MemoryContextSwitchTo(GetPerTupleMemoryContext(estate));

		if (!NextCopyFrom(cstate, econtext, values, nulls, &tuple_oid))
			break;

		if (nulls[prel->attnum - 1])
			elog(ERROR, ERR_PART_ATTR_NULL);

		/* Search for a matching partition */
		rri_holder_child = select_partition_for_insert(prel, &parts_storage,
													   values[prel->attnum - 1],
													   prel->atttype, estate);
		child_result_rel = rri_holder_child->result_rel_info;
		estate->es_result_relation_info = child_result_rel;

		/* And now we can form the input tuple. */
		tuple = heap_form_tuple(tupDesc, values, nulls);
		if (tuple_oid != InvalidOid)
			HeapTupleSetOid(tuple, tuple_oid);

		/*
		 * Constraints might reference the tableoid column, so initialize
		 * t_tableOid before evaluating them.
		 */
		tuple->t_tableOid = RelationGetRelid(child_result_rel->ri_RelationDesc);

		/* Triggers and stuff need to be invoked in query context. */
		MemoryContextSwitchTo(oldcontext);

		/* Place tuple in tuple slot --- but slot shouldn't free it */
		slot = myslot;
		ExecStoreTuple(tuple, slot, InvalidBuffer, false);

		skip_tuple = false;

		/* BEFORE ROW INSERT Triggers */
		if (child_result_rel->ri_TrigDesc &&
			child_result_rel->ri_TrigDesc->trig_insert_before_row)
		{
			slot = ExecBRInsertTriggers(estate, child_result_rel, slot);

			if (slot == NULL)	/* "do nothing" */
				skip_tuple = true;
			else	/* trigger might have changed tuple */
				tuple = ExecMaterializeSlot(slot);
		}

		/* Proceed if we still have a tuple */
		if (!skip_tuple)
		{
			List *recheckIndexes = NIL;

			/* Check the constraints of the tuple */
			if (child_result_rel->ri_RelationDesc->rd_att->constr)
				ExecConstraints(child_result_rel, slot, estate);

			/* OK, store the tuple and create index entries for it */
			simple_heap_insert(child_result_rel->ri_RelationDesc, tuple);

			if (child_result_rel->ri_NumIndices > 0)
				recheckIndexes = ExecInsertIndexTuples(slot, &(tuple->t_self),
													   estate, false, NULL, NIL);

			/* AFTER ROW INSERT Triggers */
			ExecARInsertTriggers(estate, child_result_rel, tuple,
								 recheckIndexes);

			list_free(recheckIndexes);

			/*
			 * We count only tuples not suppressed by a BEFORE INSERT trigger;
			 * this is the same definition used by execMain.c for counting
			 * tuples inserted by an INSERT command.
			 */
			processed++;
		}
	}

	MemoryContextSwitchTo(oldcontext);

	/*
	 * In the old protocol, tell pqcomm that we can process normal protocol
	 * messages again.
	 */
	if (old_protocol)
		pq_endmsgread();

	/* Execute AFTER STATEMENT insertion triggers */
	ExecASInsertTriggers(estate, parent_result_rel);

	/* Handle queued AFTER triggers */
	AfterTriggerEndQuery(estate);

	pfree(values);
	pfree(nulls);

	ExecResetTupleTable(estate->es_tupleTable, false);

	/* Close partitions and destroy hash table */
	fini_result_parts_storage(&parts_storage, true);

	/* Close parent's indices */
	ExecCloseIndices(parent_result_rel);

	FreeExecutorState(estate);

	return processed;
}

/*
 * COPY FROM does not support FDWs, emit ERROR.
 */
static void
prepare_rri_for_copy(EState *estate,
					 ResultRelInfoHolder *rri_holder,
					 const ResultPartsStorage *rps_storage,
					 void *arg)
{
	ResultRelInfo  *rri = rri_holder->result_rel_info;
	FdwRoutine	   *fdw_routine = rri->ri_FdwRoutine;

	if (fdw_routine != NULL)
		elog(ERROR, "cannot copy to foreign partition \"%s\"",
			 get_rel_name(RelationGetRelid(rri->ri_RelationDesc)));
}

/*
 * Rename RANGE\HASH check constraint of a partition on table rename event.
 */
void
PathmanRenameConstraint(Oid partition_relid,				/* cached partition Oid */
						AttrNumber partitioned_col,			/* partitioned column */
						const RenameStmt *part_rename_stmt)	/* partition rename stmt */
{
	char		   *old_constraint_name,
				   *new_constraint_name;
	RenameStmt		rename_stmt;

	/* Generate old constraint name */
	old_constraint_name =
			build_check_constraint_name_relid_internal(partition_relid,
													   partitioned_col);

	/* Generate new constraint name */
	new_constraint_name =
			build_check_constraint_name_relname_internal(part_rename_stmt->newname,
														 partitioned_col);

	/* Build check constraint RENAME statement */
	memset((void *) &rename_stmt, 0, sizeof(RenameStmt));
	NodeSetTag(&rename_stmt, T_RenameStmt);
	rename_stmt.renameType = OBJECT_TABCONSTRAINT;
	rename_stmt.relation = part_rename_stmt->relation;
	rename_stmt.subname = old_constraint_name;
	rename_stmt.newname = new_constraint_name;
	rename_stmt.missing_ok = false;

	RenameConstraint(&rename_stmt);
}
