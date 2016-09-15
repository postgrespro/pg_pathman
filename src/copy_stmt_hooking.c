/* ------------------------------------------------------------------------
 *
 * copy_stmt_hooking.c
 *		Override COPY TO/FROM statement for partitioned tables
 *
 * Copyright (c) 2016, Postgres Professional
 * Portions Copyright (c) 1996-2015, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * ------------------------------------------------------------------------
 */

#include "copy_stmt_hooking.h"
#include "init.h"
#include "relation_info.h"

#include "access/htup_details.h"
#include "access/sysattr.h"
#include "access/xact.h"
#include "catalog/namespace.h"
#include "catalog/pg_attribute.h"
#include "commands/copy.h"
#include "commands/trigger.h"
#include "executor/executor.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/rls.h"

#include "libpq/libpq.h"


/*
 * Is pg_pathman supposed to handle this COPY stmt?
 */
bool
is_pathman_related_copy(Node *parsetree)
{
	CopyStmt   *copy_stmt = (CopyStmt *) parsetree;
	Oid			partitioned_table;

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

	/* TODO: select appropriate lock for COPY */
	partitioned_table = RangeVarGetRelid(copy_stmt->relation,
										 (copy_stmt->is_from ?
											  RowExclusiveLock :
											  AccessShareLock),
										 false);

	/* Check that relation is partitioned */
	if (get_pathman_relation_info(partitioned_table))
	{
		elog(DEBUG1, "Overriding default behavior for COPY (%u)", partitioned_table);
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
			ColumnRef  *cr;
			ResTarget  *target;
			RangeVar   *from;

			if (is_from)
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				  errmsg("COPY FROM not supported with row-level security"),
						 errhint("Use INSERT statements instead.")));

			/* Build target list */
			cr = makeNode(ColumnRef);

			if (!stmt->attlist)
				cr->fields = list_make1(makeNode(A_Star));
			else
				cr->fields = stmt->attlist;

			cr->location = 1;

			target = makeNode(ResTarget);
			target->name = NULL;
			target->indirection = NIL;
			target->val = (Node *) cr;
			target->location = 1;

			/*
			 * Build RangeVar for from clause, fully qualified based on the
			 * relation which we have opened and locked.
			 */
			from = makeRangeVar(get_namespace_name(RelationGetNamespace(rel)),
								RelationGetRelationName(rel), -1);

			/* Build query */
			select = makeNode(SelectStmt);
			select->targetList = list_make1(target);
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
	else
	{
		Assert(stmt->query);

		query = stmt->query;
		rel = NULL;
	}

	/* COPY ... FROM ... */
	if (is_from)
	{
		/* There should be relation */
		Assert(rel);

		/* check read-only transaction and parallel mode */
		if (XactReadOnly && rel && !rel->rd_islocaltemp)
			PreventCommandIfReadOnly("PATHMAN COPY FROM");
		PreventCommandIfParallelMode("PATHMAN COPY FROM");

		cstate = BeginCopyFrom(rel, stmt->filename, stmt->is_program,
							   stmt->attlist, stmt->options);
		/* TODO: copy files to DB */
		heap_close(rel, NoLock);
		*processed = 0;
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
}
