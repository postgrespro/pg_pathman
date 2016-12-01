/* ------------------------------------------------------------------------
 *
 * utility_stmt_hooking.h
 *		Override COPY TO/FROM and ALTER TABLE ... RENAME statements
 *		for partitioned tables
 *
 * Copyright (c) 2016, Postgres Professional
 *
 * ------------------------------------------------------------------------
 */

#ifndef COPY_STMT_HOOKING_H
#define COPY_STMT_HOOKING_H


#include "postgres.h"
#include "commands/copy.h"
#include "nodes/nodes.h"


/* Various traits */
bool is_pathman_related_copy(Node *parsetree);
bool is_pathman_related_table_rename(Node *parsetree,
									 Oid *partition_relid_out,
									 AttrNumber *partitioned_col_out);

/* Statement handlers */
void PathmanDoCopy(const CopyStmt *stmt, const char *queryString, uint64 *processed);
void PathmanRenameConstraint(Oid partition_relid,
							 AttrNumber partitioned_col,
							 const RenameStmt *partition_rename_stmt);


#endif /* COPY_STMT_HOOKING_H */
