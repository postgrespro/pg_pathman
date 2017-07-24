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


#include "relation_info.h"

#include "postgres.h"
#include "commands/copy.h"
#include "nodes/nodes.h"


/* Various traits */
bool is_pathman_related_copy(Node *parsetree);
bool is_pathman_related_table_rename(Node *parsetree,
									 Oid *partition_relid_out);
bool is_pathman_related_alter_column_type(Node *parsetree,
										  Oid *parent_relid_out,
										  AttrNumber *attr_number,
										  PartType *part_type_out);

/* Statement handlers */
void PathmanDoCopy(const CopyStmt *stmt, const char *queryString,
				   int stmt_location, int stmt_len, uint64 *processed);
void PathmanRenameConstraint(Oid partition_relid,
							 const RenameStmt *partition_rename_stmt);


#endif /* COPY_STMT_HOOKING_H */
