#ifndef DECLARATIVE_H
#define DECLARATIVE_H

#include "postgres.h"
#include "nodes/nodes.h"
#include "nodes/parsenodes.h"

void modify_declarative_partitioning_query(Query *query);
bool is_pathman_related_partitioning_cmd(Node *parsetree, Oid *parent_relid);

/* actual actions */
void handle_attach_partition(Oid parent_relid, AlterTableCmd *cmd);
void handle_detach_partition(AlterTableCmd *cmd);
void handle_create_partition_of(Oid parent_relid, CreateStmt *stmt);

#endif /* DECLARATIVE_H */
