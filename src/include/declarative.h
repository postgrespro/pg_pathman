#ifndef DECLARATIVE_H
#define DECLARATIVE_H

#include "postgres.h"
#include "nodes/nodes.h"
#include "nodes/parsenodes.h"

typedef enum DeclarativeCommandType {
	DP_ATTACH,	/* ALTER TABLE .. ATTACH PARTITION */
	DP_DETACH	/* ALTER TABLE .. DETACH PARTITION */
} DeclarativeCommandType;

void modify_declative_partitioning_query(Query *query);
bool is_pathman_related_partitioning_cmd(Node *parsetree);

/* actual actions */
void handle_attach_partition(AlterTableStmt *stmt, AlterTableCmd *cmd);
void handle_detach_partition(AlterTableStmt *stmt, AlterTableCmd *cmd);

#endif /* DECLARATIVE_H */
