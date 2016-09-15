/* ------------------------------------------------------------------------
 *
 * copy_stmt_hooking.h
 *		Transaction-specific locks and other functions
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


bool is_pathman_related_copy(Node *parsetree);
void PathmanDoCopy(const CopyStmt *stmt, const char *queryString, uint64 *processed);

#endif
