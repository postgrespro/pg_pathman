#ifndef COPY_STMT_HOOKING_H
#define COPY_STMT_HOOKING_H


#include "postgres.h"
#include "commands/copy.h"
#include "nodes/nodes.h"


bool is_pathman_related_copy(Node *parsetree);

#endif
