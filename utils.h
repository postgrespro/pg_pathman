#ifndef UTILS_H
#define UTILS_H

#include "postgres.h"
#include "nodes/nodeFuncs.h"

bool clause_contains_extern_params(Node *clause);

#endif
