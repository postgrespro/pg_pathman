/* ------------------------------------------------------------------------
 *
 * utils.c
 *		definitions of various support functions
 *
 * Copyright (c) 2016, Postgres Professional
 *
 * ------------------------------------------------------------------------
 */
#include "utils.h"
#include "nodes/nodeFuncs.h"
#include "parser/parse_param.h"
#include "utils/builtins.h"
#include "rewrite/rewriteManip.h"


static bool clause_contains_params_walker(Node *node, void *context);

bool
clause_contains_params(Node *clause)
{
	return expression_tree_walker(clause,
								  clause_contains_params_walker,
								  NULL);
}

static bool
clause_contains_params_walker(Node *node, void *context)
{
	if (node == NULL)
		return false;
	if (IsA(node, Param))
		return true;
	return expression_tree_walker(node,
								  clause_contains_params_walker,
								  context);
}

static char *
bms_print(Bitmapset *bms)
{
  StringInfoData str;
  int x;

  initStringInfo(&str);
  x = -1;
  while ((x = bms_next_member(bms, x)) >= 0)
    appendStringInfo(&str, " %d", x);

  return str.data;
}
