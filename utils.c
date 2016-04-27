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

static Node *
replace_child_var(Var *var, replace_rte_variables_context *context)
{
	ReplaceVarsContext *cxt = (ReplaceVarsContext *) context->callback_arg;
	Var				   *new_var;

	new_var = makeNode(Var);
	memcpy(new_var, var, sizeof(Var));

	/*
	 * Replace a partition's Var with a Var
	 * pointing to the RuntimeAppend's results
	 */
	new_var->varno = cxt->parent->relid;
	new_var->location = -1;
	new_var->varnoold = 0;

	return (Node *) new_var;
}

Node *
replace_child_vars_with_parent_var(Node *node, ReplaceVarsContext *context)
{
	return replace_rte_variables(node, context->child->relid, context->sublevels_up,
								 replace_child_var, (void *) context, NULL);
}
