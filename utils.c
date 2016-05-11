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

/*
 * Sorts reltargetlist by Var's varattno (physical order) since
 * we can't use static build_path_tlist() for our custom nodes.
 *
 * See create_scan_plan & use_physical_tlist for more details.
 */
List *
sort_rel_tlist(List *tlist)
{
	int			i;
	int			plain_tlist_size = list_length(tlist);
	Var		  **plain_tlist = palloc(plain_tlist_size * sizeof(Var *));
	ListCell   *tlist_cell;
	List	   *result = NIL;

	i = 0;
	foreach (tlist_cell, tlist)
		plain_tlist[i++] = lfirst(tlist_cell);

	qsort(plain_tlist, plain_tlist_size, sizeof(Var *), cmp_tlist_vars);

	for (i = 0; i < plain_tlist_size; i++)
		result = lappend(result, plain_tlist[i]);

	return result;
}

/* Compare Vars by varattno */
int
cmp_tlist_vars(const void *a, const void *b)
{
	Var *v1 = *(Var **) a;
	Var *v2 = *(Var **) b;

	Assert(IsA(v1, Var) && IsA(v2, Var));

	if (v1->varattno > v2->varattno)
		return 1;
	else if (v1->varattno < v2->varattno)
		return -1;
	else
	{
		/* XXX: I really doubt this case is ok */
		Assert(v1->varattno != v2->varattno);
		return 0;
	}
}
