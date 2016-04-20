#include "utils.h"
#include "nodes/nodeFuncs.h"
#include "parser/parse_param.h"
#include "utils/builtins.h"


static bool clause_contains_extern_params_walker(Node *node, void *context);

bool
clause_contains_extern_params(Node *clause)
{
	return expression_tree_walker(clause,
								  clause_contains_extern_params_walker,
								  NULL);
}

static bool
clause_contains_extern_params_walker(Node *node, void *context)
{
	if (node == NULL)
		return false;
	if (IsA(node, Param))
	{
		Param	   *param = (Param *) node;

		if (param->paramkind == PARAM_EXTERN)
			return true;
		return false;
	}
	return expression_tree_walker(node,
								  clause_contains_extern_params_walker,
								  context);
}
