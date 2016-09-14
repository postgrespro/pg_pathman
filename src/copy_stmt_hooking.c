#include "copy_stmt_hooking.h"
#include "relation_info.h"

#include "catalog/namespace.h"
#include "commands/copy.h"


/*
 * Is pg_pathman supposed to handle this COPY stmt?
 */
bool
is_pathman_related_copy(Node *parsetree)
{
	CopyStmt   *copy_stmt = (CopyStmt *) parsetree;
	Oid			partitioned_table;

	/* Check that it's a CopyStmt */
	if (!IsA(parsetree, CopyStmt))
		return false;

	/* Also check that stmt->relation exists */
	if (!copy_stmt->relation)
		return false;

	/* TODO: select appropriate lock for COPY */
	partitioned_table = RangeVarGetRelid(copy_stmt->relation, NoLock, false);

	/* Check that relation is partitioned */
	if (get_pathman_relation_info(partitioned_table))
		return true;

	return false;
}
