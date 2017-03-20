/* ------------------------------------------------------------------------
 *
 * expand_rte_hook.c
 *		Fix rowmarks etc using the 'expand_inherited_rtentry_hook'
 *		NOTE: this hook exists in PostgresPro
 *
 * Copyright (c) 2017, Postgres Professional
 *
 * ------------------------------------------------------------------------
 */

#include "compat/expand_rte_hook.h"
#include "relation_info.h"
#include "init.h"

#include "postgres.h"
#include "optimizer/prep.h"


#ifdef NATIVE_EXPAND_RTE_HOOK

static expand_inherited_rtentry_hook_type expand_inherited_rtentry_hook_next = NULL;

static void pathman_expand_inherited_rtentry_hook(PlannerInfo *root,
												  RangeTblEntry *rte,
												  Index rti);


/* Initialize 'expand_inherited_rtentry_hook' */
void
init_expand_rte_hook(void)
{
	expand_inherited_rtentry_hook_next = expand_inherited_rtentry_hook;
	expand_inherited_rtentry_hook = pathman_expand_inherited_rtentry_hook;
}


/* Fix parent's RowMark (makes 'rowmarks_fix' pointless) */
static void
pathman_expand_inherited_rtentry_hook(PlannerInfo *root,
									  RangeTblEntry *rte,
									  Index rti)
{
	PlanRowMark *oldrc;

	if (!IsPathmanReady())
		return;

	/* Check that table is partitioned by pg_pathman */
	if (!get_pathman_relation_info(rte->relid))
		return;

	/* HACK: fix rowmark for parent (for preprocess_targetlist() etc) */
	oldrc = get_plan_rowmark(root->rowMarks, rti);
	if (oldrc)
		oldrc->isParent = true;
}

#endif /* NATIVE_EXPAND_RTE_HOOK */
