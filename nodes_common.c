#include "postgres.h"
#include "optimizer/paths.h"
#include "nodes_common.h"


/* Compare plans by 'original_order' */
static int
cmp_child_scan_common_by_orig_order(const void *ap,
									const void *bp)
{
	ChildScanCommon a = *(ChildScanCommon *) ap;
	ChildScanCommon b = *(ChildScanCommon *) bp;

	if (a->original_order > b->original_order)
		return 1;
	else if (a->original_order < b->original_order)
		return -1;
	else
		return 0;
}

void
explain_common(CustomScanState *node, HTAB* children_table, ExplainState *es)
{
	/* Construct excess PlanStates */
	if (!es->analyze)
	{
		int					allocated = 10;
		int					used = 0;
		ChildScanCommon	   *custom_ps = palloc(allocated * sizeof(ChildScanCommon));
		ChildScanCommon		child;
		HASH_SEQ_STATUS		seqstat;
		int					i;

		/* There can't be any nodes since we're not scanning anything */
		Assert(!node->custom_ps);

		hash_seq_init(&seqstat, children_table);

		while ((child = (ChildScanCommon) hash_seq_search(&seqstat)))
		{
			if (allocated <= used)
			{
				allocated *= 2;
				custom_ps = repalloc(custom_ps, allocated * sizeof(ChildScanCommon));
			}

			custom_ps[used++] = child;
		}

		/*
		 * We have to restore the original plan order
		 * which has been lost within the hash table
		 */
		qsort(custom_ps, used, sizeof(ChildScanCommon),
			  cmp_child_scan_common_by_orig_order);

		/*
		 * These PlanStates will be used by EXPLAIN,
		 * arrangeappend_end will destroy them eventually
		 */
		for (i = 0; i < used; i++)
			node->custom_ps = lappend(node->custom_ps,
									  ExecInitNode(custom_ps[i]->content.plan,
												   node->ss.ps.state,
												   0));
	}
}
