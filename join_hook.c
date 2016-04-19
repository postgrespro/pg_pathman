#include "postgres.h"
#include "optimizer/cost.h"
#include "optimizer/restrictinfo.h"
#include "join_hook.h"
#include "pickyappend.h"


set_join_pathlist_hook_type set_join_pathlist_next = NULL;

void
pathman_join_pathlist_hook(PlannerInfo *root,
						   RelOptInfo *joinrel,
						   RelOptInfo *outerrel,
						   RelOptInfo *innerrel,
						   JoinType jointype,
						   JoinPathExtraData *extra)
{
	JoinCostWorkspace	workspace;
	Path			   *outer,
					   *inner;
	Relids				inner_required;
	RangeTblEntry	   *inner_entry = root->simple_rte_array[innerrel->relid];
	PartRelationInfo   *inner_prel;
	NestPath		   *nest_path;
	List			   *pathkeys = NIL;
	List			   *joinrestrictclauses = extra->restrictlist;
	List			   *joinclauses,
					   *otherclauses;
	ListCell		   *lc;

	if (set_join_pathlist_next)
		set_join_pathlist_next(root, joinrel, outerrel,
							   innerrel, jointype, extra);

	if (jointype == JOIN_UNIQUE_OUTER ||
		jointype == JOIN_UNIQUE_INNER)
	{
		jointype = JOIN_INNER;
	}

	if (jointype == JOIN_FULL || !pg_pathman_enable_pickyappend)
		return;

	if (innerrel->reloptkind != RELOPT_BASEREL ||
		!inner_entry->inh ||
		!(inner_prel = get_pathman_relation_info(inner_entry->relid, NULL)))
	{
		return; /* Obviously not our case */
	}

	/* Extract join clauses which will separate partitions */
	if (IS_OUTER_JOIN(extra->sjinfo->jointype))
	{
		extract_actual_join_clauses(joinrestrictclauses,
									&joinclauses, &otherclauses);
	}
	else
	{
		/* We can treat all clauses alike for an inner join */
		joinclauses = extract_actual_clauses(joinrestrictclauses, false);
		otherclauses = NIL;
	}

	foreach (lc, innerrel->pathlist)
	{
		AppendPath *cur_inner_path = (AppendPath *) lfirst(lc);

		if (!IsA(cur_inner_path, AppendPath))
			continue;

		outer = outerrel->cheapest_total_path;

		inner_required = bms_union(PATH_REQ_OUTER((Path *) cur_inner_path),
								   bms_make_singleton(outerrel->relid));

		inner = create_pickyappend_path(root, cur_inner_path,
										get_appendrel_parampathinfo(innerrel,
																	inner_required),
										joinclauses);

		initial_cost_nestloop(root, &workspace, jointype,
							  outer, inner,
							  extra->sjinfo, &extra->semifactors);

		pathkeys = build_join_pathkeys(root, joinrel, jointype, outer->pathkeys);

		nest_path = create_nestloop_path(root, joinrel, jointype, &workspace,
										 extra->sjinfo, &extra->semifactors,
										 outer, inner, extra->restrictlist,
										 pathkeys,
										 calc_nestloop_required_outer(outer, inner));

		add_path(joinrel, (Path *) nest_path);
	}
}
