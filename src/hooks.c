/* ------------------------------------------------------------------------
 *
 * hooks.c
 *		definitions of rel_pathlist and join_pathlist hooks
 *
 * Copyright (c) 2016, Postgres Professional
 *
 * ------------------------------------------------------------------------
 */

#include "copy_stmt_hooking.h"
#include "hooks.h"
#include "init.h"
#include "partition_filter.h"
#include "pg_compat.h"
#include "runtimeappend.h"
#include "runtime_merge_append.h"
#include "utils.h"
#include "xact_handling.h"

#include "access/transam.h"
#include "miscadmin.h"
#include "optimizer/cost.h"
#include "optimizer/restrictinfo.h"
#include "utils/typcache.h"


set_join_pathlist_hook_type		set_join_pathlist_next = NULL;
set_rel_pathlist_hook_type		set_rel_pathlist_hook_next = NULL;
planner_hook_type				planner_hook_next = NULL;
post_parse_analyze_hook_type	post_parse_analyze_hook_next = NULL;
shmem_startup_hook_type			shmem_startup_hook_next = NULL;
ProcessUtility_hook_type		process_utility_hook_next = NULL;


/* Take care of joins */
void
pathman_join_pathlist_hook(PlannerInfo *root,
						   RelOptInfo *joinrel,
						   RelOptInfo *outerrel,
						   RelOptInfo *innerrel,
						   JoinType jointype,
						   JoinPathExtraData *extra)
{
	JoinCostWorkspace		workspace;
	RangeTblEntry		   *inner_rte = root->simple_rte_array[innerrel->relid];
	const PartRelationInfo *inner_prel;
	List				   *pathkeys = NIL,
						   *joinclauses,
						   *otherclauses;
	ListCell			   *lc;
	WalkerContext			context;
	double					paramsel;
	bool					innerrel_rinfo_contains_part_attr;

	/* Call hooks set by other extensions */
	if (set_join_pathlist_next)
		set_join_pathlist_next(root, joinrel, outerrel,
							   innerrel, jointype, extra);

	/* Check that both pg_pathman & RuntimeAppend nodes are enabled */
	if (!IsPathmanReady() || !pg_pathman_enable_runtimeappend)
		return;

	if (jointype == JOIN_FULL)
		return; /* handling full joins is meaningless */

	/* Check that innerrel is a BASEREL with inheritors & PartRelationInfo */
	if (innerrel->reloptkind != RELOPT_BASEREL || !inner_rte->inh ||
		!(inner_prel = get_pathman_relation_info(inner_rte->relid)))
	{
		return; /* Obviously not our case */
	}

	/*
	 * These codes are used internally in the planner, but are not supported
	 * by the executor (nor, indeed, by most of the planner).
	 */
	if (jointype == JOIN_UNIQUE_OUTER || jointype == JOIN_UNIQUE_INNER)
		jointype = JOIN_INNER; /* replace with a proper value */

	/* Extract join clauses which will separate partitions */
	if (IS_OUTER_JOIN(extra->sjinfo->jointype))
	{
		extract_actual_join_clauses(extra->restrictlist,
									&joinclauses, &otherclauses);
	}
	else
	{
		/* We can treat all clauses alike for an inner join */
		joinclauses = extract_actual_clauses(extra->restrictlist, false);
		otherclauses = NIL;
	}

	paramsel = 1.0;
	foreach (lc, joinclauses)
	{
		WrapperNode *wrap;

		InitWalkerContext(&context, inner_prel, NULL, false);

		wrap = walk_expr_tree((Expr *) lfirst(lc), &context);
		paramsel *= wrap->paramsel;
	}

	/* Check that innerrel's RestrictInfos contain partitioned column */
	innerrel_rinfo_contains_part_attr =
		get_partitioned_attr_clauses(innerrel->baserestrictinfo,
									 inner_prel, innerrel->relid) != NULL;

	foreach (lc, innerrel->pathlist)
	{
		AppendPath	   *cur_inner_path = (AppendPath *) lfirst(lc);
		Path		   *outer,
					   *inner;
		NestPath	   *nest_path;		/* NestLoop we're creating */
		ParamPathInfo  *ppi;			/* parameterization info */
		Relids			inner_required;	/* required paremeterization relids */
		List		   *filtered_joinclauses = NIL;
		ListCell	   *rinfo_lc;

		if (!IsA(cur_inner_path, AppendPath))
			continue;

		/* Select cheapest path for outerrel */
		outer = outerrel->cheapest_total_path;

		/* Make innerrel path depend on outerrel's column */
		inner_required = bms_union(PATH_REQ_OUTER((Path *) cur_inner_path),
								   bms_make_singleton(outerrel->relid));

		/* Get the ParamPathInfo for a parameterized path */
		ppi = get_baserel_parampathinfo(root, innerrel, inner_required);

		/*
		 * Skip if neither rel->baserestrictinfo nor
		 * ppi->ppi_clauses reference partition attribute
		 */
		if (!(innerrel_rinfo_contains_part_attr ||
			  (ppi && get_partitioned_attr_clauses(ppi->ppi_clauses,
												   inner_prel,
												   innerrel->relid))))
			continue;

		inner = create_runtimeappend_path(root, cur_inner_path, ppi, paramsel);

		initial_cost_nestloop(root, &workspace, jointype,
							  outer, inner, /* built paths */
							  extra->sjinfo, &extra->semifactors);

		pathkeys = build_join_pathkeys(root, joinrel, jointype, outer->pathkeys);

		nest_path = create_nestloop_path(root, joinrel, jointype, &workspace,
										 extra->sjinfo, &extra->semifactors,
										 outer, inner, extra->restrictlist,
										 pathkeys,
										 calc_nestloop_required_outer(outer, inner));

		/* Discard all clauses that are to be evaluated by 'inner' */
		foreach (rinfo_lc, extra->restrictlist)
		{
			RestrictInfo *rinfo = (RestrictInfo *) lfirst(rinfo_lc);

			Assert(IsA(rinfo, RestrictInfo));
			if (!join_clause_is_movable_to(rinfo, inner->parent))
				filtered_joinclauses = lappend(filtered_joinclauses, rinfo);
		}

		/*
		 * Override 'rows' value produced by standard estimator.
		 * Currently we use get_parameterized_joinrel_size() since
		 * it works just fine, but this might change some day.
		 */
		nest_path->path.rows = get_parameterized_joinrel_size_compat(root,
																	 joinrel,
																	 outer,
																	 inner,
																	 extra->sjinfo,
																	 filtered_joinclauses);

		/* Finally we can add the new NestLoop path */
		add_path(joinrel, (Path *) nest_path);
	}
}

/* Cope with simple relations */
void
pathman_rel_pathlist_hook(PlannerInfo *root,
						  RelOptInfo *rel,
						  Index rti,
						  RangeTblEntry *rte)
{
	const PartRelationInfo *prel;
	RangeTblEntry		  **new_rte_array;
	RelOptInfo			  **new_rel_array;
	int						len;

	/* Invoke original hook if needed */
	if (set_rel_pathlist_hook_next != NULL)
		set_rel_pathlist_hook_next(root, rel, rti, rte);

	if (!IsPathmanReady())
		return; /* pg_pathman is not ready */

	/* This works only for SELECT queries (at least for now) */
	if (root->parse->commandType != CMD_SELECT ||
		!list_member_oid(inheritance_enabled_relids, rte->relid))
		return;

	/* Proceed iff relation 'rel' is partitioned */
	if ((prel = get_pathman_relation_info(rte->relid)) != NULL)
	{
		ListCell	   *lc;
		Oid			   *children;
		List		   *ranges,
					   *wrappers;
		PathKey		   *pathkeyAsc = NULL,
					   *pathkeyDesc = NULL;
		double			paramsel = 1.0;
		WalkerContext	context;
		int				i;
		bool			rel_rinfo_contains_part_attr = false;

		if (prel->parttype == PT_RANGE)
		{
			/*
			 * Get pathkeys for ascending and descending sort by partition
			 * column
			 */
			List		   *pathkeys;
			Var			   *var;
			Oid				vartypeid,
							varcollid;
			int32			type_mod;
			TypeCacheEntry *tce;

			/* Make Var from patition column */
			get_rte_attribute_type(rte, prel->attnum,
								   &vartypeid, &type_mod, &varcollid);
			var = makeVar(rti, prel->attnum, vartypeid, type_mod, varcollid, 0);
			var->location = -1;

			/* Determine operator type */
			tce = lookup_type_cache(var->vartype, TYPECACHE_LT_OPR | TYPECACHE_GT_OPR);

			/* Make pathkeys */
			pathkeys = build_expression_pathkey(root, (Expr *)var, NULL,
												tce->lt_opr, NULL, false);
			if (pathkeys)
				pathkeyAsc = (PathKey *) linitial(pathkeys);
			pathkeys = build_expression_pathkey(root, (Expr *)var, NULL,
												tce->gt_opr, NULL, false);
			if (pathkeys)
				pathkeyDesc = (PathKey *) linitial(pathkeys);
		}

		rte->inh = true; /* we must restore 'inh' flag! */

		children = PrelGetChildrenArray(prel);
		ranges = list_make1_irange(make_irange(0, PrelLastChild(prel), false));

		/* Make wrappers over restrictions and collect final rangeset */
		InitWalkerContext(&context, prel, NULL, false);
		wrappers = NIL;
		foreach(lc, rel->baserestrictinfo)
		{
			WrapperNode	   *wrap;
			RestrictInfo   *rinfo = (RestrictInfo *) lfirst(lc);

			wrap = walk_expr_tree(rinfo->clause, &context);

			paramsel *= wrap->paramsel;
			wrappers = lappend(wrappers, wrap);
			ranges = irange_list_intersect(ranges, wrap->rangeset);
		}

		/*
		 * Expand simple_rte_array and simple_rel_array
		 */
		len = irange_list_length(ranges);
		if (prel->enable_parent)
			len++;

		if (len > 0)
		{
			/* Expand simple_rel_array and simple_rte_array */
			new_rel_array = (RelOptInfo **)
				palloc0((root->simple_rel_array_size + len) * sizeof(RelOptInfo *));

			/* simple_rte_array is an array equivalent of the rtable list */
			new_rte_array = (RangeTblEntry **)
				palloc0((root->simple_rel_array_size + len) * sizeof(RangeTblEntry *));

			/* Copy relations to the new arrays */
			for (i = 0; i < root->simple_rel_array_size; i++)
			{
				new_rel_array[i] = root->simple_rel_array[i];
				new_rte_array[i] = root->simple_rte_array[i];
			}

			/* Free old arrays */
			pfree(root->simple_rel_array);
			pfree(root->simple_rte_array);

			root->simple_rel_array_size += len;
			root->simple_rel_array = new_rel_array;
			root->simple_rte_array = new_rte_array;
		}

		/* Add parent if needed */
		if (prel->enable_parent)
			append_child_relation(root, rel, rti, rte, 0, rte->relid, NULL);

		/*
		 * Iterate all indexes in rangeset and append corresponding child
		 * relations.
		 */
		foreach(lc, ranges)
		{
			IndexRange	irange = lfirst_irange(lc);

			for (i = irange.ir_lower; i <= irange.ir_upper; i++)
				append_child_relation(root, rel, rti, rte, i, children[i],
									  wrappers);
		}

		/* Clear old path list */
		list_free(rel->pathlist);

		rel->pathlist = NIL;
		set_append_rel_pathlist(root, rel, rti, rte, pathkeyAsc, pathkeyDesc);
		set_append_rel_size_compat(root, rel, rti, rte);

		/* No need to go further (both nodes are disabled), return */
		if (!(pg_pathman_enable_runtimeappend ||
			  pg_pathman_enable_runtime_merge_append))
			return;

		/* Runtime[Merge]Append is pointless if there are no params in clauses */
		if (!clause_contains_params((Node *) get_actual_clauses(rel->baserestrictinfo)))
			return;

		/* Check that rel's RestrictInfo contains partitioned column */
		rel_rinfo_contains_part_attr =
			get_partitioned_attr_clauses(rel->baserestrictinfo,
										 prel, rel->relid) != NULL;

		foreach (lc, rel->pathlist)
		{
			AppendPath	   *cur_path = (AppendPath *) lfirst(lc);
			Relids			inner_required = PATH_REQ_OUTER((Path *) cur_path);
			ParamPathInfo  *ppi = get_appendrel_parampathinfo(rel, inner_required);
			Path		   *inner_path = NULL;

			/* Skip if rel contains some join-related stuff or path type mismatched */
			if (!(IsA(cur_path, AppendPath) || IsA(cur_path, MergeAppendPath)) ||
				rel->has_eclass_joins || rel->joininfo)
			{
				continue;
			}

			/*
			 * Skip if neither rel->baserestrictinfo nor
			 * ppi->ppi_clauses reference partition attribute
			 */
			if (!(rel_rinfo_contains_part_attr ||
				  (ppi && get_partitioned_attr_clauses(ppi->ppi_clauses,
													   prel, rel->relid))))
				continue;

			if (IsA(cur_path, AppendPath) && pg_pathman_enable_runtimeappend)
				inner_path = create_runtimeappend_path(root, cur_path,
													   ppi, paramsel);
			else if (IsA(cur_path, MergeAppendPath) &&
					 pg_pathman_enable_runtime_merge_append)
			{
				/* Check struct layout compatibility */
				if (offsetof(AppendPath, subpaths) !=
						offsetof(MergeAppendPath, subpaths))
					elog(FATAL, "Struct layouts of AppendPath and "
								"MergeAppendPath differ");

				inner_path = create_runtimemergeappend_path(root, cur_path,
															ppi, paramsel);
			}

			if (inner_path)
				add_path(rel, inner_path);
		}
	}
}

/*
 * Intercept 'pg_pathman.enable' GUC assignments.
 */
void
pg_pathman_enable_assign_hook(bool newval, void *extra)
{
	elog(DEBUG2, "pg_pathman_enable_assign_hook() [newval = %s] triggered",
		  newval ? "true" : "false");

	/* Return quickly if nothing has changed */
	if (newval == (pg_pathman_init_state.pg_pathman_enable &&
				   pg_pathman_init_state.auto_partition &&
				   pg_pathman_init_state.override_copy &&
				   pg_pathman_enable_runtimeappend &&
				   pg_pathman_enable_runtime_merge_append &&
				   pg_pathman_enable_partition_filter))
		return;

	pg_pathman_init_state.auto_partition = newval;
	pg_pathman_init_state.override_copy = newval;
	pg_pathman_enable_runtime_merge_append = newval;
	pg_pathman_enable_runtimeappend = newval;
	pg_pathman_enable_partition_filter = newval;

	elog(NOTICE,
		 "RuntimeAppend, RuntimeMergeAppend and PartitionFilter nodes "
		 "and some other options have been %s",
		 newval ? "enabled" : "disabled");
}

/*
 * Planner hook. It disables inheritance for tables that have been partitioned
 * by pathman to prevent standart PostgreSQL partitioning mechanism from
 * handling that tables.
 */
PlannedStmt *
pathman_planner_hook(Query *parse, int cursorOptions, ParamListInfo boundParams)
{
#define ExecuteForPlanTree(planned_stmt, proc) \
	do { \
		ListCell *lc; \
		proc((planned_stmt)->rtable, (planned_stmt)->planTree); \
		foreach (lc, (planned_stmt)->subplans) \
			proc((planned_stmt)->rtable, (Plan *) lfirst(lc)); \
	} while (0)

	PlannedStmt	  *result;

	/* FIXME: fix these commands (traverse whole query tree) */
	if (IsPathmanReady())
	{
		switch(parse->commandType)
		{
			case CMD_SELECT:
				disable_inheritance(parse);
				rowmark_add_tableoids(parse); /* add attributes for rowmarks */
				break;

			case CMD_UPDATE:
			case CMD_DELETE:
				disable_inheritance_cte(parse);
				disable_inheritance_subselect(parse);
				handle_modification_query(parse);
				break;

			default:
				break;
		}
	}

	/* Invoke original hook if needed */
	if (planner_hook_next)
		result = planner_hook_next(parse, cursorOptions, boundParams);
	else
		result = standard_planner(parse, cursorOptions, boundParams);

	if (IsPathmanReady())
	{
		/* Give rowmark-related attributes correct names */
		ExecuteForPlanTree(result, postprocess_lock_rows);

		/* Add PartitionFilter node for INSERT queries */
		ExecuteForPlanTree(result, add_partition_filters);
	}

	list_free(inheritance_disabled_relids);
	list_free(inheritance_enabled_relids);
	inheritance_disabled_relids = NIL;
	inheritance_enabled_relids = NIL;

	return result;
}

/*
 * Post parse analysis hook. It makes sure the config is loaded before executing
 * any statement, including utility commands
 */
void
pathman_post_parse_analysis_hook(ParseState *pstate, Query *query)
{
	/* Invoke original hook if needed */
	if (post_parse_analyze_hook_next)
		post_parse_analyze_hook_next(pstate, query);

	 /* We shouldn't do anything on BEGIN or SET ISOLATION LEVEL stmts */
	if (query->commandType == CMD_UTILITY &&
			(xact_is_transaction_stmt(query->utilityStmt) ||
			 xact_is_set_transaction_stmt(query->utilityStmt)))
	{
		return;
	}

	/* Finish delayed invalidation jobs */
	if (IsPathmanReady())
		finish_delayed_invalidation();

	/* Load config if pg_pathman exists & it's still necessary */
	if (IsPathmanEnabled() &&
		!IsPathmanInitialized() &&
		/* Now evaluate the most expensive clause */
		get_pathman_schema() != InvalidOid)
	{
		load_config(); /* perform main cache initialization */
	}

	inheritance_disabled_relids = NIL;
	inheritance_enabled_relids = NIL;
}

/*
 * Initialize dsm_config & shmem_config.
 */
void
pathman_shmem_startup_hook(void)
{
	/* Invoke original hook if needed */
	if (shmem_startup_hook_next != NULL)
		shmem_startup_hook_next();

	/* Allocate shared memory objects */
	LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);
	init_shmem_config();
	LWLockRelease(AddinShmemInitLock);
}

/*
 * Invalidate PartRelationInfo cache entry if needed.
 */
void
pathman_relcache_hook(Datum arg, Oid relid)
{
	PartParentSearch	search;
	Oid					partitioned_table;

	if (!IsPathmanReady())
		return;

	/* We shouldn't even consider special OIDs */
	if (relid < FirstNormalObjectId)
		return;

	/* Invalidation event for PATHMAN_CONFIG table (probably DROP) */
	if (relid == get_pathman_config_relid())
		delay_pathman_shutdown();

	/* Invalidate PartParentInfo cache if needed */
	partitioned_table = forget_parent_of_partition(relid, &search);

	switch (search)
	{
		/* It is (or was) a valid partition */
		case PPS_ENTRY_PART_PARENT:
		case PPS_ENTRY_PARENT:
			{
				elog(DEBUG2, "Invalidation message for partition %u [%u]",
					 relid, MyProcPid);

				delay_invalidation_parent_rel(partitioned_table);
			}
			break;

		/* Both syscache and pathman's cache say it isn't a partition */
		case PPS_ENTRY_NOT_FOUND:
			{
				if (partitioned_table != InvalidOid)
					delay_invalidation_parent_rel(partitioned_table);
#ifdef NOT_USED
				elog(DEBUG2, "Invalidation message for relation %u [%u]",
					 relid, MyProcPid);
#endif
			}
			break;

		/* We can't say anything (state is not transactional) */
		case PPS_NOT_SURE:
			{
				elog(DEBUG2, "Invalidation message for vague relation %u [%u]",
					 relid, MyProcPid);

				delay_invalidation_vague_rel(relid);
			}
			break;

		default:
			elog(ERROR, "Not implemented yet (%s)",
				 CppAsString(pathman_relcache_hook));
			break;
	}
}

/*
 * Utility function invoker hook.
 */
void
pathman_process_utility_hook(Node *parsetree,
							 const char *queryString,
							 ProcessUtilityContext context,
							 ParamListInfo params,
							 DestReceiver *dest,
							 char *completionTag)
{
	/* Call hooks set by other extensions */
	if (process_utility_hook_next)
		process_utility_hook_next(parsetree, queryString,
								  context, params,
								  dest, completionTag);

	/* Override standard COPY statement if needed */
	if (IsPathmanReady() && is_pathman_related_copy(parsetree))
	{
		uint64	processed;

		PathmanDoCopy((CopyStmt *) parsetree, queryString, &processed);
		if (completionTag)
			snprintf(completionTag, COMPLETION_TAG_BUFSIZE,
					 "PATHMAN COPY " UINT64_FORMAT, processed);

		return; /* don't call standard_ProcessUtility() */
	}

	/* Call internal implementation */
	standard_ProcessUtility(parsetree, queryString,
							context, params,
							dest, completionTag);
}
