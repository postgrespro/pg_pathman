/* ------------------------------------------------------------------------
 *
 * hooks.c
 *		definitions of rel_pathlist and join_pathlist hooks
 *
 * Copyright (c) 2016, Postgres Professional
 *
 * ------------------------------------------------------------------------
 */

#include "utility_stmt_hooking.h"
#include "hooks.h"
#include "init.h"
#include "partition_filter.h"
#include "pg_compat.h"
#include "planner_tree_modification.h"
#include "runtimeappend.h"
#include "runtime_merge_append.h"
#include "utils.h"
#include "xact_handling.h"

#include "access/transam.h"
#include "miscadmin.h"
#include "optimizer/cost.h"
#include "optimizer/restrictinfo.h"
#include "utils/typcache.h"
#include "utils/lsyscache.h"


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
	JoinType				saved_jointype = jointype;
	RangeTblEntry		   *inner_rte = root->simple_rte_array[innerrel->relid];
	const PartRelationInfo *inner_prel;
	List				   *pathkeys = NIL,
						   *joinclauses,
						   *otherclauses;
	ListCell			   *lc;
	WalkerContext			context;
	double					paramsel;

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

		InitWalkerContext(&context, innerrel->relid,
						  inner_prel, NULL, false);

		wrap = walk_expr_tree((Expr *) lfirst(lc), &context);
		paramsel *= wrap->paramsel;
	}

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
		if (saved_jointype == JOIN_UNIQUE_OUTER)
		{
			outer = (Path *) create_unique_path(root, outerrel,
												outer, extra->sjinfo);
			Assert(outer);
		}

		/* Make innerrel path depend on outerrel's column */
		inner_required = bms_union(PATH_REQ_OUTER((Path *) cur_inner_path),
								   bms_make_singleton(outerrel->relid));

		/* Get the ParamPathInfo for a parameterized path */
		ppi = get_baserel_parampathinfo(root, innerrel, inner_required);

		/* Skip ppi->ppi_clauses don't reference partition attribute */
		if (!(ppi && get_partitioned_attr_clauses(ppi->ppi_clauses,
												  inner_prel,
												  innerrel->relid)))
			continue;

		inner = create_runtimeappend_path(root, cur_inner_path, ppi, paramsel);
		if (saved_jointype == JOIN_UNIQUE_INNER)
			return; /* No way to do this with a parameterized inner path */

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

	/* Make sure that pg_pathman is ready */
	if (!IsPathmanReady())
		return;

	/* This works only for SELECTs or INSERTs on simple relations */
	if (rte->rtekind != RTE_RELATION ||
		rte->relkind != RELKIND_RELATION ||
			(root->parse->commandType != CMD_SELECT &&
			 root->parse->commandType != CMD_INSERT)) /* INSERT INTO ... SELECT ... */
		return;

	/* Skip if this table is not allowed to act as parent (see FROM ONLY) */
	if (PARENTHOOD_DISALLOWED == get_rel_parenthood_status(root->parse->queryId,
														   rte->relid))
		return;

	/* Proceed iff relation 'rel' is partitioned */
	if ((prel = get_pathman_relation_info(rte->relid)) != NULL)
	{
		Relation		parent_rel;				/* parent's relation (heap) */
		Oid			   *children;				/* selected children oids */
		List		   *ranges,					/* a list of IndexRanges */
					   *wrappers,				/* a list of WrapperNodes */
					   *rel_part_clauses = NIL;	/* clauses with part. column */
		PathKey		   *pathkeyAsc = NULL,
					   *pathkeyDesc = NULL;
		double			paramsel = 1.0;			/* default part selectivity */
		WalkerContext	context;
		ListCell	   *lc;
		int				i;

		if (prel->parttype == PT_RANGE)
		{
			/*
			 * Get pathkeys for ascending and descending sort by partitioned column.
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
		ranges = list_make1_irange(make_irange(0, PrelLastChild(prel), IR_COMPLETE));

		/* Make wrappers over restrictions and collect final rangeset */
		InitWalkerContext(&context, rti, prel, NULL, false);
		wrappers = NIL;
		foreach(lc, rel->baserestrictinfo)
		{
			WrapperNode	   *wrap;
			RestrictInfo   *rinfo = (RestrictInfo *) lfirst(lc);

			wrap = walk_expr_tree(rinfo->clause, &context);

			paramsel *= wrap->paramsel;
			wrappers = lappend(wrappers, wrap);
			ranges = irange_list_intersection(ranges, wrap->rangeset);
		}

		/* Get number of selected partitions */
		len = irange_list_length(ranges);
		if (prel->enable_parent)
			len++; /* add parent too */

		/* Expand simple_rte_array and simple_rel_array */
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

		/* Parent has already been locked by rewriter */
		parent_rel = heap_open(rte->relid, NoLock);

		/* Add parent if asked to */
		if (prel->enable_parent)
			append_child_relation(root, parent_rel, rti, 0, rte->relid, NULL);

		/*
		 * Iterate all indexes in rangeset and append corresponding child relations.
		 */
		foreach(lc, ranges)
		{
			IndexRange irange = lfirst_irange(lc);

			for (i = irange_lower(irange); i <= irange_upper(irange); i++)
				append_child_relation(root, parent_rel, rti, i, children[i], wrappers);
		}

		/* Now close parent relation */
		heap_close(parent_rel, NoLock);

		/* Clear path list and make it point to NIL */
		list_free_deep(rel->pathlist);
		rel->pathlist = NIL;

#if PG_VERSION_NUM >= 90600
		/* Clear old partial path list */
		list_free(rel->partial_pathlist);
		rel->partial_pathlist = NIL;
#endif

		/* Generate new paths using the rels we've just added */
		set_append_rel_pathlist(root, rel, rti, pathkeyAsc, pathkeyDesc);
		set_append_rel_size_compat(root, rel, rti);

#if PG_VERSION_NUM >= 90600
		/* consider gathering partial paths for the parent appendrel */
		generate_gather_paths(root, rel);
#endif

		/* No need to go further (both nodes are disabled), return */
		if (!(pg_pathman_enable_runtimeappend ||
			  pg_pathman_enable_runtime_merge_append))
			return;

		/* Check that rel's RestrictInfo contains partitioned column */
		rel_part_clauses = get_partitioned_attr_clauses(rel->baserestrictinfo,
														prel, rel->relid);

		/* Runtime[Merge]Append is pointless if there are no params in clauses */
		if (!clause_contains_params((Node *) rel_part_clauses))
			return;

		/* Generate Runtime[Merge]Append paths if needed */
		foreach (lc, rel->pathlist)
		{
			AppendPath	   *cur_path = (AppendPath *) lfirst(lc);
			Relids			inner_required = PATH_REQ_OUTER((Path *) cur_path);
			Path		   *inner_path = NULL;
			ParamPathInfo  *ppi;
			List		   *ppi_part_clauses = NIL;

			/* Fetch ParamPathInfo & try to extract part-related clauses */
			ppi = get_baserel_parampathinfo(root, rel, inner_required);
			if (ppi && ppi->ppi_clauses)
				ppi_part_clauses = get_partitioned_attr_clauses(ppi->ppi_clauses,
																prel, rel->relid);

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
			if (!(rel_part_clauses || ppi_part_clauses))
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

	PlannedStmt	   *result;
	uint32			query_id = parse->queryId;
	bool			pathman_ready = IsPathmanReady(); /* in case it changes */

	PG_TRY();
	{
		if (pathman_ready)
		{
			/* Increment parenthood_statuses refcount */
			incr_refcount_parenthood_statuses();

			/* Modify query tree if needed */
			pathman_transform_query(parse);
		}

		/* Invoke original hook if needed */
		if (planner_hook_next)
			result = planner_hook_next(parse, cursorOptions, boundParams);
		else
			result = standard_planner(parse, cursorOptions, boundParams);

		if (pathman_ready)
		{
			/* Give rowmark-related attributes correct names */
			ExecuteForPlanTree(result, postprocess_lock_rows);

			/* Add PartitionFilter node for INSERT queries */
			ExecuteForPlanTree(result, add_partition_filters);

			/* Decrement parenthood_statuses refcount */
			decr_refcount_parenthood_statuses();

			/* HACK: restore queryId set by pg_stat_statements */
			result->queryId = query_id;
		}
	}
	/* We must decrease parenthood statuses refcount on ERROR */
	PG_CATCH();
	{
		if (pathman_ready)
		{
			/* Caught an ERROR, decrease refcount */
			decr_refcount_parenthood_statuses();
		}

		/* Rethrow ERROR further */
		PG_RE_THROW();
	}
	PG_END_TRY();

	/* Finally return the Plan */
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
		return;

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

	/* Process inlined SQL functions (we've already entered planning stage) */
	if (IsPathmanReady() && get_refcount_parenthood_statuses() > 0)
	{
		/* Check that pg_pathman is the last extension loaded */
		if (post_parse_analyze_hook != pathman_post_parse_analysis_hook)
		{
			char *spl_value; /* value of "shared_preload_libraries" GUC */

#if PG_VERSION_NUM >= 90600
			spl_value = GetConfigOptionByName("shared_preload_libraries", NULL, false);
#else
			spl_value = GetConfigOptionByName("shared_preload_libraries", NULL);
#endif

			ereport(ERROR,
					(errmsg("extension conflict has been detected"),
					 errdetail("shared_preload_libraries = \"%s\"", spl_value),
					 errhint("pg_pathman should be the last extension listed in "
							 "\"shared_preload_libraries\" GUC in order to "
							 "prevent possible conflicts with other extensions")));
		}

		/* Modify query tree if needed */
		pathman_transform_query(query);
	}
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
	if (relid == get_pathman_config_relid(false))
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
				Assert(partitioned_table == InvalidOid);

				/* Which means that 'relid' might be parent */
				if (relid != InvalidOid)
					delay_invalidation_parent_rel(relid);
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
	if (IsPathmanReady())
	{
		Oid			partition_relid;
		AttrNumber	partitioned_col;

		/* Override standard COPY statement if needed */
		if (is_pathman_related_copy(parsetree))
		{
			uint64	processed;

			/* Handle our COPY case (and show a special cmd name) */
			PathmanDoCopy((CopyStmt *) parsetree, queryString, &processed);
			if (completionTag)
				snprintf(completionTag, COMPLETION_TAG_BUFSIZE,
						 "PATHMAN COPY " UINT64_FORMAT, processed);

			return; /* don't call standard_ProcessUtility() or hooks */
		}

		/* Override standard RENAME statement if needed */
		if (is_pathman_related_table_rename(parsetree,
											&partition_relid,
											&partitioned_col))
			PathmanRenameConstraint(partition_relid,
									partitioned_col,
									(const RenameStmt *) parsetree);
	}

	/* Call hooks set by other extensions if needed */
	if (process_utility_hook_next)
		process_utility_hook_next(parsetree, queryString,
								  context, params,
								  dest, completionTag);
	/* Else call internal implementation */
	else
		standard_ProcessUtility(parsetree, queryString,
								context, params,
								dest, completionTag);
}
