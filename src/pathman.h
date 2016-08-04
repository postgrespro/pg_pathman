/* ------------------------------------------------------------------------
 *
 * pathman.h
 *		structures and prototypes for pathman functions
 *
 * Copyright (c) 2015-2016, Postgres Professional
 *
 * ------------------------------------------------------------------------
 */

#ifndef PATHMAN_H
#define PATHMAN_H

#include "dsm_array.h"
#include "init.h"
#include "relation_info.h"
#include "rangeset.h"

#include "postgres.h"
#include "utils/date.h"
#include "utils/snapshot.h"
#include "utils/typcache.h"
#include "nodes/makefuncs.h"
#include "nodes/primnodes.h"
#include "nodes/execnodes.h"
#include "optimizer/planner.h"
#include "parser/parsetree.h"
#include "storage/lwlock.h"


/* Check PostgreSQL version */
#if PG_VERSION_NUM < 90500
	#error "You are trying to build pg_pathman with PostgreSQL version lower than 9.5.  Please, check your environment."
#endif

#define ALL NIL
#define INITIAL_BLOCKS_COUNT 8192


/*
 * Definitions for the "pathman_config" table.
 */
#define PATHMAN_CONFIG						"pathman_config"
#define Natts_pathman_config				5
#define Anum_pathman_config_id				1
#define Anum_pathman_config_partrel			2
#define Anum_pathman_config_attname			3
#define Anum_pathman_config_parttype		4
#define Anum_pathman_config_range_interval	5

#define PATHMAN_CONFIG_partrel_idx			"pathman_config_partrel_idx"


/*
 * pg_pathman's global state.
 */
typedef struct PathmanState
{
	LWLock		   *dsm_init_lock,
				   *load_config_lock,
				   *edit_partitions_lock;
	DsmArray		databases;
} PathmanState;


typedef enum
{
	SEARCH_RANGEREL_OUT_OF_RANGE = 0,
	SEARCH_RANGEREL_GAP,
	SEARCH_RANGEREL_FOUND
} search_rangerel_result;


/*
 * The list of partitioned relation relids that must be handled by pg_pathman
 */
extern List			   *inheritance_enabled_relids;
/*
 * This list is used to ensure that partitioned relation isn't used both
 * with and without ONLY modifiers
 */
extern List			   *inheritance_disabled_relids;

extern bool 			pg_pathman_enable;
extern PathmanState    *pmstate;


#define PATHMAN_GET_DATUM(value, by_val) \
	( (by_val) ? (Datum) (value) : PointerGetDatum(&value) )

/*
 * Check if pg_pathman is initialized & enabled.
 */
#define IsPathmanReady() ( !initialization_needed && pg_pathman_enable )

#define IsPathmanEnabled() ( pg_pathman_enable )

#define DisablePathman() \
	do { \
		pg_pathman_enable = false; \
	} while (0)


/* utility functions */
int append_child_relation(PlannerInfo *root, RelOptInfo *rel, Index rti,
						  RangeTblEntry *rte, int index, Oid childOID, List *wrappers);

search_rangerel_result search_range_partition_eq(const Datum value,
												 FmgrInfo *cmp_func,
												 const PartRelationInfo *prel,
												 RangeEntry *out_re);

Oid create_partitions_bg_worker(Oid relid, Datum value, Oid value_type);
uint32 make_hash(uint32 value, uint32 partitions);

void handle_modification_query(Query *parse);
void disable_inheritance(Query *parse);
void disable_inheritance_cte(Query *parse);
void disable_inheritance_subselect(Query *parse);

/* copied from allpaths.h */
void set_append_rel_size(PlannerInfo *root, RelOptInfo *rel,
						 Index rti, RangeTblEntry *rte);
void set_append_rel_pathlist(PlannerInfo *root, RelOptInfo *rel, Index rti, RangeTblEntry *rte,
							 PathKey *pathkeyAsc, PathKey *pathkeyDesc);

typedef struct
{
	const Node			   *orig;
	List				   *args;
	List				   *rangeset;
	bool					found_gap;
	double					paramsel;
} WrapperNode;

typedef struct
{
	/* Main partitioning structure */
	const PartRelationInfo *prel;

	/* Long-living context for cached values */
	MemoryContext			persistent_mcxt;

	/* Cached values */
	const RangeEntry	   *ranges;		/* cached RangeEntry array (copy) */
	size_t					nranges;	/* number of RangeEntries */
	ExprContext			   *econtext;	/* for ExecEvalExpr() */

	/* Runtime values */
	bool					hasLeast,
							hasGreatest;
	Datum					least,
							greatest;

	bool					for_insert;	/* are we in PartitionFilter now? */
} WalkerContext;

/*
 * Usual initialization procedure for WalkerContext
 */
#define InitWalkerContext(context, prel_info, ecxt, mcxt, for_ins) \
	do { \
		(context)->prel = (prel_info); \
		(context)->econtext = (ecxt); \
		(context)->ranges = NULL; \
		(context)->nranges = 0; \
		(context)->hasLeast = false; \
		(context)->hasGreatest = false; \
		(context)->persistent_mcxt = (mcxt); \
		(context)->for_insert = (for_ins); \
	} while (0)

/*
 * We'd like to persist RangeEntry (ranges) array
 * in case of range partitioning, so 'wcxt' is stored
 * inside of Custom Node
 */
#define InitWalkerContextCustomNode(context, prel_info, ecxt, mcxt, for_ins, isCached) \
	do { \
		if (!*isCached) \
		{ \
			(context)->prel = prel_info; \
			(context)->econtext = ecxt; \
			(context)->ranges = NULL; \
			(context)->nranges = 0; \
			(context)->persistent_mcxt = (mcxt); \
			(context)->for_insert = (for_ins); \
			*isCached = true; \
		} \
		(context)->hasLeast = false; \
		(context)->hasGreatest = false; \
	} while (0)

/* Check that WalkerContext contains ExprContext (plan execution stage) */
#define WcxtHasExprContext(wcxt) ( (wcxt)->econtext )

Oid create_partitions_internal(Oid relid, Datum value, Oid value_type);
Oid create_partitions(Oid relid, Datum value, Oid value_type);

void select_range_partitions(const Datum value,
							 FmgrInfo *cmp_func,
							 const RangeEntry *ranges,
							 const size_t nranges,
							 const int strategy,
							 WrapperNode *result);

WrapperNode *walk_expr_tree(Expr *expr, WalkerContext *context);
void finish_least_greatest(WrapperNode *wrap, WalkerContext *context);
void refresh_walker_context_ranges(WalkerContext *context);
void clear_walker_context(WalkerContext *context);

#endif   /* PATHMAN_H */
