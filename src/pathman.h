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

#include "relation_info.h"
#include "rangeset.h"

#include "postgres.h"
#include "nodes/makefuncs.h"
#include "nodes/primnodes.h"
#include "nodes/execnodes.h"
#include "optimizer/planner.h"
#include "parser/parsetree.h"


/* Check PostgreSQL version (9.5.4 contains an important fix for BGW) */
#if PG_VERSION_NUM < 90503
	#error "Cannot build pg_pathman with PostgreSQL version lower than 9.5.3"
#elif PG_VERSION_NUM < 90504
	#warning "It is STRONGLY recommended to use pg_pathman with PostgreSQL 9.5.4 since it contains important fixes"
#endif

/* Get CString representation of Datum (simple wrapper) */
#ifdef USE_ASSERT_CHECKING
	#include "utils.h"
	#define DebugPrintDatum(datum, typid) ( datum_to_cstring((datum), (typid)) )
#else
	#define DebugPrintDatum(datum, typid) ( "[use --enable-cassert]" )
#endif


/*
 * Definitions for the "pathman_config" table.
 */
#define PATHMAN_CONFIG						"pathman_config"
#define Natts_pathman_config				4
#define Anum_pathman_config_partrel			1	/* partitioned relation (regclass) */
#define Anum_pathman_config_attname			2	/* partitioned column (text) */
#define Anum_pathman_config_parttype		3	/* partitioning type (1|2) */
#define Anum_pathman_config_range_interval	4	/* interval for RANGE pt. (text) */

/* type modifier (typmod) for 'range_interval' */
#define PATHMAN_CONFIG_interval_typmod		-1

/*
 * Definitions for the "pathman_config_params" table.
 */
#define PATHMAN_CONFIG_PARAMS						"pathman_config_params"
#define Natts_pathman_config_params					4
#define Anum_pathman_config_params_partrel			1	/* primary key */
#define Anum_pathman_config_params_enable_parent	2	/* include parent into plan */
#define Anum_pathman_config_params_auto				3	/* auto partitions creation */
#define Anum_pathman_config_params_init_callback	4	/* partition action callback */

/*
 * Definitions for the "pathman_partition_list" view.
 */
#define PATHMAN_PARTITION_LIST				"pathman_partition_list"
#define Natts_pathman_partition_list		6
#define Anum_pathman_pl_parent				1
#define Anum_pathman_pl_partition			2
#define Anum_pathman_pl_parttype			3
#define Anum_pathman_pl_partattr			4
#define Anum_pathman_pl_range_min			5
#define Anum_pathman_pl_range_max			6


/*
 * Cache current PATHMAN_CONFIG relid (set during load_config()).
 */
extern Oid	pathman_config_relid;
extern Oid	pathman_config_params_relid;

/*
 * Just to clarify our intentions (return the corresponding relid).
 */
Oid get_pathman_config_relid(void);
Oid get_pathman_config_params_relid(void);

/*
 * pg_pathman's global state structure.
 */
typedef struct PathmanState
{
	LWLock		   *dsm_init_lock;	/* unused */
} PathmanState;


/*
 * Result of search_range_partition_eq().
 */
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

/*
 * pg_pathman's global state.
 */
extern PathmanState    *pmstate;


int append_child_relation(PlannerInfo *root, RelOptInfo *rel, Index rti,
						  RangeTblEntry *rte, int index, Oid childOID, List *wrappers);

search_rangerel_result search_range_partition_eq(const Datum value,
												 FmgrInfo *cmp_func,
												 const PartRelationInfo *prel,
												 RangeEntry *out_re);

uint32 hash_to_part_index(uint32 value, uint32 partitions);

void handle_modification_query(Query *parse);
void disable_inheritance(Query *parse);
void disable_inheritance_cte(Query *parse);
void disable_inheritance_subselect(Query *parse);

/* copied from allpaths.h */
void set_append_rel_size(PlannerInfo *root, RelOptInfo *rel,
						 Index rti, RangeTblEntry *rte);
void set_append_rel_pathlist(PlannerInfo *root, RelOptInfo *rel, Index rti,
							 RangeTblEntry *rte, PathKey *pathkeyAsc,
							 PathKey *pathkeyDesc);

typedef struct
{
	const Node			   *orig;		/* examined expression */
	List				   *args;		/* extracted from 'orig' */
	List				   *rangeset;	/* IndexRanges representing selected parts */
	bool					found_gap;	/* were there any gaps? */
	double					paramsel;	/* estimated selectivity */
} WrapperNode;

typedef struct
{
	const PartRelationInfo *prel;		/* main partitioning structure */
	ExprContext			   *econtext;	/* for ExecEvalExpr() */
	bool					for_insert;	/* are we in PartitionFilter now? */
} WalkerContext;

/*
 * Usual initialization procedure for WalkerContext.
 */
#define InitWalkerContext(context, prel_info, ecxt, for_ins) \
	do { \
		(context)->prel = (prel_info); \
		(context)->econtext = (ecxt); \
		(context)->for_insert = (for_ins); \
	} while (0)

/* Check that WalkerContext contains ExprContext (plan execution stage) */
#define WcxtHasExprContext(wcxt) ( (wcxt)->econtext )

/*
 * Functions for partition creation, use create_partitions().
 */
Oid create_partitions(Oid relid, Datum value, Oid value_type);
Oid create_partitions_bg_worker(Oid relid, Datum value, Oid value_type);
Oid create_partitions_internal(Oid relid, Datum value, Oid value_type);

void select_range_partitions(const Datum value,
							 FmgrInfo *cmp_func,
							 const RangeEntry *ranges,
							 const int nranges,
							 const int strategy,
							 WrapperNode *result);

/* Examine expression in order to select partitions. */
WrapperNode *walk_expr_tree(Expr *expr, WalkerContext *context);

#endif   /* PATHMAN_H */
