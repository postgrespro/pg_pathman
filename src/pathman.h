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

#include "postgres.h"
#include "utils/date.h"
#include "utils/hsearch.h"
#include "utils/snapshot.h"
#include "utils/typcache.h"
#include "nodes/pg_list.h"
#include "nodes/makefuncs.h"
#include "nodes/primnodes.h"
#include "nodes/execnodes.h"
#include "optimizer/planner.h"
#include "parser/parsetree.h"
#include "storage/dsm.h"
#include "storage/lwlock.h"

/* Check PostgreSQL version */
#if PG_VERSION_NUM < 90500
	#error "You are trying to build pg_pathman with PostgreSQL version lower than 9.5.  Please, check your environment."
#endif

#define ALL NIL
#define INITIAL_BLOCKS_COUNT 8192

/*
 * Partitioning type
 */
typedef enum PartType
{
	PT_HASH = 1,
	PT_RANGE
} PartType;

/*
 * Dynamic shared memory array
 */
typedef struct DsmArray
{
	dsm_handle		segment;
	size_t			offset;
	size_t			elem_count;
	size_t			entry_size;
} DsmArray;

/*
 * Hashtable key for relations
 */
typedef struct RelationKey
{
	Oid				dbid;
	Oid				relid;
} RelationKey;

/*
 * PartRelationInfo
 *		Per-relation partitioning information
 *
 *		oid - parent table's Oid
 *		children - list of children's Oids
 *		parttype - partitioning type (HASH, LIST or RANGE)
 *		attnum - attribute number of parent relation's column
 *		atttype - attribute type
 *		atttypmod - attrubute type modifier
 *		cmp_proc - compare fuction for a type of the attribute
 *		hash_proc - hash function for a type of the attribute
 */
typedef struct PartRelationInfo
{
	RelationKey		key;
	DsmArray		children;
	int				children_count;
	PartType		parttype;
	Index			attnum;
	Oid				atttype;
	int32			atttypmod;
	Oid				cmp_proc;
	Oid				hash_proc;
} PartRelationInfo;

/*
 * Child relation info for HASH partitioning
 */
typedef struct HashRelationKey
{
	uint32			hash;
	Oid				parent_oid;
} HashRelationKey;

typedef struct HashRelation
{
	HashRelationKey	key;
	Oid				child_oid;
} HashRelation;

/*
 * Child relation info for RANGE partitioning
 */
typedef struct RangeEntry
{
	Oid				child_oid;

#ifdef HAVE_INT64_TIMESTAMP
	int64			min;
	int64			max;
#else
	double			min;
	double			max;
#endif
} RangeEntry;

typedef struct RangeRelation
{
	RelationKey		key;
	bool			by_val;
	DsmArray		ranges;
} RangeRelation;

typedef struct PathmanState
{
	LWLock		   *load_config_lock;
	LWLock		   *dsm_init_lock;
	LWLock		   *edit_partitions_lock;
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

#define PATHMAN_GET_DATUM(value, by_val) ( (by_val) ? (value) : PointerGetDatum(&value) )

typedef struct {
	bool	ir_valid : 1;
	bool	ir_lossy : 1;
	uint32	ir_lower : 31;
	uint32	ir_upper : 31;
} IndexRange;

#define RANGE_MASK			0xEFFFFFFF

#define InvalidIndexRange	{ false, false, 0, 0 }

inline static IndexRange
make_irange(uint32 lower, uint32 upper, bool lossy)
{
	IndexRange result;

	result.ir_valid = true;
	result.ir_lossy = lossy;
	result.ir_lower = (lower & RANGE_MASK);
	result.ir_upper = (upper & RANGE_MASK);

	return result;
}

inline static IndexRange *
alloc_irange(IndexRange irange)
{
	IndexRange *result = (IndexRange *) palloc(sizeof(IndexRange));

	memcpy((void *) result, (void *) &irange, sizeof(IndexRange));

	return result;
}

#define lfirst_irange(lc)				( *(IndexRange *) lfirst(lc) )
#define lappend_irange(list, irange)	( lappend((list), alloc_irange(irange)) )
#define lcons_irange(irange, list)		( lcons(alloc_irange(irange), (list)) )
#define list_make1_irange(irange)		( lcons(alloc_irange(irange), NIL) )
#define llast_irange(list)				( lfirst_irange(list_tail(list)) )
#define linitial_irange(list)			( lfirst_irange(list_head(list)) )


extern HTAB	   *relations;
extern HTAB	   *range_restrictions;
extern bool		initialization_needed;


/* rangeset.c */
bool irange_intersects(IndexRange a, IndexRange b);
bool irange_conjuncted(IndexRange a, IndexRange b);
IndexRange irange_union(IndexRange a, IndexRange b);
IndexRange irange_intersect(IndexRange a, IndexRange b);
List *irange_list_union(List *a, List *b);
List *irange_list_intersect(List *a, List *b);
int irange_list_length(List *rangeset);
bool irange_list_find(List *rangeset, int index, bool *lossy);

/* Dynamic shared memory functions */
Size get_dsm_shared_size(void);
void init_dsm_config(void);
bool init_dsm_segment(size_t blocks_count, size_t block_size);
void init_dsm_table(size_t block_size, size_t start, size_t end);
void alloc_dsm_array(DsmArray *arr, size_t entry_size, size_t elem_count);
void free_dsm_array(DsmArray *arr);
void resize_dsm_array(DsmArray *arr, size_t entry_size, size_t elem_count);
void *dsm_array_get_pointer(const DsmArray *arr, bool copy);
dsm_handle get_dsm_array_segment(void);
void attach_dsm_array_segment(void);

/* initialization functions */
Size pathman_memsize(void);
void init_shmem_config(void);
void load_config(void);
void create_relations_hashtable(void);
void create_hash_restrictions_hashtable(void);
void create_range_restrictions_hashtable(void);
void load_relations_hashtable(bool reinitialize);
void load_check_constraints(Oid parent_oid, Snapshot snapshot);
void remove_relation_info(Oid relid);

/* utility functions */
int append_child_relation(PlannerInfo *root, RelOptInfo *rel, Index rti,
						  RangeTblEntry *rte, int index, Oid childOID, List *wrappers);
PartRelationInfo *get_pathman_relation_info(Oid relid, bool *found);
RangeRelation *get_pathman_range_relation(Oid relid, bool *found);
search_rangerel_result search_range_partition_eq(const Datum value,
												 FmgrInfo *cmp_func,
												 const RangeRelation *rangerel,
												 RangeEntry *out_rentry);
char *get_extension_schema(void);
Oid create_partitions_bg_worker(Oid relid, Datum value, Oid value_type);
Oid create_partitions(Oid relid, Datum value, Oid value_type, bool *crashed);
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

	/* Cached values */
	const RangeEntry	   *ranges;		/* cached RangeEntry array (copy) */
	size_t					nranges;	/* number of RangeEntries */
	ExprContext			   *econtext;	/* for ExecEvalExpr() */

	/* Runtime values */
	bool					hasLeast,
							hasGreatest;
	Datum					least,
							greatest;
} WalkerContext;

/*
 * Usual initialization procedure for WalkerContext
 */
#define InitWalkerContext(context, prel_info, ecxt) \
	do { \
		(context)->prel = (prel_info); \
		(context)->econtext = (ecxt); \
		(context)->ranges = NULL; \
		(context)->hasLeast = false; \
		(context)->hasGreatest = false; \
	} while (0)

/*
 * We'd like to persist RangeEntry (ranges) array
 * in case of range partitioning, so 'wcxt' is stored
 * inside of Custom Node
 */
#define InitWalkerContextCustomNode(context, prel_info, ecxt, isCached) \
	do { \
		if (!*isCached) \
		{ \
			(context)->prel = prel_info; \
			(context)->econtext = ecxt; \
			(context)->ranges = NULL; \
			*isCached = true; \
		} \
		(context)->hasLeast = false; \
		(context)->hasGreatest = false; \
	} while (0)

void select_range_partitions(const Datum value,
							 const bool byVal,
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
