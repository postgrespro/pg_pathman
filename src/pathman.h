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
	#error "You are trying to build pg_pathman with PostgreSQL version lower than 9.5.  Please, check you environment."
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
	dsm_handle	segment;
	size_t		offset;
	size_t		length;
} DsmArray;

/*
 * Hashtable key for relations
 */
typedef struct RelationKey
{
	Oid		dbid;
	Oid		relid;
} RelationKey;

/*
 * PartRelationInfo
 *		Per-relation partitioning information
 *
 *		oid - parent table oid
 *		children - list of children oids
 *		parttype - partitioning type (HASH, LIST or RANGE)
 *		attnum - attribute number of parent relation
 */
typedef struct PartRelationInfo
{
	RelationKey	key;
	DsmArray	children;
	int			children_count;
	PartType	parttype;
	Index		attnum;
	Oid			atttype;
	Oid			cmp_proc;
	Oid			hash_proc;
} PartRelationInfo;

/*
 * Child relation for HASH partitioning
 */
typedef struct HashRelationKey
{
	int		hash;
	Oid		parent_oid;
} HashRelationKey;

typedef struct HashRelation
{
	HashRelationKey key;
	Oid		child_oid;
} HashRelation;

/*
 * Child relation for RANGE partitioning
 */
typedef struct RangeEntry
{
	Oid			child_oid;
#ifdef HAVE_INT64_TIMESTAMP
	int64		min;
	int64		max;
#else
	double		min;
	double		max;
#endif
} RangeEntry;

typedef struct RangeRelation
{
	RelationKey	key;
	bool        by_val;
	DsmArray    ranges;
} RangeRelation;

typedef struct PathmanState
{
	LWLock	   *load_config_lock;
	LWLock	   *dsm_init_lock;
	LWLock	   *edit_partitions_lock;
	DsmArray	databases;
} PathmanState;

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

#define PATHMAN_GET_DATUM(value, by_val) ( (by_val) ? (Datum) (value) : PointerGetDatum(&value) )

typedef int IndexRange;
#define RANGE_INFINITY 0x7FFF
#define RANGE_LOSSY 0x80000000

#define make_irange(lower, upper, lossy) \
	(((lower) & RANGE_INFINITY) << 15 | ((upper) & RANGE_INFINITY) | ((lossy) ? RANGE_LOSSY : 0))

#define irange_lower(irange) \
	(((irange) >> 15) & RANGE_INFINITY)

#define irange_upper(irange) \
	((irange) & RANGE_INFINITY)

#define irange_is_lossy(irange) \
	((irange) & RANGE_LOSSY)

#define lfirst_irange(lc)				((IndexRange)(lc)->data.int_value)
#define lappend_irange(list, irange)	(lappend_int((list), (int)(irange)))
#define lcons_irange(irange, list)		lcons_int((int)(irange), (list))
#define list_make1_irange(irange)		lcons_int((int)(irange), NIL)
#define llast_irange(l)					(IndexRange)lfirst_int(list_tail(l))

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
void alloc_dsm_array(DsmArray *arr, size_t entry_size, size_t length);
void free_dsm_array(DsmArray *arr);
void resize_dsm_array(DsmArray *arr, size_t entry_size, size_t length);
void *dsm_array_get_pointer(const DsmArray* arr);
dsm_handle get_dsm_array_segment(void);
void attach_dsm_array_segment(void);

HTAB *relations;
HTAB *range_restrictions;
bool initialization_needed;

/* initialization functions */
Size pathman_memsize(void);
void init_shmem_config(void);
void load_config(void);
void create_relations_hashtable(void);
void create_hash_restrictions_hashtable(void);
void create_range_restrictions_hashtable(void);
void load_relations(bool reinitialize);
void load_partitions(Oid parent_oid, Snapshot snapshot);
void remove_relation_info(Oid relid);

/* utility functions */
int append_child_relation(PlannerInfo *root, RelOptInfo *rel, Index rti,
						  RangeTblEntry *rte, int index, Oid childOID, List *wrappers);
PartRelationInfo *get_pathman_relation_info(Oid relid, bool *found);
RangeRelation *get_pathman_range_relation(Oid relid, bool *found);
int range_binary_search(const RangeRelation *rangerel, FmgrInfo *cmp_func, Datum value, bool *fountPtr);
char *get_extension_schema(void);
FmgrInfo *get_cmp_func(Oid type1, Oid type2);
Oid create_partitions_bg_worker(Oid relid, Datum value, Oid value_type, bool *crashed);
Oid create_partitions(Oid relid, Datum value, Oid value_type, bool *crashed);
uint32 make_hash(uint32 value, uint32 partitions);

/* copied from allpaths.h */
void set_append_rel_size(PlannerInfo *root, RelOptInfo *rel,
						 Index rti, RangeTblEntry *rte);
void set_append_rel_pathlist(PlannerInfo *root, RelOptInfo *rel, Index rti, RangeTblEntry *rte,
							 PathKey *pathkeyAsc, PathKey *pathkeyDesc);

typedef struct
{
	const Node	   *orig;
	List		   *args;
	List		   *rangeset;
	double			paramsel;
} WrapperNode;

typedef struct
{
	const PartRelationInfo *prel;
	bool					hasLeast,
							hasGreatest;
	Datum					least,
							greatest;

	PlanState			   *pstate;
	ExprContext			   *econtext;
} WalkerContext;

WrapperNode *walk_expr_tree(Expr *expr, WalkerContext *context);
void finish_least_greatest(WrapperNode *wrap, WalkerContext *context);

#endif   /* PATHMAN_H */
