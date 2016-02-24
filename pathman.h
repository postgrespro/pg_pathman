#ifndef PATHMAN_H
#define PATHMAN_H

#include "postgres.h"
#include "utils/date.h"
#include "utils/hsearch.h"
#include "utils/snapshot.h"
#include "nodes/pg_list.h"
#include "storage/dsm.h"
#include "storage/lwlock.h"


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
	Oid		child_oid;
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

#define PATHMAN_GET_DATUM(value, by_val) ( (by_val) ? (value) : PointerGetDatum(&value) )

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


LWLock *load_config_lock;
LWLock *dsm_init_lock;
LWLock *edit_partitions_lock;


/* Dynamic shared memory functions */
void init_dsm_config(void);
bool init_dsm_segment(size_t blocks_count, size_t block_size);
void init_dsm_table(size_t block_size, size_t start, size_t end);
void alloc_dsm_array(DsmArray *arr, size_t entry_size, size_t length);
void free_dsm_array(DsmArray *arr);
void *dsm_array_get_pointer(const DsmArray* arr);
dsm_handle get_dsm_array_segment(void);
void attach_dsm_array_segment(void);

HTAB *relations;
HTAB *range_restrictions;
bool initialization_needed;

/* initialization functions */
void init_shmem_config(void);
void load_config(void);
void create_relations_hashtable(void);
void create_hash_restrictions_hashtable(void);
void create_range_restrictions_hashtable(void);
void load_relations_hashtable(bool reinitialize);
void load_check_constraints(Oid parent_oid, Snapshot snapshot);
void remove_relation_info(Oid relid);

/* utility functions */
PartRelationInfo *get_pathman_relation_info(Oid relid, bool *found);
RangeRelation *get_pathman_range_relation(Oid relid, bool *found);
int range_binary_search(const RangeRelation *rangerel, FmgrInfo *cmp_func, Datum value, bool *fountPtr);
char *get_extension_schema(void);
FmgrInfo *get_cmp_func(Oid type1, Oid type2);
Oid create_partitions_bg_worker(Oid relid, Datum value, Oid value_type);
Oid create_partitions(Oid relid, Datum value, Oid value_type);

#endif   /* PATHMAN_H */
