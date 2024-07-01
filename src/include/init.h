/* ------------------------------------------------------------------------
 *
 * init.h
 *		Initialization functions
 *
 * Copyright (c) 2015-2016, Postgres Professional
 *
 * ------------------------------------------------------------------------
 */

#ifndef PATHMAN_INIT_H
#define PATHMAN_INIT_H


#include "relation_info.h"

#include "postgres.h"
#include "storage/lmgr.h"
#include "utils/guc.h"
#include "utils/hsearch.h"
#include "utils/snapshot.h"


/* Help user in case of emergency */
#define INIT_ERROR_HINT "pg_pathman will be disabled to allow you to resolve this issue"

/* Initial size of 'partitioned_rels' table */
#define PART_RELS_SIZE	10
#define CHILD_FACTOR	500


/*
 * pg_pathman's initialization state structure.
 */
typedef struct
{
	bool 	pg_pathman_enable;		/* GUC variable implementation */
	bool	auto_partition;			/* GUC variable for auto partition propagation */
	bool	override_copy;			/* override COPY TO/FROM */
	bool	initialization_needed;	/* do we need to perform init? */
} PathmanInitState;


/* Check that this is a temporary memory context that's going to be destroyed */
#define AssertTemporaryContext() \
	do { \
		Assert(CurrentMemoryContext != TopMemoryContext); \
		Assert(CurrentMemoryContext != TopPathmanContext); \
		Assert(CurrentMemoryContext != PathmanParentsCacheContext); \
		Assert(CurrentMemoryContext != PathmanStatusCacheContext); \
		Assert(CurrentMemoryContext != PathmanBoundsCacheContext); \
	} while (0)


#define PATHMAN_MCXT_COUNT	4
extern MemoryContext		TopPathmanContext;
extern MemoryContext		PathmanParentsCacheContext;
extern MemoryContext		PathmanStatusCacheContext;
extern MemoryContext		PathmanBoundsCacheContext;

extern HTAB				   *parents_cache;
extern HTAB				   *status_cache;
extern HTAB				   *bounds_cache;

/* pg_pathman's initialization state */
extern PathmanInitState 	pathman_init_state;

/* pg_pathman's hooks state */
extern bool					pathman_hooks_enabled;


#define PATHMAN_TOP_CONTEXT		"maintenance"
#define PATHMAN_PARENTS_CACHE	"partition parents cache"
#define PATHMAN_STATUS_CACHE	"partition status cache"
#define PATHMAN_BOUNDS_CACHE	"partition bounds cache"


/* Transform pg_pathman's memory context into simple name */
static inline const char *
simplify_mcxt_name(MemoryContext mcxt)
{
	if (mcxt == TopPathmanContext)
		return PATHMAN_TOP_CONTEXT;

	else if (mcxt == PathmanParentsCacheContext)
		return PATHMAN_PARENTS_CACHE;

	else if (mcxt == PathmanStatusCacheContext)
		return PATHMAN_STATUS_CACHE;

	else if (mcxt == PathmanBoundsCacheContext)
		return PATHMAN_BOUNDS_CACHE;

	else elog(ERROR, "unknown memory context");

	return NULL;  /* keep compiler quiet */
}


/*
 * Check if pg_pathman is initialized.
 */
#define IsPathmanInitialized()		( !pathman_init_state.initialization_needed )

/*
 * Check if pg_pathman is enabled.
 */
#define IsPathmanEnabled()			( pathman_init_state.pg_pathman_enable )

/*
 * Check if pg_pathman is initialized & enabled.
 */
#define IsPathmanReady()			( IsPathmanInitialized() && IsPathmanEnabled() )

/*
 * Should we override COPY stmt handling?
 */
#define IsOverrideCopyEnabled()		( pathman_init_state.override_copy )

/*
 * Check if auto partition creation is enabled.
 */
#define IsAutoPartitionEnabled()	( pathman_init_state.auto_partition )

/*
 * Enable/disable auto partition propagation. Note that this only works if
 * partitioned relation supports this. See enable_auto() and disable_auto()
 * functions.
 */
#define SetAutoPartitionEnabled(value) \
	do { \
		Assert((value) == true || (value) == false); \
		pathman_init_state.auto_partition = (value); \
	} while (0)

/*
 * Emergency disable mechanism.
 */
#define DisablePathman() \
	do { \
		pathman_init_state.pg_pathman_enable		= false; \
		pathman_init_state.auto_partition			= false; \
		pathman_init_state.override_copy			= false; \
		unload_config(); \
	} while (0)


/* Default column values for PATHMAN_CONFIG_PARAMS */
#define DEFAULT_PATHMAN_ENABLE_PARENT		false
#define DEFAULT_PATHMAN_AUTO				true
#define DEFAULT_PATHMAN_INIT_CALLBACK		InvalidOid
#define DEFAULT_PATHMAN_SPAWN_USING_BGW		false

/* Other default values (for GUCs etc) */
#define DEFAULT_PATHMAN_ENABLE				true
#define DEFAULT_PATHMAN_OVERRIDE_COPY		true


/* Lowest version of Pl/PgSQL frontend compatible with internals */
#define LOWEST_COMPATIBLE_FRONT		"1.5.0"

/* Current version of native C library */
#define CURRENT_LIB_VERSION			"1.5.12"


void *pathman_cache_search_relid(HTAB *cache_table,
								 Oid relid,
								 HASHACTION action,
								 bool *found);

/*
 * Save and restore PathmanInitState.
 */
void save_pathman_init_state(volatile PathmanInitState *temp_init_state);
void restore_pathman_init_state(const volatile PathmanInitState *temp_init_state);

/*
 * Create main GUC variables.
 */
void init_main_pathman_toggles(void);

/*
 * Shared & local config.
 */
Size estimate_pathman_shmem_size(void);
bool load_config(void);
void unload_config(void);


/* Result of find_inheritance_children_array() */
typedef enum
{
	FCS_NO_CHILDREN = 0,	/* could not find any children (GOOD) */
	FCS_COULD_NOT_LOCK,		/* could not lock one of the children */
	FCS_FOUND				/* found some children (GOOD) */
} find_children_status;

find_children_status find_inheritance_children_array(Oid parentrelId,
													 LOCKMODE lockmode,
													 bool nowait,
													 uint32 *children_size,
													 Oid **children);

char *build_check_constraint_name_relid_internal(Oid relid);
char *build_check_constraint_name_relname_internal(const char *relname);

char *build_sequence_name_relid_internal(Oid relid);
char *build_sequence_name_relname_internal(const char *relname);

char *build_update_trigger_name_internal(Oid relid);
char *build_update_trigger_func_name_internal(Oid relid);

bool pathman_config_contains_relation(Oid relid,
									  Datum *values,
									  bool *isnull,
									  TransactionId *xmin,
									  ItemPointerData *iptr);

void pathman_config_invalidate_parsed_expression(Oid relid);

void pathman_config_refresh_parsed_expression(Oid relid,
											  Datum *values,
											  bool *isnull,
											  ItemPointer iptr);


bool read_pathman_params(Oid relid,
						 Datum *values,
						 bool *isnull);


bool validate_range_constraint(const Expr *expr,
							   const PartRelationInfo *prel,
							   Datum *lower, Datum *upper,
							   bool *lower_null, bool *upper_null);

bool validate_hash_constraint(const Expr *expr,
							  const PartRelationInfo *prel,
							  uint32 *part_idx);


#endif /* PATHMAN_INIT_H */
