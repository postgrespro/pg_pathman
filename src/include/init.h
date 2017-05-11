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
		Assert(CurrentMemoryContext != PathmanRelationCacheContext); \
		Assert(CurrentMemoryContext != PathmanParentCacheContext); \
		Assert(CurrentMemoryContext != PathmanBoundCacheContext); \
	} while (0)


#define PATHMAN_MCXT_COUNT	4
extern MemoryContext		TopPathmanContext;
extern MemoryContext		PathmanRelationCacheContext;
extern MemoryContext		PathmanParentCacheContext;
extern MemoryContext		PathmanBoundCacheContext;

extern HTAB				   *partitioned_rels;
extern HTAB				   *parent_cache;
extern HTAB				   *bound_cache;

/* pg_pathman's initialization state */
extern PathmanInitState 	pg_pathman_init_state;


/* Transform pg_pathman's memory context into simple name */
static inline const char *
simpify_mcxt_name(MemoryContext mcxt)
{
	static const char  *top_mcxt	= "maintenance",
					   *rel_mcxt	= "partition dispatch cache",
					   *parent_mcxt	= "partition parents cache",
					   *bound_mcxt	= "partition bounds cache";

	if (mcxt == TopPathmanContext)
		return top_mcxt;

	else if (mcxt == PathmanRelationCacheContext)
		return rel_mcxt;

	else if (mcxt == PathmanParentCacheContext)
		return parent_mcxt;

	else if (mcxt == PathmanBoundCacheContext)
		return bound_mcxt;

	else elog(ERROR, "error in function " CppAsString(simpify_mcxt_name));
}


/*
 * Check if pg_pathman is initialized.
 */
#define IsPathmanInitialized()		( !pg_pathman_init_state.initialization_needed )

/*
 * Check if pg_pathman is enabled.
 */
#define IsPathmanEnabled()			( pg_pathman_init_state.pg_pathman_enable )

/*
 * Check if pg_pathman is initialized & enabled.
 */
#define IsPathmanReady()			( IsPathmanInitialized() && IsPathmanEnabled() )

/*
 * Should we override COPY stmt handling?
 */
#define IsOverrideCopyEnabled()		( pg_pathman_init_state.override_copy )

/*
 * Check if auto partition creation is enabled.
 */
#define IsAutoPartitionEnabled()	( pg_pathman_init_state.auto_partition )

/*
 * Enable/disable auto partition propagation. Note that this only works if
 * partitioned relation supports this. See enable_auto() and disable_auto()
 * functions.
 */
#define SetAutoPartitionEnabled(value) \
	do { \
		Assert((value) == true || (value) == false); \
		pg_pathman_init_state.auto_partition = (value); \
	} while (0)

/*
 * Emergency disable mechanism.
 */
#define DisablePathman() \
	do { \
		pg_pathman_init_state.pg_pathman_enable = false; \
		pg_pathman_init_state.auto_partition = false; \
		pg_pathman_init_state.override_copy = false; \
		pg_pathman_init_state.initialization_needed = true; \
	} while (0)


/* Default column values for PATHMAN_CONFIG_PARAMS */
#define DEFAULT_ENABLE_PARENT		false
#define DEFAULT_AUTO				true
#define DEFAULT_INIT_CALLBACK		InvalidOid
#define DEFAULT_SPAWN_USING_BGW		false

/* Other default values (for GUCs etc) */
#define DEFAULT_PATHMAN_ENABLE		true
#define DEFAULT_OVERRIDE_COPY		true


/* Lowest version of Pl/PgSQL frontend compatible with internals (0xAA_BB_CC) */
#define LOWEST_COMPATIBLE_FRONT		0x010400

/* Current version of native C library (0xAA_BB_CC) */
#define CURRENT_LIB_VERSION			0x010400


void *pathman_cache_search_relid(HTAB *cache_table,
								 Oid relid,
								 HASHACTION action,
								 bool *found);

/*
 * Save and restore PathmanInitState.
 */
void save_pathman_init_state(PathmanInitState *temp_init_state);
void restore_pathman_init_state(const PathmanInitState *temp_init_state);

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

char *build_sequence_name_internal(Oid relid);

char *build_update_trigger_name_internal(Oid relid);
char *build_update_trigger_func_name_internal(Oid relid);

char *build_nop_trigger_name_internal(Oid relid);

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
