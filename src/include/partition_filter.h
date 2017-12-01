/* ------------------------------------------------------------------------
 *
 * partition_filter.h
 *		Select partition for INSERT operation
 *
 * Copyright (c) 2016, Postgres Professional
 *
 * ------------------------------------------------------------------------
 */

#ifndef PARTITION_FILTER_H
#define PARTITION_FILTER_H


#include "relation_info.h"
#include "utils.h"

#include "postgres.h"
#include "access/tupconvert.h"
#include "commands/explain.h"
#include "optimizer/planner.h"

#if PG_VERSION_NUM >= 90600
#include "nodes/extensible.h"
#endif


#define INSERT_NODE_NAME "PartitionFilter"


#define ERR_PART_ATTR_NULL				"partitioning expression's value should not be NULL"
#define ERR_PART_ATTR_MULTIPLE_RESULTS	"partitioning expression should return single value"
#define ERR_PART_ATTR_NO_PART			"no suitable partition for key '%s'"
#define ERR_PART_ATTR_MULTIPLE			INSERT_NODE_NAME " selected more than one partition"
#define ERR_PART_DESC_CONVERT			"could not convert row type for partition"


/*
 * Single element of 'result_rels_table'.
 */
typedef struct
{
	Oid					partid;				/* partition's relid */
	ResultRelInfo	   *result_rel_info;	/* cached ResultRelInfo */
	TupleConversionMap *tuple_map;			/* tuple mapping (parent => child) */
	JunkFilter		   *junkfilter;			/* junkfilter for cached ResultRelInfo */
	bool				has_children;		/* hint that it might have children */
	ExprState		   *expr_state;			/* children have their own expressions */
} ResultRelInfoHolder;


/* Default settings for ResultPartsStorage */
#define RPS_DEFAULT_ENTRY_SIZE		sizeof(ResultPartsStorage)
#define RPS_DEFAULT_SPECULATIVE		false /* speculative inserts */

#define RPS_CLOSE_RELATIONS			true
#define RPS_SKIP_RELATIONS			false

/* Neat wrapper for readability */
#define RPS_RRI_CB(cb, args)		(cb), ((void *) args)


/* Forward declaration (for on_rri_holder()) */
struct ResultPartsStorage;
typedef struct ResultPartsStorage ResultPartsStorage;

/*
 * Callback to be fired at rri_holder creation/destruction.
 */
typedef void (*rri_holder_cb)(ResultRelInfoHolder *rri_holder,
							  const ResultPartsStorage *rps_storage);

/*
 * Cached ResultRelInfos of partitions.
 */
struct ResultPartsStorage
{
	ResultRelInfo	   *base_rri;				/* original ResultRelInfo (parent) */
	EState			   *estate;					/* pointer to executor's state */
	CmdType				command_type;			/* INSERT | UPDATE */

	HTAB			   *result_rels_table;
	HASHCTL				result_rels_table_config;

	bool				speculative_inserts;	/* for ExecOpenIndices() */

	rri_holder_cb		init_rri_holder_cb;
	void			   *init_rri_holder_cb_arg;

	rri_holder_cb		fini_rri_holder_cb;
	void			   *fini_rri_holder_cb_arg;

	bool				close_relations;
	LOCKMODE			head_open_lock_mode;
	LOCKMODE			heap_close_lock_mode;
};

typedef struct
{
	CustomScanState		css;

	Oid					partitioned_table;
	OnConflictAction	on_conflict_action;
	List			   *returning_list;

	Plan			   *subplan;				/* proxy variable to store subplan */
	ResultPartsStorage	result_parts;			/* partition ResultRelInfo cache */

	bool				warning_triggered;		/* warning message counter */

	TupleTableSlot	   *tup_convert_slot;		/* slot for rebuilt tuples */
	CmdType				command_type;

	TupleTableSlot     *subplan_slot;			/* slot that was returned from subplan */
	JunkFilter		   *junkfilter;				/* junkfilter for subplan_slot */

	ExprContext		   *tup_convert_econtext;	/* ExprContext for projections */
	ExprState		   *expr_state;				/* for partitioning expression */
} PartitionFilterState;


extern bool					pg_pathman_enable_partition_filter;
extern int					pg_pathman_insert_into_fdw;

extern CustomScanMethods	partition_filter_plan_methods;
extern CustomExecMethods	partition_filter_exec_methods;


#define IsPartitionFilterPlan(node) \
	( \
		IsA((node), CustomScan) && \
		(((CustomScan *) (node))->methods == &partition_filter_plan_methods) \
	)

#define IsPartitionFilterState(node) \
	( \
		IsA((node), CustomScanState) && \
		(((CustomScanState *) (node))->methods == &partition_filter_exec_methods) \
	)

#define IsPartitionFilter(node) \
	( IsPartitionFilterPlan(node) || IsPartitionFilterState(node) )



void init_partition_filter_static_data(void);


/*
 * ResultPartsStorage API (select partition for INSERT & UPDATE).
 */

/* Initialize storage for some parent table */
void init_result_parts_storage(ResultPartsStorage *parts_storage,
							   ResultRelInfo *parent_rri,
							   EState *estate,
							   CmdType cmd_type,
							   Size table_entry_size,
							   bool close_relations,
							   bool speculative_inserts,
							   rri_holder_cb init_rri_holder_cb,
							   void *init_rri_holder_cb_arg,
							   rri_holder_cb fini_rri_holder_cb,
							   void *fini_rri_holder_cb_arg);

/* Free storage and opened relations */
void fini_result_parts_storage(ResultPartsStorage *parts_storage);

/* Find ResultRelInfo holder in storage */
ResultRelInfoHolder * scan_result_parts_storage(Oid partid,
												ResultPartsStorage *storage);

TupleConversionMap * build_part_tuple_map(Relation parent_rel, Relation child_rel);


/* Find suitable partition using 'value' */
Oid * find_partitions_for_value(Datum value, Oid value_type,
								const PartRelationInfo *prel,
								int *nparts);

ResultRelInfoHolder *select_partition_for_insert(ExprState *expr_state,
												 ExprContext *econtext,
												 EState *estate,
												 const PartRelationInfo *prel,
												 ResultPartsStorage *parts_storage);

Plan * make_partition_filter(Plan *subplan,
							 Oid parent_relid,
							 Index parent_rti,
							 OnConflictAction conflict_action,
							 List *returning_list,
							 CmdType command_type);


Node * partition_filter_create_scan_state(CustomScan *node);

void partition_filter_begin(CustomScanState *node,
							EState *estate,
							int eflags);

TupleTableSlot * partition_filter_exec(CustomScanState *node);

void partition_filter_end(CustomScanState *node);

void partition_filter_rescan(CustomScanState *node);

void partition_filter_explain(CustomScanState *node,
							  List *ancestors,
							  ExplainState *es);


#endif /* PARTITION_FILTER_H */
