# contrib/pg_pathman/Makefile

MODULE_big = pg_pathman

OBJS = src/init.o src/relation_info.o src/utils.o src/partition_filter.o \
	src/runtime_append.o src/runtime_merge_append.o src/pg_pathman.o src/rangeset.o \
	src/pl_funcs.o src/pl_range_funcs.o src/pl_hash_funcs.o src/pathman_workers.o \
	src/hooks.o src/nodes_common.o src/xact_handling.o src/utility_stmt_hooking.o \
	src/planner_tree_modification.o src/debug_print.o src/partition_creation.o \
	src/compat/pg_compat.o src/compat/rowmarks_fix.o src/partition_router.o \
	src/partition_overseer.o $(WIN32RES)

ifdef USE_PGXS
override PG_CPPFLAGS += -I$(CURDIR)/src/include
else
override PG_CPPFLAGS += -I$(top_srcdir)/$(subdir)/src/include
endif

EXTENSION = pg_pathman

EXTVERSION = 1.5

DATA_built = pg_pathman--$(EXTVERSION).sql

DATA = pg_pathman--1.0--1.1.sql \
	   pg_pathman--1.1--1.2.sql \
	   pg_pathman--1.2--1.3.sql \
	   pg_pathman--1.3--1.4.sql \
	   pg_pathman--1.4--1.5.sql

PGFILEDESC = "pg_pathman - partitioning tool for PostgreSQL"

ifneq (pg_pathman,$(filter pg_pathman,$(PG_TEST_SKIP)))
REGRESS = pathman_array_qual \
		  pathman_basic \
		  pathman_bgw \
		  pathman_cache_pranks \
		  pathman_calamity \
		  pathman_callbacks \
		  pathman_column_type \
		  pathman_cte \
		  pathman_domains \
		  pathman_dropped_cols \
		  pathman_expressions \
		  pathman_foreign_keys \
		  pathman_gaps \
		  pathman_inserts \
		  pathman_interval \
		  pathman_join_clause \
		  pathman_lateral \
		  pathman_hashjoin \
		  pathman_mergejoin \
		  pathman_only \
		  pathman_param_upd_del \
		  pathman_permissions \
		  pathman_rebuild_deletes \
		  pathman_rebuild_updates \
		  pathman_rowmarks \
		  pathman_runtime_nodes \
		  pathman_subpartitions \
		  pathman_update_node \
		  pathman_update_triggers \
		  pathman_upd_del \
		  pathman_utility_stmt \
		  pathman_views \
		  pathman_CVE-2020-14350

REGRESS := $(filter-out pathman_upd_del, $(REGRESS))
endif

ISOLATION = insert_nodes for_update rollback_on_create_partitions

REGRESS_OPTS = --temp-config $(top_srcdir)/$(subdir)/conf.add
ISOLATION_OPTS = --temp-config $(top_srcdir)/$(subdir)/conf.add

CMOCKA_EXTRA_CLEAN = missing_basic.o missing_list.o missing_stringinfo.o missing_bitmapset.o rangeset_tests.o rangeset_tests
EXTRA_CLEAN = $(patsubst %,tests/cmocka/%, $(CMOCKA_EXTRA_CLEAN))

ifdef USE_PGXS
PG_CONFIG=pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
VNUM := $(shell $(PG_CONFIG) --version | awk '{print $$2}')

# check for declarative syntax
# this feature will not be ported to >=12
ifeq ($(VNUM),$(filter 10% 11%,$(VNUM)))
REGRESS += pathman_declarative
OBJS += src/declarative.o
override PG_CPPFLAGS += -DENABLE_DECLARATIVE
endif

# We cannot run isolation test for versions 12,13 in PGXS case
# because 'pg_isolation_regress' is not copied to install
# directory, see src/test/isolation/Makefile
ifeq ($(VNUM),$(filter 12% 13%,$(VNUM)))
undefine ISOLATION
undefine ISOLATION_OPTS
endif

include $(PGXS)
else
subdir = contrib/pg_pathman
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif

$(EXTENSION)--$(EXTVERSION).sql: init.sql hash.sql range.sql
	cat $^ > $@

python_tests:
	$(MAKE) -C tests/python partitioning_tests CASE=$(CASE)

cmocka_tests:
	$(MAKE) -C tests/cmocka check

clean_gcov:
	find . \
		-name "*.gcda" -delete -o \
		-name "*.gcno" -delete -o \
		-name "*.gcov" -delete
