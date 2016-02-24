# contrib/pg_pathman/Makefile

MODULE_big = pg_pathman
OBJS = init.o pg_pathman.o dsm_array.o rangeset.o pl_funcs.o worker.o $(WIN32RES)

EXTENSION = pg_pathman
EXTVERSION = 0.1
DATA_built = $(EXTENSION)--$(EXTVERSION).sql
PGFILEDESC = "pg_pathman - partitioning tool"

REGRESS = pg_pathman
EXTRA_REGRESS_OPTS=--temp-config=$(top_srcdir)/$(subdir)/conf.add
EXTRA_CLEAN = $(EXTENSION)--$(EXTVERSION).sql ./isolation_output

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/pg_pathman
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif

$(EXTENSION)--$(EXTVERSION).sql: init.sql hash.sql range.sql
	cat $^ > $@

ISOLATIONCHECKS=insert_trigger

submake-isolation:
	$(MAKE) -C $(top_builddir)/src/test/isolation all

isolationcheck: | submake-isolation
	$(MKDIR_P) isolation_output
	$(pg_isolation_regress_check) \
	    --temp-config=$(top_srcdir)/$(subdir)/conf.add \
	    --outputdir=./isolation_output \
	    $(ISOLATIONCHECKS)