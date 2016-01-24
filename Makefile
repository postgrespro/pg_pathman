# contrib/pg_pathman/Makefile

MODULE_big = pg_pathman
OBJS = init.o pg_pathman.o dsm_array.o rangeset.o pl_funcs.o $(WIN32RES)

EXTENSION = pg_pathman
EXTVERSION = 0.1
DATA_built = $(EXTENSION)--$(EXTVERSION).sql
PGFILEDESC = "pg_pathman - partitioning tool"

REGRESS = pg_pathman
EXTRA_CLEAN = $(EXTENSION)--$(EXTVERSION).sql

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

$(EXTENSION)--$(EXTVERSION).sql: sql/init.sql sql/hash.sql sql/range.sql
	cat $^ > $@
check: EXTRA_REGRESS_OPTS=--temp-config=$(top_srcdir)/$(subdir)/conf.add
