#!/bin/bash

# This is a main testing script for:
#	* regression tests
#	* testgres-based tests
#	* cmocka-based tests
# Copyright (c) 2017, Postgres Professional

set -ux

echo CHECK_CODE=$CHECK_CODE
echo PG_VERSION=$(pg_config --version)

status=0

# change relevant core dump settings
CORE_DIR=/tmp/cores
ulimit -c unlimited -S
mkdir "$CORE_DIR"
echo "$CORE_DIR/%e-%s-%p.core" | sudo tee /proc/sys/kernel/core_pattern

# perform code analysis if necessary
if [ "$CHECK_CODE" = "clang" ]; then
    scan-build --status-bugs make USE_PGXS=1 || status=$?
    exit $status
fi

# we need testgres for pathman tests
virtualenv env
export VIRTUAL_ENV_DISABLE_PROMPT=1
source env/bin/activate
pip install testgres
pip freeze | grep testgres

# initialize database
initdb

# build pg_pathman (using PG_CPPFLAGS and SHLIB_LINK for gcov)
set -e
make USE_PGXS=1 clean
make USE_PGXS=1 PG_CPPFLAGS="-coverage" SHLIB_LINK="-coverage"
make USE_PGXS=1 install
set +e

# add pg_pathman to shared_preload_libraries and restart cluster 'test'
echo "shared_preload_libraries = 'pg_pathman'" >> $PGDATA/postgresql.conf
echo "port = 55435" >> $PGDATA/postgresql.conf
pg_ctl start -l /tmp/postgres.log -w || cat /tmp/postgres.log

# run regression tests
export PG_REGRESS_DIFF_OPTS="-w -U3" # for alpine's diff (BusyBox)
PGPORT=55435 make USE_PGXS=1 installcheck || status=$?

# show diff if it exists
if test -f regression.diffs; then cat regression.diffs; fi

# list cores and exit if we failed
ls "$CORE_DIR"
if [ $status -ne 0 ]; then exit $status; fi

# run python tests
set +u
make USE_PGXS=1 python_tests || status=$?
set -u

# list cores and exit if we failed
ls "$CORE_DIR"
if [ $status -ne 0 ]; then exit $status; fi

# run cmocka tests (using CFLAGS_SL for gcov)
make USE_PGXS=1 PG_CPPFLAGS="-coverage" cmocka_tests || exit $?

# remove useless gcov files
rm -f tests/cmocka/*.gcno
rm -f tests/cmocka/*.gcda

# generate *.gcov files
gcov src/*.c src/compat/*.c src/include/*.h src/include/compat/*.h

# send coverage stats to Coveralls
set +u
bash <(curl -s https://codecov.io/bash)
set -u

exit $status
