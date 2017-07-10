#!/bin/bash

# This is a main testing script for:
#	* regression tests
#	* testgres-based tests
#	* cmocka-based tests
# Copyright (c) 2017, Postgres Professional

set -eux

echo CHECK_CODE=$CHECK_CODE

status=0

# perform code analysis if necessary
if [ "$CHECK_CODE" = "clang" ]; then
    scan-build --status-bugs make USE_PGXS=1 || status=$?
    exit $status

elif [ "$CHECK_CODE" = "cppcheck" ]; then
    cppcheck \
		--template "{file} ({line}): {severity} ({id}): {message}" \
        --enable=warning,portability,performance \
        --suppress=redundantAssignment \
        --suppress=uselessAssignmentPtrArg \
		--suppress=literalWithCharPtrCompare \
        --suppress=incorrectStringBooleanError \
        --std=c89 src/*.c src/include/*.h 2> cppcheck.log

    if [ -s cppcheck.log ]; then
        cat cppcheck.log
        status=1 # error
    fi

    exit $status
fi

# don't forget to "make clean"
make USE_PGXS=1 clean

# initialize database
initdb

# build pg_pathman (using PG_CPPFLAGS and SHLIB_LINK for gcov)
make USE_PGXS=1 PG_CPPFLAGS="-coverage" SHLIB_LINK="-coverage"
make USE_PGXS=1 install

# check build
status=$?
if [ $status -ne 0 ]; then exit $status; fi

# add pg_pathman to shared_preload_libraries and restart cluster 'test'
echo "shared_preload_libraries = 'pg_pathman'" >> $PGDATA/postgresql.conf
echo "port = 55435" >> $PGDATA/postgresql.conf
pg_ctl start -l /tmp/postgres.log -w

# check startup
status=$?
if [ $status -ne 0 ]; then cat /tmp/postgres.log; fi

# run regression tests
PGPORT=55435 make USE_PGXS=1 installcheck || status=$?

# show diff if it exists
if test -f regression.diffs; then cat regression.diffs; fi

set +u

# run python tests
make USE_PGXS=1 python_tests || status=$?
if [ $status -ne 0 ]; then exit $status; fi

set -u

# run cmocka tests (using CFLAGS_SL for gcov)
make USE_PGXS=1 PG_CPPFLAGS="-coverage" cmocka_tests || status=$?
if [ $status -ne 0 ]; then exit $status; fi

# remove useless gcov files
rm -f tests/cmocka/*.gcno
rm -f tests/cmocka/*.gcda

# generate *.gcov files
gcov src/*.c src/compat/*.c src/include/*.h src/include/compat/*.h

# send coverage stats to Coveralls
bash <(curl -s https://codecov.io/bash)

exit $status
