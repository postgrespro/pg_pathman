#!/bin/bash

set -eux

echo CC=$CC
echo CHECK_CODE=$CHECK_CODE
echo PG_VERSION=$PG_VERSION

# perform code analysis if necessary
if [ $CHECK_CODE = "true" ]; then

	if [ "$CC" = "clang" ]; then
		scan-build --status-bugs make USE_PGXS=1 || status=$?
		exit $status

	elif [ "$CC" = "gcc" ]; then
		cppcheck --template "{file} ({line}): {severity} ({id}): {message}" \
			--enable=warning,portability,performance \
			--suppress=redundantAssignment \
			--suppress=uselessAssignmentPtrArg \
			--suppress=incorrectStringBooleanError \
			--std=c89 src/*.c src/*.h 2> cppcheck.log

		if [ -s cppcheck.log ]; then
			cat cppcheck.log
			status=1 # error
		fi

		exit $status
	fi

	# don't forget to "make clean"
	make USE_PGXS=1 clean
fi

# initialize database
initdb

# build pg_pathman (using CFLAGS_SL for gcov)
make USE_PGXS=1 CFLAGS_SL="$(pg_config --cflags_sl) -coverage"
make USE_PGXS=1 install

# check build
status=$?
if [ $status -ne 0 ]; then exit $status; fi

# add pg_pathman to shared_preload_libraries and restart cluster 'test'
echo "shared_preload_libraries = 'pg_pathman'" >> $PGDATA/postgresql.conf
echo "port = 55435" >> $PGDATA/postgresql.conf
pg_ctl start -l /tmp/postgres.log -w

# run regression tests
PGPORT=55435 make USE_PGXS=1 installcheck || status=$?

# show diff if it exists
if test -f regression.diffs; then cat regression.diffs; fi

set +u

# run python tests
make USE_PGXS=1 python_tests || status=$?
if [ $status -ne 0 ]; then exit $status; fi

set -u

# run mock tests (using CFLAGS_SL for gcov)
make USE_PGXS=1 PG_CPPFLAGS="-coverage" cmocka_tests || status=$?
if [ $status -ne 0 ]; then exit $status; fi

# remove useless gcov files
rm -f tests/cmocka/*.gcno
rm -f tests/cmocka/*.gcda

#generate *.gcov files
gcov src/*.c src/compat/*.c src/include/*.h src/include/compat/*.h

exit $status
