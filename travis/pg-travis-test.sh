#!/bin/bash

set -eux

sudo apt-get update


# required packages
apt_packages="postgresql-$PG_VER postgresql-server-dev-$PG_VER postgresql-common python-pip python-dev build-essential"
pip_packages="testgres==0.4.0"

# exit code
status=0

# pg_config path
pg_ctl_path=/usr/lib/postgresql/$PG_VER/bin/pg_ctl
initdb_path=/usr/lib/postgresql/$PG_VER/bin/initdb
config_path=/usr/lib/postgresql/$PG_VER/bin/pg_config


# bug: http://www.postgresql.org/message-id/20130508192711.GA9243@msgid.df7cb.de
sudo update-alternatives --remove-all postmaster.1.gz

# stop all existing instances (because of https://github.com/travis-ci/travis-cookbooks/pull/221)
sudo service postgresql stop
# ... and make sure they don't come back
echo 'exit 0' | sudo tee /etc/init.d/postgresql
sudo chmod a+x /etc/init.d/postgresql

# install required packages
sudo apt-get -o Dpkg::Options::="--force-confdef" -o Dpkg::Options::="--force-confold" -y install -qq $apt_packages


# perform code analysis if necessary
if [ $CHECK_CODE = "true" ]; then

	if [ "$CC" = "clang" ]; then
		sudo apt-get -y install -qq clang-$LLVM_VER

		scan-build-$LLVM_VER --status-bugs make USE_PGXS=1 PG_CONFIG=$config_path || status=$?
		exit $status

	elif [ "$CC" = "gcc" ]; then
		sudo apt-get -y install -qq cppcheck

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
	make clean USE_PGXS=1 PG_CONFIG=$config_path
fi


# create cluster 'test'
CLUSTER_PATH=$(pwd)/test_cluster
$initdb_path -D $CLUSTER_PATH -U $USER -A trust

# build pg_pathman (using CFLAGS_SL for gcov)
make USE_PGXS=1 PG_CONFIG=$config_path CFLAGS_SL="$($config_path --cflags_sl) -coverage"
sudo make install USE_PGXS=1 PG_CONFIG=$config_path

# check build
status=$?
if [ $status -ne 0 ]; then exit $status; fi

# set permission to write postgres locks
sudo chown $USER /var/run/postgresql/

# add pg_pathman to shared_preload_libraries and restart cluster 'test'
echo "shared_preload_libraries = 'pg_pathman'" >> $CLUSTER_PATH/postgresql.conf
echo "port = 55435" >> $CLUSTER_PATH/postgresql.conf
$pg_ctl_path -D $CLUSTER_PATH start -l postgres.log -w

# run regression tests
PGPORT=55435 PGUSER=$USER PG_CONFIG=$config_path make installcheck USE_PGXS=1 || status=$?

# show diff if it exists
if test -f regression.diffs; then cat regression.diffs; fi


set +u

# create virtual environment and activate it
virtualenv /tmp/envs/pg_pathman --python=python3
source /tmp/envs/pg_pathman/bin/activate
type python
type pip

# install pip packages
pip install $pip_packages

# run python tests
make USE_PGXS=1 PG_CONFIG=$config_path python_tests || status=$?

# deactivate virtual environment
deactivate

set -u


# install cmake for cmocka
sudo apt-get -y install -qq cmake

# build & install cmocka
CMOCKA_VER=1.1.1
cd tests/cmocka
tar xf cmocka-$CMOCKA_VER.tar.xz
cd cmocka-$CMOCKA_VER
mkdir build && cd build
cmake ..
make && sudo make install
cd ../../../..

# export path to libcmocka.so
LD_LIBRARY_PATH=/usr/local/lib
export LD_LIBRARY_PATH

# run cmocka tests (using CFLAGS_SL for gcov)
make USE_PGXS=1 PG_CONFIG=$config_path PG_CPPFLAGS="-coverage" cmocka_tests || status=$?

# remove useless gcov files
rm -f tests/cmocka/*.gcno
rm -f tests/cmocka/*.gcda

#generate *.gcov files
gcov src/*.c src/compat/*.c src/include/*.h src/include/compat/*.h


exit $status
