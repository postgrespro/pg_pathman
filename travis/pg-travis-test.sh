#!/bin/bash

set -eux

sudo apt-get update


# required packages
packages="postgresql-$PGVERSION postgresql-server-dev-$PGVERSION postgresql-common"

# exit code
status=0

# pg_config path
config_path=/usr/lib/postgresql/$PGVERSION/bin/pg_config


# bug: http://www.postgresql.org/message-id/20130508192711.GA9243@msgid.df7cb.de
sudo update-alternatives --remove-all postmaster.1.gz

# stop all existing instances (because of https://github.com/travis-ci/travis-cookbooks/pull/221)
sudo service postgresql stop
# ... and make sure they don't come back
echo 'exit 0' | sudo tee /etc/init.d/postgresql
sudo chmod a+x /etc/init.d/postgresql

# install required packages
sudo apt-get -o Dpkg::Options::="--force-confdef" -o Dpkg::Options::="--force-confold" -y install -qq $packages

# create cluster 'test'
sudo pg_createcluster --start $PGVERSION test -p 55435 -- -A trust

# perform code analysis if necessary
if [ $CHECK_CODE = "true" ]; then

	if [ "$CC" = "clang" ]; then
		sudo apt-get -y install -qq clang-3.5

		scan-build-3.5 --status-bugs make USE_PGXS=1 PG_CONFIG=$config_path || status=$?
		exit $status

	elif [ "$CC" = "gcc" ]; then
		sudo apt-get -y install -qq cppcheck

		cppcheck --template "{file} ({line}): {severity} ({id}): {message}" \
			--enable=warning,portability,performance \
			--suppress=redundantAssignment \
			--suppress=uselessAssignmentPtrArg \
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

# build pg_pathman
make USE_PGXS=1 PG_CONFIG=$config_path
sudo make install USE_PGXS=1 PG_CONFIG=$config_path

# add pg_pathman to shared_preload_libraries and restart cluster 'test'
sudo bash -c "echo \"shared_preload_libraries = 'pg_pathman'\" >> /etc/postgresql/$PGVERSION/test/postgresql.conf"
sudo pg_ctlcluster $PGVERSION test restart

# run regression tests
PGPORT=55435 make installcheck USE_PGXS=1 PGUSER=postgres PG_CONFIG=$config_path || status=$?

# show diff if it exists
if test -f regression.diffs; then cat regression.diffs; fi
exit $status
