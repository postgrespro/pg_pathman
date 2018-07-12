FROM postgres:${PG_VERSION}-alpine

# Install dependencies
RUN apk add --no-cache \
	openssl curl \
	cmocka-dev \
	perl perl-ipc-run \
	python3 python3-dev py3-virtualenv \
	coreutils linux-headers \
	make musl-dev gcc bison flex \
	zlib-dev libedit-dev \
	clang clang-analyzer;

# Install fresh valgrind
RUN apk add valgrind \
	--update-cache \
	--repository http://dl-3.alpinelinux.org/alpine/edge/main;

# Environment
ENV LANG=C.UTF-8 PGDATA=/pg/data

# Make directories
RUN	mkdir -p ${PGDATA} && \
	mkdir -p /pg/testdir

# Add data to test dir
ADD . /pg/testdir

# Grant privileges
RUN	chown -R postgres:postgres ${PGDATA} && \
	chown -R postgres:postgres /pg/testdir && \
	chmod a+rwx /usr/local/lib/postgresql && \
	chmod a+rwx /usr/local/share/postgresql/extension

COPY run_tests.sh /run.sh
RUN chmod 755 /run.sh

USER postgres
WORKDIR /pg/testdir
ENTRYPOINT LEVEL=${LEVEL} /run.sh
