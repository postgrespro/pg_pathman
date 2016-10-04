# Tests

This directory contains script to tests some features which cannot be tested
with only regression tests

## Running

First of all you need to install `testgres` python module which contains useful
functions to start postgres clusters and make queries:

```
pip install testgres
```

To run tests execute:

```
python -m unittest partitioning_test
```

from current directory. If you want to run a specific postgres build then
you should specify the path to your pg_config executable by setting PG_CONFIG
environment variable:

```
export PG_CONFIG=/path/to/pg_config
```

To test FDW features you need to install postgres_fdw contrib module first.
If you want to skip FDW tests set the FDW_DISABLED environment variable:

```
export FDW_DISABLED=1
```
