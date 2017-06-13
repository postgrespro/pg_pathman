## pg_pathman's update checker

It's necessary to check that `ALTER EXTENSION pg_pathman UPDATE` produces an SQL frontend that is exactly the same as a fresh install.

Usage:

```bash
PG_CONFIG=... ./dump_pathman_objects %DBNAME%

diff file_1 file_2
```
