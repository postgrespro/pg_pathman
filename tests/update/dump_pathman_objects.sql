CREATE EXTENSION IF NOT EXISTS pg_pathman;

SELECT pg_get_functiondef(objid)
FROM pg_catalog.pg_depend JOIN pg_proc ON pg_proc.oid = pg_depend.objid
WHERE refclassid = 'pg_catalog.pg_extension'::REGCLASS AND
          refobjid = (SELECT oid
                                  FROM pg_catalog.pg_extension
                                  WHERE extname = 'pg_pathman') AND
          deptype = 'e'
ORDER BY objid::regprocedure::TEXT ASC;

\d+ pathman_config
\d+ pathman_config_params
\d+ pathman_partition_list
\d+ pathman_cache_stats
\d+ pathman_concurrent_part_tasks
