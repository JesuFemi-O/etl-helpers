-- check the table first to see how many records
SELECT * FROM incremental_poc.bigquery_replica_table;

-- execute the stored procedure
CALL incremental_poc.incremental_load_procedure();