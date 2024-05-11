CREATE OR REPLACE PROCEDURE incremental_poc.incremental_load_procedure()
BEGIN
  DECLARE max_updated_at TIMESTAMP DEFAULT NULL;
  DECLARE sql_query STRING;
  
  -- Step 1: Get the maximum updated_at value from the bigquery replica table
  SET max_updated_at = (SELECT MAX(updated_at) FROM incremental_poc.bigquery_replica_table);
  
  -- Step 2: Construct the federated query with a filter for updated_at > max_updated_at
  SET sql_query = '''
    SELECT *
    FROM incremental_poc.external_sample
    WHERE updated_at > "''' || CAST(max_updated_at AS STRING) || '''"
  ''';
  
  -- Step 3: Perform the federated query and handle the results
  EXECUTE IMMEDIATE (
    'CREATE OR REPLACE TABLE incremental_poc.temp_table AS ' || sql_query
  );
  
  -- Step 4: Update existing records and insert new records in the bigquery replica table
  MERGE INTO incremental_poc.bigquery_replica_table AS target
  USING incremental_poc.temp_table AS source
  ON target.id = source.id
  WHEN MATCHED THEN
    UPDATE SET target.id = source.id, target.name = source.name, target.updated_at = source.updated_at
  WHEN NOT MATCHED THEN
    INSERT (id, name, updated_at)
    VALUES (source.id, source.name, source.updated_at);
  
  -- Step 5: Clean up temporary table
  EXECUTE IMMEDIATE 'DROP TABLE IF EXISTS incremental_poc.temp_table';
END;