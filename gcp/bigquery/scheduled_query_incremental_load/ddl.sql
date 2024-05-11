-- after uplading csv to GCS, create external table
CREATE EXTERNAL TABLE infinite-rider-376814.incremental_poc.external_sample (
  id INT64,
  name STRING,
  updated_at TIMESTAMP
)
OPTIONS (
  format = 'CSV',
  uris = ['gs://external-data-test-six/external_table.csv'],
  skip_leading_rows = 1,
  field_delimiter=','
);

-- create the destination bigquery table

CREATE TABLE infinite-rider-376814.incremental_poc.bigquery_replica_table (
  id INT64,
  name STRING,
  updated_at TIMESTAMP
);

-- insert some of the external table records into the bigquery table

INSERT INTO incremental_poc.bigquery_replica_table (id, name, updated_at)
VALUES (1, 'John Doe', TIMESTAMP '2023-05-01 12:00:00'),
       (2, 'Jane Smith', TIMESTAMP '2023-05-02 10:30:00');