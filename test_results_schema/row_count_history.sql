CREATE TABLE `your_project.your_dataset.row_count_history` (
  run_id STRING NOT NULL,
  dataset STRING NOT NULL,
  table STRING NOT NULL,
  group_by_columns STRING NOT NULL,
  group_values STRING NOT NULL,
  row_count INT64 NOT NULL,
  filter_condition STRING,
  timestamp TIMESTAMP NOT NULL
);
