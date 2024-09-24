CREATE TABLE `your_project.your_dataset.data_quality_runs` (
  run_id STRING NOT NULL,
  start_time TIMESTAMP NOT NULL,
  end_time TIMESTAMP NOT NULL,
  status STRING NOT NULL,
  error_message STRING
);
