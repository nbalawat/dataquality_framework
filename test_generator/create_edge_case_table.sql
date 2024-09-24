-- Create empty_table
CREATE OR REPLACE TABLE `your_project.test_dataset.empty_table` AS
SELECT * FROM `your_project.test_dataset.transactions` WHERE 1=0;

-- Create all_nulls table
CREATE OR REPLACE TABLE `your_project.test_dataset.all_nulls` AS
SELECT
  NULL AS column1,
  NULL AS column2,
  NULL AS column3
FROM
  UNNEST(GENERATE_ARRAY(1, 100)) AS x;

-- Create all_duplicates table
CREATE OR REPLACE TABLE `your_project.test_dataset.all_duplicates` AS
SELECT
  'duplicate_value' AS column1,
  'duplicate_value' AS column2,
  'duplicate_value' AS column3
FROM
  UNNEST(GENERATE_ARRAY(1, 100)) AS x;
