-- Create and populate the transactions table
CREATE OR REPLACE TABLE `your_project.test_dataset.transactions` AS
WITH
  base_data AS (
    SELECT
      GENERATE_UUID() AS transaction_id,
      IF(RAND() < 0.05, NULL, CONCAT('user_', CAST(CAST(1000 * RAND() AS INT64) AS STRING))) AS user_id,
      IF(RAND() < 0.1, -ABS(ROUND(100 * RAND(), 2)), ROUND(100 * RAND(), 2)) AS amount,
      IF(RAND() < 0.05, 'INVALID', 'USD') AS currency,
      IF(RAND() < 0.1, 'failed', 'completed') AS status,
      DATE_SUB(CURRENT_DATE(), INTERVAL CAST(100 * RAND() AS INT64) DAY) AS transaction_date,
      CASE WHEN RAND() < 0.5 THEN 'North America' ELSE 'Europe' END AS region
    FROM
      UNNEST(GENERATE_ARRAY(1, 10000)) AS x
  ),
  duplicate_data AS (
    SELECT
      transaction_id,
      user_id,
      amount,
      currency,
      status,
      transaction_date,
      region
    FROM
      base_data
    WHERE
      RAND() < 0.05  -- 5% duplicates
  )
SELECT * FROM base_data
UNION ALL
SELECT * FROM duplicate_data;
