-- Create and populate the sales table
CREATE OR REPLACE TABLE `your_project.test_dataset.sales` AS
WITH
  date_range AS (
    SELECT
      DATE_SUB(CURRENT_DATE(), INTERVAL days DAY) AS sale_date
    FROM
      UNNEST(GENERATE_ARRAY(0, 59)) AS days
  ),
  base_data AS (
    SELECT
      GENERATE_UUID() AS sale_id,
      CONCAT('product_', CAST(CAST(100 * RAND() AS INT64) AS STRING)) AS product_id,
      CAST(1 + RAND() * 10 AS INT64) AS quantity,
      d.sale_date,
      CONCAT('store_', CAST(CAST(10 * RAND() AS INT64) AS STRING)) AS store_id,
      CASE
        WHEN RAND() < 0.5 THEN 'East'
        ELSE 'West'
      END AS region
    FROM
      date_range AS d,
      UNNEST(GENERATE_ARRAY(1, 100 + CAST(50 * SIN(PI() * CAST(EXTRACT(DAY FROM d.sale_date) AS FLOAT64) / 15) AS INT64))) AS x
  ),
  anomaly_data AS (
    SELECT
      GENERATE_UUID() AS sale_id,
      CONCAT('product_', CAST(CAST(100 * RAND() AS INT64) AS STRING)) AS product_id,
      CAST(1 + RAND() * 10 AS INT64) AS quantity,
      DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY) AS sale_date,
      CONCAT('store_', CAST(CAST(10 * RAND() AS INT64) AS STRING)) AS store_id,
      'East' AS region
    FROM
      UNNEST(GENERATE_ARRAY(1, 500)) AS x  -- Increase sales significantly to simulate anomaly
  )
SELECT * FROM base_data
UNION ALL
SELECT * FROM anomaly_data;
