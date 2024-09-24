-- Create and populate the users table
CREATE OR REPLACE TABLE `your_project.test_dataset.users` AS
WITH
  base_data AS (
    SELECT
      CONCAT('user_', CAST(x AS STRING)) AS user_id,
      IF(RAND() < 0.05, NULL, CONCAT('user', x, '@example.com')) AS email,
      DATE_SUB(CURRENT_DATE(), INTERVAL CAST(1000 * RAND() AS INT64) DAY) AS signup_date,
      CASE
        WHEN RAND() < 0.05 THEN 'inactive'
        WHEN RAND() < 0.1 THEN 'pending'
        ELSE 'active'
      END AS status,
      CASE
        WHEN RAND() < 0.3 THEN 'US'
        WHEN RAND() < 0.6 THEN 'GB'
        ELSE 'CA'
      END AS country
    FROM
      UNNEST(GENERATE_ARRAY(1, 5000)) AS x
  ),
  duplicate_emails AS (
    SELECT
      user_id,
      email,
      signup_date,
      status,
      country
    FROM
      base_data
    WHERE
      RAND() < 0.05  -- 5% duplicate emails
  )
SELECT * FROM base_data
UNION ALL
SELECT * FROM duplicate_emails;
