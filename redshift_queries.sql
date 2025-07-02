-- Write an SQL query to find the maximum number of times a core has been reused.
SELECT
  cores_core,
  COUNT(*) AS reuse_count
FROM your_table_name
WHERE cores_core IS NOT NULL
GROUP BY cores_core
ORDER BY reuse_count DESC
LIMIT 1;


-- Write an SQL query to find the cores that have been reused in less than 50 days after the previous launch.
WITH core_launches AS (
  SELECT
    cores_core,
    date_unix,
    ROW_NUMBER() OVER (PARTITION BY cores_core ORDER BY date_unix) AS rn
  FROM your_table_name
  WHERE cores_core IS NOT NULL
),
core_launches_with_prev AS (
  SELECT
    curr.cores_core,
    curr.date_unix AS curr_launch_unix,
    prev.date_unix AS prev_launch_unix,
    (curr.date_unix - prev.date_unix) / 86400.0 AS days_between
  FROM core_launches curr
  JOIN core_launches prev
    ON curr.cores_core = prev.cores_core
   AND curr.rn = prev.rn + 1
)
SELECT *
FROM core_launches_with_prev
WHERE days_between < 50;
