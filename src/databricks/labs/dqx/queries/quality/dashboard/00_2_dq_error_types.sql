/* --title 'Error and Warning Types Breakdown' */
WITH error_types AS (
    SELECT
        'Error' AS category,
        key AS type,
        COUNT(*) AS count
    FROM $catalog.schema.table
    LATERAL VIEW EXPLODE(MAP_KEYS(_errors)) exploded_errors AS key
    WHERE _errors IS NOT NULL
    GROUP BY key
),
warning_types AS (
    SELECT
        'Warning' AS category,
        key AS type,
        COUNT(*) AS count
    FROM $catalog.schema.table
    LATERAL VIEW EXPLODE(MAP_KEYS(_warnings)) exploded_warnings AS key
    WHERE _warnings IS NOT NULL
    GROUP BY key
),
combined AS (
    SELECT * FROM error_types
    UNION ALL
    SELECT * FROM warning_types
),
total AS (
    SELECT SUM(count) AS total_count FROM combined
)
SELECT
    category,
    type,
    count,
    ROUND((count * 100.0) / total.total_count, 2) AS percentage
FROM combined, total
ORDER BY category, count DESC;
