/* --title 'Error and Warning Types Breakdown' */
WITH error_types AS (
    SELECT
        'Error' AS category,
        error_struct.name AS type,
        COUNT(*) AS count
    FROM $catalog.schema.table
    LATERAL VIEW EXPLODE(_errors) exploded_errors AS error_struct
    WHERE _errors IS NOT NULL
    GROUP BY error_struct.name
),
warning_types AS (
    SELECT
        'Warning' AS category,
        warning_struct.name AS type,
        COUNT(*) AS count
    FROM $catalog.schema.table
    LATERAL VIEW EXPLODE(_warnings) exploded_warnings AS warning_struct
    WHERE _warnings IS NOT NULL
    GROUP BY warning_struct.name
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