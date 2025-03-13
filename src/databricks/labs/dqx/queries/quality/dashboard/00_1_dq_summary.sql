/* --title 'Data Quality Summary' */
SELECT 
    CASE
        WHEN _errors IS NOT NULL THEN 'Errors'
        WHEN _warnings IS NOT NULL THEN 'Warnings'
    END AS category,
    ROUND((COUNT(*) * 100.0 / (SELECT COUNT(*) FROM $catalog.schema.table)), 2) AS percentage
FROM $catalog.schema.table
GROUP BY category
