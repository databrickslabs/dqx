/* --title 'Data Quality Summary' */
SELECT 
    CASE
        WHEN _errors IS NOT NULL THEN 'Errors'
        WHEN _warnings IS NOT NULL THEN 'Warnings'
    END AS category,
    ROUND((COUNT(*) * 100.0 / (SELECT COUNT(*) FROM main.dqx_test.output_table)), 2) AS percentage
FROM main.dqx_test.output_table
GROUP BY category
