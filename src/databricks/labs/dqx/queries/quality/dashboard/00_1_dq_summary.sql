/* --title 'Data Quality Summary' */
SELECT 
    case when _errors IS NOT NULL then 'Errors' when _warnings is not null then 'Warnings' else 'Good' end AS category,
    (COUNT(*) * 100.0 / (SELECT COUNT(*) FROM $catalog.schema.table)) AS percentage
FROM $catalog.schema.table
GROUP BY category
