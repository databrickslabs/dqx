with source_data as (
    select 1 as id
    union all
    select 1 as id
    union all
    select NULL as id
    union all
    select 3 as id
)
select *
from source_data
