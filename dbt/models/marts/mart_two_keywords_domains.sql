
{{
    config(
        materialized = 'table'
    )
}}

select *
from {{ ref('mart_auction_inventory') }}
where length between 7 and 12
