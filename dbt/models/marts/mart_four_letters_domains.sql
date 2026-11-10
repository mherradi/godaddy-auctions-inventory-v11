
{{
    config(
        materialized = 'table'
    )
}}

select *
from {{ ref('mart_auction_inventory') }}
where length = 4
