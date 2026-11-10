
select
    snapshot_date,
    type,
    extension,
    count(*) as duplicate_count

from {{ ref('mart_auction_daily_summary') }}

group by
    snapshot_date,
    type,
    extension

having count(*) > 1
