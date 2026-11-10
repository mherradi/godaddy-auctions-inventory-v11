
{{
    config(
        materialized = 'table'
    )
}}

select
    snapshot_date,
    type,
    extension,

    -- Inventory metrics
    count(*) as total_listings,

    -- Price metrics
    sum(price) as total_price,
    avg(price) as average_price,
    approx_quantiles(price, 100)[offset(50)] as median_price,
    min(price) as minimum_price,
    max(price) as maximum_price,

    -- Valuation metrics
    countif(has_valuation) as listings_with_valuation,
    avg(valuation) as average_valuation,

    safe_divide(
        countif(has_valuation),
        count(*)
    ) as valuation_coverage_rate,

    avg(safe_divide(valuation, price)) as average_valuation_to_price_ratio,

    -- Bidding metrics
    countif(has_bids) as listings_with_bids,
    sum(bids) as total_bids,
    avg(bids) as average_bids_per_listing

from {{ ref('mart_auction_inventory') }}

group by
    snapshot_date,
    type,
    extension
