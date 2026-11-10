
{{
    config(
        materialized = 'table'
    )
}}

select
    listing_id,
    domain,
    url,
    type,
    end_time,
    price,
    valuation,
    bids,
    age,
    pageviews,
    is_adult,
    domain_normalized,
    source,
    snapshot_date,
    ingested_at,
    name,
    extension,
    length,
    sayable,
    keyword_in,
    pattern,
    pure_letter,
    with_num,
    hyphenated,
    has_valuation,
    has_bids,
    ending_today

from {{ ref('int_auction_inventory') }}
