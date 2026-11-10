with source_data as (

    select *
    from {{ source('godaddy', 'auction_inventory') }}

),

renamed as (

    select
        domain_name,
        listing_url,
        auction_type,
        auction_end_time,
        price,
        valuation,
        monthly_parking_revenue,
        number_of_bids,
        domain_age,
        pageviews,
        is_adult,
        listing_id,
        domain_name_normalized,
        source_dataset,
        snapshot_date,
        ingested_at
    from source_data

)

select *
from renamed
