
with source_data as (

    select
        listing_id,
        domain_name as domain,
        listing_url as url,
        auction_type as type,
        auction_end_time as end_time,
        price,
        valuation,
        number_of_bids as bids,
        domain_age as age,
        pageviews,
        is_adult,
        domain_name_normalized as domain_normalized,
        source_dataset as source,
        snapshot_date,
        ingested_at

    from {{ ref('stg_auction_inventory') }}

),

-- Match domains against the suffix reference table.
suffix_matches as (

    select
        d.*,
        s.suffix as matched_suffix,

        row_number() over (
            partition by d.listing_id
            order by
                length(s.suffix) desc,
                s.suffix
        ) as suffix_rank

    from source_data as d

    left join {{ ref('suffixes') }} as s
        on ends_with(
            lower(d.domain_normalized),
            lower(s.suffix)
        )

),

-- Keep only the longest matching suffix.
best_suffix as (

    select
        * except (suffix_rank)
    from suffix_matches
    where suffix_rank = 1

),

-- Extract the domain name and extension.
domain_features as (

    select
        * except (matched_suffix),

        coalesce(
            matched_suffix,
            regexp_extract(domain_normalized, r'(\.[^.]+)$')
        ) as extension,

        case
            when matched_suffix is not null then
                substr(
                    domain_normalized,
                    1,
                    length(domain_normalized) - length(matched_suffix)
                )

            when regexp_extract(
                domain_normalized,
                r'(\.[^.]+)$'
            ) is not null then
                regexp_replace(
                    domain_normalized,
                    r'\.[^.]+$',
                    ''
                )

            else domain_normalized
        end as name

    from best_suffix

),

-- Match domain names against the keyword reference table.
keyword_matches as (

    select
        d.listing_id,

        count(k.keyword) > 0 as keyword_in

    from domain_features as d

    left join {{ ref('keywords') }} as k
        on strpos(
            lower(d.name),
            lower(k.keyword)
        ) > 0

    group by d.listing_id

),

-- Calculate the final domain features.
derived_features as (

    select
        d.listing_id,
        d.domain,
        d.url,
        d.type,
        d.end_time,
        d.price,
        d.valuation,
        d.bids,
        d.age,
        d.pageviews,
        d.is_adult,
        d.domain_normalized,
        d.source,
        d.snapshot_date,
        d.ingested_at,

        d.name,
        d.extension,
        length(d.name) as length,

        -- Pronounceability heuristic.
        (
            regexp_contains(d.name, r'^[a-z]+$')
            and regexp_contains(d.name, r'[aeiouy]')
            and not regexp_contains(d.name, r'[^aeiouy]{4,}')
        ) as sayable,

        coalesce(k.keyword_in, false) as keyword_in,

        -- Naming patterns to be implemented later.
        cast([] as array<string>) as pattern,

        -- Domain-characteristic flags.
        regexp_contains(d.name, r'^[a-z]+$') as pure_letter,
        regexp_contains(d.name, r'[0-9]') as with_num,
        regexp_contains(d.name, r'-') as hyphenated,

        -- Business flags.
        d.valuation is not null as has_valuation,
        coalesce(d.bids, 0) > 0 as has_bids,

        (
        DATE(d.end_time, 'UTC') = CURRENT_DATE('UTC')
        AND d.end_time > CURRENT_TIMESTAMP()
        ) AS ending_today

    from domain_features as d

    left join keyword_matches as k
        on d.listing_id = k.listing_id

)

select *
from derived_features
