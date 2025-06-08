
    
    

with all_values as (

    select
        signal_type as value_field,
        count(*) as n_records

    from "solana_pipeline"."public_gold"."mart_active_tokens"
    group by signal_type

)

select *
from all_values
where value_field not in (
    'most_unique_buyers','most_unique_sellers','most_net_buyers'
)


