
    
    

select
    token_address as unique_field,
    count(*) as n_records

from "solana_pipeline"."public_gold"."mart_active_tokens"
where token_address is not null
group by token_address
having count(*) > 1


