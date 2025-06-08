
    
    

select
    signature as unique_field,
    count(*) as n_records

from "solana_pipeline"."silver"."helius_txns_clean"
where signature is not null
group by signature
having count(*) > 1


