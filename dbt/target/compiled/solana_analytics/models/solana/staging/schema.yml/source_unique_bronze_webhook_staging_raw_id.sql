
    
    

select
    raw_id as unique_field,
    count(*) as n_records

from "solana_pipeline"."bronze"."webhook_staging"
where raw_id is not null
group by raw_id
having count(*) > 1


