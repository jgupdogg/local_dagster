
    
    



select dbt_updated_at
from "solana_pipeline"."public_gold"."mart_active_tokens"
where dbt_updated_at is null


