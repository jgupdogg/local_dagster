
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select token_address
from "solana_pipeline"."public_gold"."mart_active_tokens"
where token_address is null



  
  
      
    ) dbt_internal_test