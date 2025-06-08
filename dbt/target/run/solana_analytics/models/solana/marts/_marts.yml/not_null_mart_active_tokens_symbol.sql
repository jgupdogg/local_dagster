
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select symbol
from "solana_pipeline"."public_gold"."mart_active_tokens"
where symbol is null



  
  
      
    ) dbt_internal_test