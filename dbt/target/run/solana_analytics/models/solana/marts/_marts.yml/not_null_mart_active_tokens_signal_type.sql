
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select signal_type
from "solana_pipeline"."public_gold"."mart_active_tokens"
where signal_type is null



  
  
      
    ) dbt_internal_test