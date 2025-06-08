
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select score
from "solana_pipeline"."public_gold"."mart_active_tokens"
where score is null



  
  
      
    ) dbt_internal_test