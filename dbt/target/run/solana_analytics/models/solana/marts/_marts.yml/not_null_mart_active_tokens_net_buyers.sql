
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select net_buyers
from "solana_pipeline"."public_gold"."mart_active_tokens"
where net_buyers is null



  
  
      
    ) dbt_internal_test