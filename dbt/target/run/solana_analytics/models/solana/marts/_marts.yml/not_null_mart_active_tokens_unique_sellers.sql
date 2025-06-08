
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select unique_sellers
from "solana_pipeline"."public_gold"."mart_active_tokens"
where unique_sellers is null



  
  
      
    ) dbt_internal_test