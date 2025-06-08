
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select payload
from "solana_pipeline"."bronze"."webhook_staging"
where payload is null



  
  
      
    ) dbt_internal_test