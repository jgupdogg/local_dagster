
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select timestamp
from "solana_pipeline"."silver"."helius_txns_clean"
where timestamp is null



  
  
      
    ) dbt_internal_test