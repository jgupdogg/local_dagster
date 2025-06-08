
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

select
    signature as unique_field,
    count(*) as n_records

from "solana_pipeline"."silver"."helius_txns_clean"
where signature is not null
group by signature
having count(*) > 1



  
  
      
    ) dbt_internal_test