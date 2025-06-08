
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

select
    token_address as unique_field,
    count(*) as n_records

from "solana_pipeline"."public_gold"."mart_active_tokens"
where token_address is not null
group by token_address
having count(*) > 1



  
  
      
    ) dbt_internal_test