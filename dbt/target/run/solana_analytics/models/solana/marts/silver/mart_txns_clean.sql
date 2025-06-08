
      
        
        
        delete from "solana_pipeline"."public_silver"."mart_txns_clean" as DBT_INTERNAL_DEST
        where (signature) in (
            select distinct signature
            from "mart_txns_clean__dbt_tmp235530748855" as DBT_INTERNAL_SOURCE
        );

    

    insert into "solana_pipeline"."public_silver"."mart_txns_clean" ("signature", "raw_id", "user_address", "swapfromtoken", "swapfromamount", "swaptotoken", "swaptoamount", "source", "timestamp", "processed", "notification_sent", "created_at", "updated_at")
    (
        select "signature", "raw_id", "user_address", "swapfromtoken", "swapfromamount", "swaptotoken", "swaptoamount", "source", "timestamp", "processed", "notification_sent", "created_at", "updated_at"
        from "mart_txns_clean__dbt_tmp235530748855"
    )
  