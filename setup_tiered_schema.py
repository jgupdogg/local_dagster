#!/usr/bin/env python3
"""
Tiered Schema Setup Script for Solana Pipeline

This script:
1. Creates the bronze, silver, and gold schemas
2. Creates tables in their proper schemas
3. Drops any tables from 'public' schema if they exist
4. Reports on the schema structure
"""
import os
import sys
from dotenv import load_dotenv
from sqlalchemy import create_engine, text, inspect
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Add project root to path
project_root = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, project_root)

# Load environment variables
load_dotenv()

def setup_tiered_schema():
    """Set up the tiered schema structure with tables in the correct schemas"""
    print("\n" + "=" * 50)
    print("SOLANA PIPELINE TIERED SCHEMA SETUP")
    print("=" * 50)
    
    # Get connection details
    db_user = os.getenv("DB_USER", "postgres")
    db_password = os.getenv("DB_PASSWORD")
    db_host = os.getenv("DB_HOST", "localhost")
    db_port = os.getenv("DB_PORT", "5432")
    db_name = os.getenv("DB_NAME", "solana_pipeline")
    
    if not db_password:
        print("ERROR: DB_PASSWORD environment variable not set")
        return False
    
    print(f"Connecting to database: {db_name} on {db_host}:{db_port} as {db_user}")
    
    try:
        # Check if database exists, connect to postgres db
        postgres_url = f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/postgres"
        postgres_engine = create_engine(postgres_url)
        
        with postgres_engine.connect() as conn:
            # Check if database exists
            result = conn.execute(text(
                "SELECT 1 FROM pg_database WHERE datname = :db_name"
            ), {"db_name": db_name})
            
            if result.scalar() is None:
                print(f"Database '{db_name}' does not exist. Creating...")
                # Need to commit transactions before creating a database
                conn.execute(text("COMMIT"))
                conn.execute(text(f"CREATE DATABASE {db_name}"))
                print(f"Created database '{db_name}'")
            else:
                print(f"Database '{db_name}' already exists")
    
    except Exception as e:
        print(f"ERROR connecting to postgres database: {e}")
        return False
    
    # Now connect to our specific database
    db_url = f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
    
    try:
        # Create engine and connect
        engine = create_engine(db_url)
        
        # Test connection
        with engine.connect() as conn:
            result = conn.execute(text("SELECT 1")).scalar()
            if result == 1:
                print(f"Successfully connected to {db_name} database")
            else:
                print(f"Connection test returned unexpected result: {result}")
                return False
        
        # Step 1: Drop existing tables in public schema (clean slate)
        print("\nChecking for existing tables in public schema...")
        inspector = inspect(engine)
        public_tables = inspector.get_table_names(schema="public")
        
        if public_tables:
            print(f"Found tables in public schema: {public_tables}")
            print("Dropping tables from public schema...")
            
            with engine.connect() as conn:
                for table in public_tables:
                    conn.execute(text(f"DROP TABLE IF EXISTS public.{table} CASCADE"))
                    print(f"Dropped table public.{table}")
                
                # Commit the changes
                conn.execute(text("COMMIT"))
        else:
            print("No tables found in public schema")
            
        # Step 2: Create the three schemas
        print("\nCreating tiered schemas...")
        schemas = ["bronze", "silver", "gold"]
        
        with engine.connect() as conn:
            for schema in schemas:
                conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema}"))
                print(f"Created schema '{schema}' (if not exists)")
            
            # Commit the changes
            conn.execute(text("COMMIT"))
        
        # Step 3: Verify schemas
        with engine.connect() as conn:
            result = conn.execute(text(
                "SELECT schema_name FROM information_schema.schemata WHERE schema_name IN ('bronze', 'silver', 'gold')"
            ))
            existing_schemas = [row[0] for row in result]
            print(f"Verified schemas: {existing_schemas}")
            
            if set(schemas) != set(existing_schemas):
                missing = set(schemas) - set(existing_schemas)
                print(f"WARNING: Missing schemas: {missing}")
        
        # Step 4: Import and create tables in the correct schemas
        print("\nCreating tables in their respective schemas...")
        from solana_pipeline.models.solana_models import SQLModel, TrendingToken, TrackedToken, TokenWhale, WalletTradeHistory, WalletPnL, TopTrader, TokenListV3
        
        # Create all tables according to their __table_args__ schemas
        SQLModel.metadata.create_all(engine)
        
        # Step 5: Verify tables in the correct schemas
        print("\nVerifying tables in schemas...")
        all_schema_tables = {}
        
        for schema in schemas:
            tables = inspector.get_table_names(schema=schema)
            all_schema_tables[schema] = tables
            if tables:
                print(f"Tables in '{schema}' schema: {tables}")
            else:
                print(f"WARNING: No tables found in '{schema}' schema")
        
        # Step 6: Print expected table locations
        print("\nExpected table locations:")
        expected_locations = {
            "bronze": ["trending_tokens", "token_whales", "wallet_trade_history", "token_list_v3"],
            "silver": ["tracked_tokens", "wallet_pnl"],
            "gold": ["top_traders", "alpha_signal", "active_token_signals"]
        }
        
        for schema, tables in expected_locations.items():
            print(f"{schema}: {', '.join(tables)}")
        
        # Step 7: Compare expected vs. actual
        print("\nVerification results:")
        all_pass = True
        
        for schema, expected_tables in expected_locations.items():
            actual_tables = all_schema_tables.get(schema, [])
            
            for table in expected_tables:
                if table in actual_tables:
                    print(f"✅ {schema}.{table}: Found")
                else:
                    print(f"❌ {schema}.{table}: Not found")
                    all_pass = False
        
        return all_pass
        
    except Exception as e:
        print(f"ERROR: {e}")
        return False

if __name__ == "__main__":
    success = setup_tiered_schema()
    if success:
        print("\n✅ Tiered schema setup completed successfully!")
        print("Your database now has bronze, silver, and gold schemas with tables in the correct tiers.")
        sys.exit(0)
    else:
        print("\n❌ Tiered schema setup failed!")
        sys.exit(1)