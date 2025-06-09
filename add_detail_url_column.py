"""
Add detail_url column to existing silver table
"""
from common.resources.database import DatabaseResource

def add_detail_url_column():
    """Add detail_url column to existing silver table"""
    from sqlalchemy import create_engine, text
    
    # Use same config as engineering pipeline
    DATABASE_URL = "postgresql://postgres:St0ck!adePG@localhost:5432/engineering"
    engine = create_engine(DATABASE_URL)
    
    try:        
        with engine.connect() as conn:
            # Add the detail_url column
            conn.execute(text("ALTER TABLE silver.emma_public_solicitations_silver ADD COLUMN IF NOT EXISTS detail_url TEXT;"))
            conn.commit()
            print("✅ Successfully added detail_url column")
            
            # Verify the column was added
            result = conn.execute(text("""
                SELECT column_name, data_type 
                FROM information_schema.columns 
                WHERE table_schema = 'silver' 
                AND table_name = 'emma_public_solicitations_silver'
                ORDER BY ordinal_position;
            """))
            
            print("\nCurrent table structure:")
            for row in result:
                print(f"  {row.column_name}: {row.data_type}")
    
    except Exception as e:
        print(f"Error: {e}")
    finally:
        engine.dispose()

if __name__ == "__main__":
    add_detail_url_column()