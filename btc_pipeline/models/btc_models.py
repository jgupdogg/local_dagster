"""SQLModel definitions for Bitcoin Pipeline with data validation."""
from sqlmodel import SQLModel, Field, create_engine
import os
from dotenv import load_dotenv
from sqlalchemy import Column, Integer, String, DateTime, Float, Numeric, Boolean, Index, UniqueConstraint, text
from datetime import datetime
from typing import Optional
from decimal import Decimal

# Load environment variables
load_dotenv()


class FearGreedIndex(SQLModel, table=True):
    """Model for Bitcoin Fear & Greed Index data."""
    __tablename__ = 'fear_greed_index'
    
    id: Optional[int] = Field(default=None, primary_key=True)
    # Timestamp with unique constraint for duplicate prevention
    timestamp: datetime = Field(sa_column=Column(DateTime, nullable=False, unique=True, index=True))
    value: int = Field(sa_column=Column(Integer, nullable=False))
    classification: str = Field(sa_column=Column(String(50), nullable=False))
    api_url: Optional[str] = Field(default=None, max_length=500)
    
    # Server-side timestamps for consistency
    created_at: datetime = Field(
        sa_column=Column(DateTime, nullable=False, server_default=text("CURRENT_TIMESTAMP"))
    )
    updated_at: Optional[datetime] = Field(
        sa_column=Column(DateTime, server_default=text("CURRENT_TIMESTAMP"), 
                         onupdate=datetime.utcnow)
    )
    
    __table_args__ = (
        Index('idx_fear_greed_timestamp_value', 'timestamp', 'value'),
        {"schema": "bronze"}
    )


class BitcoinDerivativesMetrics(SQLModel, table=True):
    """Model for aggregated Bitcoin derivatives metrics."""
    __tablename__ = "bitcoin_derivatives_metrics"
    
    id: Optional[int] = Field(default=None, primary_key=True)
    timestamp: datetime = Field(sa_column=Column(DateTime(timezone=True), nullable=False, index=True))
    exchange: str = Field(sa_column=Column(String(100), nullable=False))
    
    # Use Decimal type hints for financial precision
    open_interest_usd: Optional[Decimal] = Field(default=None, sa_column=Column(Numeric(20,2)))
    volume_24h: Optional[Decimal] = Field(default=None, sa_column=Column(Numeric(20,2)))
    funding_rate: Optional[Decimal] = Field(default=None, sa_column=Column(Numeric(10,6)))
    liquidations_usd: Optional[Decimal] = Field(default=None, sa_column=Column(Numeric(20,2)))
    long_short_ratio: Optional[Decimal] = Field(default=None, sa_column=Column(Numeric(5,2)))
    
    api_url: Optional[str] = Field(default=None, max_length=500)
    
    # Server-side timestamps
    created_at: datetime = Field(
        sa_column=Column(DateTime, nullable=False, server_default=text("CURRENT_TIMESTAMP"))
    )
    updated_at: Optional[datetime] = Field(
        sa_column=Column(DateTime, server_default=text("CURRENT_TIMESTAMP"), 
                         onupdate=datetime.utcnow)
    )
    
    __table_args__ = (
        # Unique constraint for duplicate prevention with ON CONFLICT
        UniqueConstraint('timestamp', 'exchange', name='uq_btc_deriv_timestamp_exchange'),
        Index("idx_btc_deriv_ts_ex", "timestamp", "exchange"),
        {"schema": "bronze"}
    )


def create_tables():
    """Create database tables if they don't exist."""
    # Build connection string from environment variables
    db_user = os.getenv("DB_USER", "postgres")
    db_password = os.getenv("DB_PASSWORD")
    db_host = os.getenv("DB_HOST", "localhost")
    db_port = os.getenv("DB_PORT", "5432")
    db_name = "btc_pipeline"
    
    connection_string = f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
    
    # Create engine
    engine = create_engine(connection_string)
    
    # Create schemas first
    with engine.connect() as conn:
        from sqlalchemy import text
        # Create schemas if they don't exist
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS bronze"))
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS silver"))
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS gold"))
        conn.commit()
        print("Created schemas (bronze, silver, gold)")
    
    # Create all tables
    SQLModel.metadata.create_all(engine)
    
    print("Database tables created successfully")


if __name__ == "__main__":
    create_tables()