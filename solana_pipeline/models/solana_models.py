"""SQLModel definitions for Solana Pipeline."""
from sqlmodel import SQLModel, Field, create_engine
from datetime import datetime
import os
from dotenv import load_dotenv
from sqlmodel import SQLModel, Field, Column, JSON
from sqlalchemy import String, Numeric, DateTime, JSON, Text, ARRAY
from typing import Optional, List, Dict, Any

# Load environment variables
load_dotenv()


class TrendingToken(SQLModel, table=True):
    """Bronze tier storage of raw token data with state tracking."""
    __tablename__ = "trending_tokens"
    __table_args__ = {"schema": "bronze"}  # Store in bronze schema
    
    id: Optional[int] = Field(default=None, primary_key=True)
    token_address: str = Field(index=True)
    symbol: Optional[str] = None
    name: Optional[str] = None
    decimals: Optional[int] = None
    liquidity: Optional[float] = None
    logoURI: Optional[str] = None
    volume24hUSD: Optional[float] = None
    volume24hChangePercent: Optional[float] = None
    rank: Optional[int] = None
    price: Optional[float] = None
    price24hChangePercent: Optional[float] = None
    fdv: Optional[float] = None  # Fully Diluted Valuation
    marketcap: Optional[float] = None
    
    # Additional metadata fields
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)
    
    # State tracking fields
    tracked: Optional[bool] = None  # NULL initially, then True/False
    processed_at: Optional[datetime] = None  # NULL initially, then timestamp when processed
    processing_status: Optional[str] = None  # Can be 'pending', 'processing', 'completed', 'failed'


class TokenListV3(SQLModel, table=True):
    """Bronze tier storage of token data from v3 API with state tracking."""
    __tablename__ = "token_list_v3"
    __table_args__ = {"schema": "bronze"}  # Store in bronze schema
    
    id: Optional[int] = Field(default=None, primary_key=True)
    
    # Basic token info
    token_address: str = Field(index=True, sa_column_kwargs={"name": "address"})
    symbol: Optional[str] = None
    name: Optional[str] = None
    decimals: Optional[int] = None
    logo_uri: Optional[str] = None
    
    # Market data
    market_cap: Optional[float] = None
    fdv: Optional[float] = None  # Fully Diluted Valuation
    liquidity: Optional[float] = None
    last_trade_unix_time: Optional[int] = None
    holder: Optional[int] = None
    recent_listing_time: Optional[int] = None
    
    # Price data
    price: Optional[float] = None
    price_change_1h_percent: Optional[float] = None
    price_change_2h_percent: Optional[float] = None
    price_change_4h_percent: Optional[float] = None
    price_change_8h_percent: Optional[float] = None
    price_change_24h_percent: Optional[float] = None
    
    # Volume data for different time periods
    volume_1h_usd: Optional[float] = None
    volume_2h_usd: Optional[float] = None
    volume_4h_usd: Optional[float] = None
    volume_8h_usd: Optional[float] = None
    volume_24h_usd: Optional[float] = None
    
    # Volume change percentages
    volume_1h_change_percent: Optional[float] = None
    volume_2h_change_percent: Optional[float] = None
    volume_4h_change_percent: Optional[float] = None
    volume_8h_change_percent: Optional[float] = None
    volume_24h_change_percent: Optional[float] = None
    
    # Trade counts for different time periods
    trade_1h_count: Optional[int] = None
    trade_2h_count: Optional[int] = None
    trade_4h_count: Optional[int] = None
    trade_8h_count: Optional[int] = None
    trade_24h_count: Optional[int] = None
    
    # Extensions (stored as JSON)
    extensions: Optional[Dict[str, Any]] = Field(default=None, sa_column=Column(JSON))
    
    # Additional metadata fields
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)
    
    # State tracking fields (same as TrendingToken)
    tracked: Optional[bool] = None  # NULL initially, then True/False
    processed_at: Optional[datetime] = None  # NULL initially, then timestamp when processed
    processing_status: Optional[str] = None  # Can be 'pending', 'processing', 'completed', 'failed'
    
    
class TrackedToken(SQLModel, table=True):
    """Silver tier storage of filtered token data."""
    __tablename__ = "tracked_tokens"
    __table_args__ = {"schema": "silver"}  # Store in silver schema
    
    id: Optional[int] = Field(default=None, primary_key=True)
    token_address: str = Field(index=True)
    symbol: Optional[str] = None
    name: Optional[str] = None
    decimals: Optional[int] = None
    liquidity: Optional[float] = None
    logoURI: Optional[str] = None
    volume24hUSD: Optional[float] = None
    volume24hChangePercent: Optional[float] = None
    rank: Optional[int] = None
    price: Optional[float] = None
    price24hChangePercent: Optional[float] = None
    fdv: Optional[float] = None
    marketcap: Optional[float] = None
    volume_mcap_ratio: Optional[float] = None
    quality_score: Optional[float] = None
    
    # Bronze reference
    bronze_id: Optional[int] = None
    
    # Tracking fields
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)
    
    # Processing tracking timestamps
    latest_metadata: Optional[datetime] = None
    latest_security: Optional[datetime] = None
    latest_social_score: Optional[datetime] = None
    latest_whales: Optional[datetime] = None
    latest_pnl: Optional[datetime] = None
    
    
class TokenWhale(SQLModel, table=True):
    """Bronze tier storage of token whales."""
    __tablename__ = "token_whales"
    # Define schema correctly
    __table_args__ = {"schema": "bronze"}  # Store in bronze schema
    
    id: Optional[int] = Field(default=None, primary_key=True)
    token_address: str = Field(index=True)
    wallet_address: str = Field(index=True)
    holdings_amount: Optional[float] = None
    holdings_value: Optional[float] = None
    holdings_pct: Optional[float] = None
    last_transaction_date: Optional[datetime] = None
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)
    # Add a flag to track whether transactions have been fetched
    fetched_txns: Optional[datetime] = None

class WalletTradeHistory(SQLModel, table=True):
    """Bronze tier storage of transaction history with PnL processing state."""
    __tablename__ = "wallet_trade_history"
    __table_args__ = {"schema": "bronze"}  # Store in bronze schema
    
    id: Optional[int] = Field(default=None, primary_key=True)
    wallet_address: str = Field(index=True)
    token_address: str = Field(index=True)  # The token we're tracking
    
    # Transaction metadata
    transaction_hash: str = Field(index=True)
    source: str  # e.g., "raydium", "meteora_dlmm"
    block_unix_time: int
    tx_type: str  # e.g., "swap"
    timestamp: datetime = Field(index=True)
    
    # Transaction direction (BUY/SELL relative to the token we care about)
    transaction_type: str
    
    # "From" token information
    from_symbol: str
    from_address: str
    from_decimals: Optional[int] = None
    from_amount: Optional[float] = None  # Using ui_amount (human-readable)
    from_raw_amount: Optional[str] = None  # Original amount value (for precision)
    
    # "To" token information
    to_symbol: str
    to_address: str
    to_decimals: Optional[int] = None
    to_amount: Optional[float] = None  # Using ui_amount (human-readable)
    to_raw_amount: Optional[str] = None  # Original amount value (for precision)
    
    # Price information
    base_price: Optional[float] = None
    quote_price: Optional[float] = None
    
    # Calculated fields
    value_usd: Optional[float] = None  # Value of the transaction in USD
    
    # Metadata
    created_at: datetime = Field(default_factory=datetime.utcnow)
    
    # State tracking fields
    processed_for_pnl: Optional[bool] = None  # NULL initially, then True/False
    processed_at: Optional[datetime] = None  # When this transaction was processed for PnL
    processing_status: Optional[str] = None  # Can be 'pending', 'processing', 'completed', 'failed'
    
    

class WalletPnL(SQLModel, table=True):
    """Silver tier storage of wallet PnL calculations with additional metadata."""
    __tablename__ = "wallet_pnl"
    __table_args__ = {"schema": "silver"}  # Store in silver schema
    
    id: Optional[int] = Field(default=None, primary_key=True)
    wallet_address: str = Field(index=True)
    token_address: str = Field(index=True)  # Added to track PnL per token
    time_period: str  # e.g., "all", "week", "month"
    
    # PnL metrics
    realized_pnl: Optional[float] = None
    unrealized_pnl: Optional[float] = None
    total_pnl: Optional[float] = None
    win_rate: Optional[float] = None
    trade_count: Optional[int] = None
    avg_holding_time: Optional[float] = None  # in hours
    total_bought: Optional[float] = None  # Total amount bought in USD
    total_sold: Optional[float] = None  # Total amount sold in USD
    roi: Optional[float] = None
    
    # Added trade frequency metric
    trade_frequency_daily: Optional[float] = None  # Trades per day
    
    # Time references
    first_transaction: Optional[datetime] = None  # First transaction in the analysis period
    last_transaction: Optional[datetime] = None  # Last transaction in the analysis period
    
    # Top trader flag
    top_trader: bool = Field(default=False)
    
    # Timestamps for tracking
    created_at: datetime = Field(default_factory=datetime.utcnow)
    processed_at: datetime = Field(default_factory=datetime.utcnow)  # Renamed from updated_at
    
    # Bronze reference
    bronze_whale_id: Optional[int] = None  # Reference to the TokenWhale record


class TopTrader(SQLModel, table=True):
    """Gold tier storage of top traders with key performance metrics."""
    __tablename__ = "top_traders"
    __table_args__ = {"schema": "gold"}  # Store in gold schema
    
    id: Optional[int] = Field(default=None, primary_key=True)
    wallet_address: str = Field(index=True)
    time_period: str  # e.g., "all", "week", "month"
    rank: int = Field(index=True)  # Ranking position
    
    # PnL metrics
    realized_pnl: Optional[float] = None
    unrealized_pnl: Optional[float] = None
    total_pnl: Optional[float] = None
    win_rate: Optional[float] = None
    trade_count: Optional[int] = None
    avg_holding_time: Optional[float] = None  # in hours
    total_bought: Optional[float] = None
    total_sold: Optional[float] = None
    roi: Optional[float] = None
    
    # Added trade frequency metric
    trade_frequency_daily: Optional[float] = None  # Trades per day
    
    # Time references  
    first_transaction: Optional[datetime] = None
    last_transaction: Optional[datetime] = None
    
    # Tags for categorization (stored as JSON)
    tags: Optional[List[str]] = Field(default=None, sa_column=Column(JSON))
    
    # Webhook tracking
    added_to_webhook: bool = Field(default=False)
    added_at: Optional[datetime] = None
    
    # Timestamp
    created_at: datetime = Field(default_factory=datetime.utcnow)
    
    # Silver references
    silver_pnl_id: Optional[int] = None
    
    



# Bronze tier models
class HeliusHook(SQLModel, table=True):
    """Bronze tier storage of raw webhook data from Helius."""
    __tablename__ = "helius_hook"
    __table_args__ = {"schema": "bronze"}
    
    id: Optional[int] = Field(default=None, primary_key=True)
    # Changed from JSON to Text to match DB
    payload: str = Field(sa_column=Column(Text))
    received_at: datetime = Field(default_factory=datetime.utcnow)
    processed: bool = Field(default=False)
    # Added missing field
    processed_at: Optional[datetime] = Field(default=None)


# Silver tier models
class HeliusTxnClean(SQLModel, table=True):
    """Silver tier storage of processed transaction data."""
    __tablename__ = "helius_txns_clean"
    __table_args__ = {"schema": "silver"}
    
    # No ID field - removed as it's not in the DB
    # Primary key is signature instead
    signature: str = Field(primary_key=True)
    
    # Required fields
    raw_id: Optional[int] = Field(default=None)
    user_address: str
    swapfromtoken: str
    swapfromamount: float = Field(sa_column=Column(Numeric))
    swaptotoken: str
    swaptoamount: float = Field(sa_column=Column(Numeric))
    source: Optional[str] = Field(default=None)
    timestamp: datetime
    processed: bool = Field(default=False)
    notification_sent: bool = Field(default=False)
    


class AlphaSignal(SQLModel, table=True):
    """Gold tier storage for detected alpha signals from tracked traders."""
    __tablename__ = "alpha_signals"
    __table_args__ = {"schema": "gold"}  # Store in gold schema
    
    id: Optional[int] = Field(default=None, primary_key=True)
    
    # Token information
    token_address: str = Field(index=True)
    symbol: str
    name: Optional[str] = None
    price: Optional[float] = None
    price_change_24h: Optional[float] = None
    
    # Signal classification
    signal_type: str  # "initial_accumulation", "strong_accumulation", "distribution"
    confidence: float  # 0-100 scale
    
    # Trader metrics
    alpha_tier_count: int  # Number of alpha tier wallets involved
    solid_tier_count: int  # Number of solid tier wallets involved
    tracking_tier_count: int  # Number of tracking tier wallets involved
    total_wallet_count: int  # Total wallets involved
    
    # Trader wallets (store as JSON arrays)
    alpha_wallets: Optional[List[str]] = Field(default=None, sa_column=Column(JSON))
    solid_wallets: Optional[List[str]] = Field(default=None, sa_column=Column(JSON))
    tracking_wallets: Optional[List[str]] = Field(default=None, sa_column=Column(JSON))
    
    # Activity metrics
    total_volume: Optional[float] = None  # Total volume in USD
    avg_position_size: Optional[float] = None  # Avg position size relative to wallet typical size
    weighted_score: Optional[float] = None  # Weighted activity score based on wallet tiers
    
    # Time windows and detection info
    window_hours: int  # Which time window triggered this (10, 20, 60, 1200 for testing)
    first_transaction_time: datetime  # Timestamp of first relevant transaction
    latest_transaction_time: datetime  # Timestamp of most recent relevant transaction
    detected_at: datetime = Field(default_factory=datetime.utcnow)  # When signal was detected
    
    # Notification status
    notification_sent: bool = Field(default=False)
    notification_sent_at: Optional[datetime] = None
    
    # Signal status tracking
    validated: Optional[bool] = None  # Did price move as expected after signal
    price_at_signal: Optional[float] = None
    max_price_after_signal: Optional[float] = None  # For tracking performance
    min_price_after_signal: Optional[float] = None  # For tracking performance
    price_24h_after_signal: Optional[float] = None
    
    # Metadata
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)
    
    


def create_tables():
    """Create database tables if they don't exist."""
    # Build connection string from environment variables
    db_user = os.getenv("DB_USER", "postgres")
    db_password = os.getenv("DB_PASSWORD")
    db_host = os.getenv("DB_HOST", "localhost")
    db_port = os.getenv("DB_PORT", "5432")
    db_name = os.getenv("DB_NAME", "solana_pipeline")
    
    connection_string = f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
    
    # Create engine
    engine = create_engine(connection_string)
    
    # Create schemas first
    with engine.connect() as conn:
        # Create schemas if they don't exist
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS bronze"))
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS silver"))
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS gold"))
        print("Created schemas (bronze, silver, gold)")
    
    # Create all tables
    SQLModel.metadata.create_all(engine)
    
    print("Database tables created successfully")

if __name__ == "__main__":
    from sqlalchemy import text
    create_tables()
    
    