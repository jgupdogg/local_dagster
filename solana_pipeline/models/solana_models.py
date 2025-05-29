"""SQLModel definitions for Solana Pipeline with data validation."""
from sqlmodel import SQLModel, Field, create_engine, Column
from datetime import datetime
import os
from dotenv import load_dotenv
from sqlalchemy import String, Numeric, DateTime, JSON, Text, ARRAY
from typing import Optional, List, Dict, Any
from pydantic import field_validator, model_validator
import re

# Load environment variables
load_dotenv()

# Constants for validation
SOLANA_ADDRESS_PATTERN = r'^[1-9A-HJ-NP-Za-km-z]{32,44}$'
URL_PATTERN = r'^https?://.*'
TWITTER_PATTERN = r'^@?[A-Za-z0-9_]{1,15}$'


class TrendingToken(SQLModel, table=True):
    """Bronze tier storage of raw token data with state tracking."""
    __tablename__ = "trending_tokens"
    __table_args__ = {"schema": "bronze"}
    
    id: Optional[int] = Field(default=None, primary_key=True)
    token_address: str = Field(index=True, min_length=32, max_length=44)
    symbol: Optional[str] = Field(default=None, max_length=10)
    name: Optional[str] = Field(default=None, max_length=100)
    decimals: Optional[int] = Field(default=None, ge=0, le=18)
    liquidity: Optional[float] = Field(default=None, ge=0)
    logoURI: Optional[str] = Field(default=None, max_length=500)
    volume24hUSD: Optional[float] = Field(default=None, ge=0)
    volume24hChangePercent: Optional[float] = None  # Can be negative
    rank: Optional[int] = Field(default=None, ge=1)
    price: Optional[float] = Field(default=None, ge=0)
    price24hChangePercent: Optional[float] = None  # Can be negative
    fdv: Optional[float] = Field(default=None, ge=0)
    marketcap: Optional[float] = Field(default=None, ge=0)
    
    # Additional metadata fields
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)
    
    # State tracking fields
    tracked: Optional[bool] = None
    processed_at: Optional[datetime] = None
    processing_status: Optional[str] = Field(
        default=None, 
        regex='^(pending|processing|completed|failed)$'
    )
    
    @field_validator('token_address')
    def validate_token_address(cls, v):
        if not re.match(SOLANA_ADDRESS_PATTERN, v):
            raise ValueError('Invalid Solana address format')
        return v
    
    @field_validator('logoURI')
    def validate_logo_uri(cls, v):
        if v and not re.match(URL_PATTERN, v):
            raise ValueError('Invalid URL format for logoURI')
        return v


class TokenListV3(SQLModel, table=True):
    """Bronze tier storage of token data from v3 API with state tracking."""
    __tablename__ = "token_list_v3"
    __table_args__ = {"schema": "bronze"}
    
    id: Optional[int] = Field(default=None, primary_key=True)
    
    # Basic token info
    token_address: str = Field(
        index=True, 
        sa_column_kwargs={"name": "address"},
        min_length=32,
        max_length=44
    )
    symbol: Optional[str] = Field(default=None, max_length=10)
    name: Optional[str] = Field(default=None, max_length=100)
    decimals: Optional[int] = Field(default=None, ge=0, le=18)
    logo_uri: Optional[str] = Field(default=None, max_length=500)
    
    # Market data (all must be non-negative)
    market_cap: Optional[float] = Field(default=None, ge=0)
    fdv: Optional[float] = Field(default=None, ge=0)
    liquidity: Optional[float] = Field(default=None, ge=0)
    last_trade_unix_time: Optional[int] = Field(default=None, ge=0)
    holder: Optional[int] = Field(default=None, ge=0)
    recent_listing_time: Optional[int] = Field(default=None, ge=0)
    
    # Price data
    price: Optional[float] = Field(default=None, ge=0)
    price_change_1h_percent: Optional[float] = None
    price_change_2h_percent: Optional[float] = None
    price_change_4h_percent: Optional[float] = None
    price_change_8h_percent: Optional[float] = None
    price_change_24h_percent: Optional[float] = None
    
    # Volume data (all must be non-negative)
    volume_1h_usd: Optional[float] = Field(default=None, ge=0)
    volume_2h_usd: Optional[float] = Field(default=None, ge=0)
    volume_4h_usd: Optional[float] = Field(default=None, ge=0)
    volume_8h_usd: Optional[float] = Field(default=None, ge=0)
    volume_24h_usd: Optional[float] = Field(default=None, ge=0)
    
    # Volume change percentages (can be negative)
    volume_1h_change_percent: Optional[float] = None
    volume_2h_change_percent: Optional[float] = None
    volume_4h_change_percent: Optional[float] = None
    volume_8h_change_percent: Optional[float] = None
    volume_24h_change_percent: Optional[float] = None
    
    # Trade counts (must be non-negative)
    trade_1h_count: Optional[int] = Field(default=None, ge=0)
    trade_2h_count: Optional[int] = Field(default=None, ge=0)
    trade_4h_count: Optional[int] = Field(default=None, ge=0)
    trade_8h_count: Optional[int] = Field(default=None, ge=0)
    trade_24h_count: Optional[int] = Field(default=None, ge=0)
    
    # Extensions (stored as JSON)
    extensions: Optional[Dict[str, Any]] = Field(default=None, sa_column=Column(JSON))
    
    # Additional metadata fields
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)
    
    # State tracking fields
    tracked: Optional[bool] = None
    processed_at: Optional[datetime] = None
    processing_status: Optional[str] = Field(
        default=None,
        regex='^(pending|processing|completed|failed)$'
    )
    
    @field_validator('token_address')
    def validate_token_address(cls, v):
        if not re.match(SOLANA_ADDRESS_PATTERN, v):
            raise ValueError('Invalid Solana address format')
        return v


class TrackedToken(SQLModel, table=True):
    """Silver tier storage of filtered token data."""
    __tablename__ = "tracked_tokens"
    __table_args__ = {"schema": "silver"}
    
    id: Optional[int] = Field(default=None, primary_key=True)
    token_address: str = Field(index=True, min_length=32, max_length=44)
    symbol: Optional[str] = Field(default=None, max_length=10)
    name: Optional[str] = Field(default=None, max_length=100)
    decimals: Optional[int] = Field(default=None, ge=0, le=18)
    liquidity: Optional[float] = Field(default=None, ge=0)
    logoURI: Optional[str] = Field(default=None, max_length=500)
    volume24hUSD: Optional[float] = Field(default=None, ge=0)
    volume24hChangePercent: Optional[float] = None
    rank: Optional[int] = Field(default=None, ge=1)
    price: Optional[float] = Field(default=None, ge=0)
    price24hChangePercent: Optional[float] = None
    fdv: Optional[float] = Field(default=None, ge=0)
    marketcap: Optional[float] = Field(default=None, ge=0)
    volume_mcap_ratio: Optional[float] = Field(default=None, ge=0)
    quality_score: Optional[float] = Field(default=None, ge=0, le=100)
    
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
    
    @field_validator('token_address')
    def validate_token_address(cls, v):
        if not re.match(SOLANA_ADDRESS_PATTERN, v):
            raise ValueError('Invalid Solana address format')
        return v


class TokenWhale(SQLModel, table=True):
    """Bronze tier storage of token whales."""
    __tablename__ = "token_whales"
    __table_args__ = {"schema": "bronze"}
    
    id: Optional[int] = Field(default=None, primary_key=True)
    token_address: str = Field(index=True, min_length=32, max_length=44)
    wallet_address: str = Field(index=True, min_length=32, max_length=44)
    holdings_amount: Optional[float] = Field(default=None, ge=0)
    holdings_value: Optional[float] = Field(default=None, ge=0)
    holdings_pct: Optional[float] = Field(default=None, ge=0, le=100)
    last_transaction_date: Optional[datetime] = None
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)
    fetched_txns: Optional[datetime] = None
    
    @field_validator('token_address', 'wallet_address')
    def validate_addresses(cls, v):
        if not re.match(SOLANA_ADDRESS_PATTERN, v):
            raise ValueError('Invalid Solana address format')
        return v


class WalletTradeHistory(SQLModel, table=True):
    """Bronze tier storage of transaction history with PnL processing state."""
    __tablename__ = "wallet_trade_history"
    __table_args__ = {"schema": "bronze"}
    
    id: Optional[int] = Field(default=None, primary_key=True)
    wallet_address: str = Field(index=True, min_length=32, max_length=44)
    token_address: str = Field(index=True, min_length=32, max_length=44)
    
    # Transaction metadata
    transaction_hash: str = Field(index=True, min_length=64, max_length=88)
    source: str = Field(max_length=50)
    block_unix_time: int = Field(ge=0)
    tx_type: str = Field(max_length=20)
    timestamp: datetime = Field(index=True)
    
    # Transaction direction
    transaction_type: str = Field(regex='^(BUY|SELL)$')
    
    # "From" token information
    from_symbol: str = Field(max_length=10)
    from_address: str = Field(min_length=32, max_length=44)
    from_decimals: Optional[int] = Field(default=None, ge=0, le=18)
    from_amount: Optional[float] = Field(default=None, ge=0)
    from_raw_amount: Optional[str] = None
    
    # "To" token information
    to_symbol: str = Field(max_length=10)
    to_address: str = Field(min_length=32, max_length=44)
    to_decimals: Optional[int] = Field(default=None, ge=0, le=18)
    to_amount: Optional[float] = Field(default=None, ge=0)
    to_raw_amount: Optional[str] = None
    
    # Price information
    base_price: Optional[float] = Field(default=None, ge=0)
    quote_price: Optional[float] = Field(default=None, ge=0)
    
    # Calculated fields
    value_usd: Optional[float] = Field(default=None, ge=0)
    
    # Metadata
    created_at: datetime = Field(default_factory=datetime.utcnow)
    
    # State tracking fields
    processed_for_pnl: Optional[bool] = None
    processed_at: Optional[datetime] = None
    processing_status: Optional[str] = Field(
        default=None,
        regex='^(pending|processing|completed|failed)$'
    )
    
    @field_validator('wallet_address', 'token_address', 'from_address', 'to_address')
    def validate_addresses(cls, v):
        if not re.match(SOLANA_ADDRESS_PATTERN, v):
            raise ValueError('Invalid Solana address format')
        return v


class TokenMetadata(SQLModel, table=True):
    """Raw token metadata from BirdEye API"""
    __tablename__ = "token_metadata"
    __table_args__ = {"schema": "bronze"}
    
    id: Optional[int] = Field(default=None, primary_key=True)
    
    # Required fields
    token_address: str = Field(
        index=True, 
        unique=True,
        min_length=32,
        max_length=44
    )
    name: str = Field(min_length=1, max_length=100)
    symbol: str = Field(min_length=1, max_length=10)
    
    # Optional fields
    decimals: Optional[int] = Field(default=None, ge=0, le=18)
    logo_uri: Optional[str] = Field(default=None, max_length=500)
    twitter: Optional[str] = Field(default=None, max_length=100)
    website: Optional[str] = Field(default=None, max_length=200)
    description: Optional[str] = Field(default=None, max_length=1000)
    coingecko_id: Optional[str] = Field(default=None, max_length=100)
    
    # Tracking fields
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(
        default_factory=datetime.utcnow,
        sa_column_kwargs={"onupdate": datetime.utcnow}
    )
    
    @field_validator('token_address')
    def validate_token_address(cls, v):
        if not re.match(SOLANA_ADDRESS_PATTERN, v):
            raise ValueError('Invalid Solana address format')
        return v
    
    @field_validator('twitter')
    def validate_twitter(cls, v):
        if v and not re.match(TWITTER_PATTERN, v):
            # If it's a URL, that's ok too
            if not re.match(URL_PATTERN, v):
                raise ValueError('Invalid Twitter handle format')
        return v
    
    @field_validator('website', 'logo_uri')
    def validate_urls(cls, v):
        if v and not re.match(URL_PATTERN, v):
            raise ValueError('Invalid URL format')
        return v


class TokenSecurity(SQLModel, table=True):
    """Raw token security data from BirdEye API"""
    __tablename__ = "token_security"
    __table_args__ = {"schema": "bronze"}
    
    id: Optional[int] = Field(default=None, primary_key=True)
    
    # Required field
    token_address: str = Field(
        index=True,
        unique=True,
        min_length=32,
        max_length=44
    )
    
    # Creator info
    creator_address: Optional[str] = Field(default=None, max_length=44)
    creator_owner_address: Optional[str] = Field(default=None, max_length=44)
    owner_address: Optional[str] = Field(default=None, max_length=44)
    owner_of_owner_address: Optional[str] = Field(default=None, max_length=44)
    
    # Creation details
    creation_tx: Optional[str] = Field(default=None, max_length=88)
    creation_time: Optional[int] = Field(default=None, ge=0)
    creation_slot: Optional[int] = Field(default=None, ge=0)
    mint_tx: Optional[str] = Field(default=None, max_length=88)
    mint_time: Optional[int] = Field(default=None, ge=0)
    mint_slot: Optional[int] = Field(default=None, ge=0)
    
    # Balance info
    creator_balance: Optional[float] = Field(default=None, ge=0)
    owner_balance: Optional[float] = Field(default=None, ge=0)
    owner_percentage: Optional[float] = Field(default=None, ge=0, le=100)
    creator_percentage: Optional[float] = Field(default=None, ge=0, le=100)
    
    # Metaplex info
    metaplex_update_authority: Optional[str] = Field(default=None, max_length=44)
    metaplex_owner_update_authority: Optional[str] = Field(default=None, max_length=44)
    metaplex_update_authority_balance: Optional[float] = Field(default=None, ge=0)
    metaplex_update_authority_percent: Optional[float] = Field(default=None, ge=0, le=100)
    mutable_metadata: Optional[bool] = None
    
    # Holder info
    top10_holder_balance: Optional[float] = Field(default=None, ge=0)
    top10_holder_percent: Optional[float] = Field(default=None, ge=0, le=100)
    top10_user_balance: Optional[float] = Field(default=None, ge=0)
    top10_user_percent: Optional[float] = Field(default=None, ge=0, le=100)
    
    # Token verification
    is_true_token: Optional[bool] = None
    fake_token: Optional[bool] = None
    total_supply: Optional[float] = Field(default=None, ge=0)
    
    # Arrays stored as JSON
    pre_market_holder: Optional[list] = Field(default=None, sa_column=Column(JSON))
    
    # Lock and freeze info
    lock_info: Optional[dict] = Field(default=None, sa_column=Column(JSON))
    freezeable: Optional[bool] = None
    freeze_authority: Optional[str] = Field(default=None, max_length=44)
    
    # Transfer fee info
    transfer_fee_enable: Optional[bool] = None
    transfer_fee_data: Optional[dict] = Field(default=None, sa_column=Column(JSON))
    
    # Token standards
    is_token_2022: Optional[bool] = None
    non_transferable: Optional[bool] = None
    jup_strict_list: Optional[bool] = None
    
    # Tracking fields
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(
        default_factory=datetime.utcnow,
        sa_column_kwargs={"onupdate": datetime.utcnow}
    )
    
    @field_validator('token_address')
    def validate_token_address(cls, v):
        if not re.match(SOLANA_ADDRESS_PATTERN, v):
            raise ValueError('Invalid Solana address format')
        return v
    
    @field_validator(
        'creator_address', 'creator_owner_address', 'owner_address',
        'owner_of_owner_address', 'metaplex_update_authority',
        'metaplex_owner_update_authority', 'freeze_authority'
    )
    def validate_optional_addresses(cls, v):
        if v and not re.match(SOLANA_ADDRESS_PATTERN, v):
            raise ValueError('Invalid Solana address format')
        return v


class TokenCreation(SQLModel, table=True):
    """Raw token creation info from BirdEye API"""
    __tablename__ = "token_creation"
    __table_args__ = {"schema": "bronze"}
    
    id: Optional[int] = Field(default=None, primary_key=True)
    
    # Required field
    token_address: str = Field(
        index=True,
        unique=True,
        min_length=32,
        max_length=44
    )
    
    # Creation details
    tx_hash: Optional[str] = Field(default=None, max_length=88)
    slot: Optional[int] = Field(default=None, ge=0)
    decimals: Optional[int] = Field(default=None, ge=0, le=18)
    owner: Optional[str] = Field(default=None, max_length=44)
    block_unix_time: Optional[int] = Field(default=None, ge=0)
    block_human_time: Optional[str] = Field(default=None, max_length=50)
    
    # Tracking fields
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(
        default_factory=datetime.utcnow,
        sa_column_kwargs={"onupdate": datetime.utcnow}
    )
    
    @field_validator('token_address', 'owner')
    def validate_addresses(cls, v):
        if v and not re.match(SOLANA_ADDRESS_PATTERN, v):
            raise ValueError('Invalid Solana address format')
        return v


class WalletPnL(SQLModel, table=True):
    """Silver tier storage of wallet PnL calculations with additional metadata."""
    __tablename__ = "wallet_pnl"
    __table_args__ = {"schema": "silver"}
    
    id: Optional[int] = Field(default=None, primary_key=True)
    wallet_address: str = Field(index=True, min_length=32, max_length=44)
    token_address: str = Field(index=True, min_length=32, max_length=44)
    time_period: str = Field(regex='^(all|week|month|day)$')
    
    # PnL metrics
    realized_pnl: Optional[float] = None  # Can be negative
    unrealized_pnl: Optional[float] = None  # Can be negative
    total_pnl: Optional[float] = None  # Can be negative
    win_rate: Optional[float] = Field(default=None, ge=0, le=100)
    trade_count: Optional[int] = Field(default=None, ge=0)
    avg_holding_time: Optional[float] = Field(default=None, ge=0)  # in hours
    total_bought: Optional[float] = Field(default=None, ge=0)
    total_sold: Optional[float] = Field(default=None, ge=0)
    roi: Optional[float] = None  # Can be negative
    
    # Added trade frequency metric
    trade_frequency_daily: Optional[float] = Field(default=None, ge=0)
    
    # Time references
    first_transaction: Optional[datetime] = None
    last_transaction: Optional[datetime] = None
    
    # Top trader flag
    top_trader: bool = Field(default=False)
    
    # Timestamps for tracking
    created_at: datetime = Field(default_factory=datetime.utcnow)
    processed_at: datetime = Field(default_factory=datetime.utcnow)
    
    # Bronze reference
    bronze_whale_id: Optional[int] = None
    
    @field_validator('wallet_address', 'token_address')
    def validate_addresses(cls, v):
        if not re.match(SOLANA_ADDRESS_PATTERN, v):
            raise ValueError('Invalid Solana address format')
        return v
    
    @model_validator(mode='after')
    def validate_pnl_consistency(self):
        """Ensure total_pnl = realized_pnl + unrealized_pnl when all are present"""
        if (self.realized_pnl is not None and 
            self.unrealized_pnl is not None and 
            self.total_pnl is not None):
            expected_total = self.realized_pnl + self.unrealized_pnl
            # Allow small floating point differences
            if abs(self.total_pnl - expected_total) > 0.01:
                raise ValueError(
                    f'total_pnl ({self.total_pnl}) != realized_pnl ({self.realized_pnl}) '
                    f'+ unrealized_pnl ({self.unrealized_pnl})'
                )
        return self


class TopTrader(SQLModel, table=True):
    """Gold tier storage of top traders with key performance metrics."""
    __tablename__ = "top_traders"
    __table_args__ = {"schema": "gold"}
    
    id: Optional[int] = Field(default=None, primary_key=True)
    wallet_address: str = Field(index=True, min_length=32, max_length=44)
    time_period: str = Field(regex='^(all|week|month|day)$')
    rank: int = Field(index=True, ge=1)
    
    # PnL metrics
    realized_pnl: Optional[float] = None
    unrealized_pnl: Optional[float] = None
    total_pnl: Optional[float] = None
    win_rate: Optional[float] = Field(default=None, ge=0, le=100)
    trade_count: Optional[int] = Field(default=None, ge=0)
    avg_holding_time: Optional[float] = Field(default=None, ge=0)
    total_bought: Optional[float] = Field(default=None, ge=0)
    total_sold: Optional[float] = Field(default=None, ge=0)
    roi: Optional[float] = None
    
    # Added trade frequency metric
    trade_frequency_daily: Optional[float] = Field(default=None, ge=0)
    
    # Time references  
    first_transaction: Optional[datetime] = None
    last_transaction: Optional[datetime] = None
    
    # Tags for categorization
    tags: Optional[List[str]] = Field(default=None, sa_column=Column(JSON))
    
    # Webhook tracking
    added_to_webhook: bool = Field(default=False)
    added_at: Optional[datetime] = None
    
    # Timestamp
    created_at: datetime = Field(default_factory=datetime.utcnow)
    
    # Silver references
    silver_pnl_id: Optional[int] = None
    
    @field_validator('wallet_address')
    def validate_wallet_address(cls, v):
        if not re.match(SOLANA_ADDRESS_PATTERN, v):
            raise ValueError('Invalid Solana address format')
        return v


class HeliusHook(SQLModel, table=True):
    """Bronze tier storage of raw webhook data from Helius."""
    __tablename__ = "helius_hook"
    __table_args__ = {"schema": "bronze"}
    
    id: Optional[int] = Field(default=None, primary_key=True)
    payload: str = Field(sa_column=Column(Text), min_length=1)
    received_at: datetime = Field(default_factory=datetime.utcnow)
    processed: bool = Field(default=False)
    processed_at: Optional[datetime] = Field(default=None)
    
    @field_validator('payload')
    def validate_payload_json(cls, v):
        """Ensure payload is valid JSON string"""
        try:
            import json
            json.loads(v)
        except json.JSONDecodeError:
            raise ValueError('Payload must be valid JSON')
        return v


class HeliusTxnClean(SQLModel, table=True):
    """Silver tier storage of processed transaction data."""
    __tablename__ = "helius_txns_clean"
    __table_args__ = {"schema": "silver"}
    
    signature: str = Field(primary_key=True, min_length=64, max_length=88)
    
    # Required fields
    raw_id: Optional[int] = Field(default=None)
    user_address: str = Field(min_length=32, max_length=44)
    swapfromtoken: str = Field(min_length=32, max_length=44)
    swapfromamount: float = Field(sa_column=Column(Numeric), ge=0)
    swaptotoken: str = Field(min_length=32, max_length=44)
    swaptoamount: float = Field(sa_column=Column(Numeric), ge=0)
    source: Optional[str] = Field(default=None, max_length=50)
    timestamp: datetime
    processed: bool = Field(default=False)
    notification_sent: bool = Field(default=False)
    
    @field_validator('user_address', 'swapfromtoken', 'swaptotoken')
    def validate_addresses(cls, v):
        if not re.match(SOLANA_ADDRESS_PATTERN, v):
            raise ValueError('Invalid Solana address format')
        return v


class AlphaSignal(SQLModel, table=True):
    """Gold tier storage for detected alpha signals from tracked traders."""
    __tablename__ = "alpha_signals"
    __table_args__ = {"schema": "gold"}
    
    id: Optional[int] = Field(default=None, primary_key=True)
    
    # Token information
    token_address: str = Field(index=True, min_length=32, max_length=44)
    symbol: str = Field(max_length=10)
    name: Optional[str] = Field(default=None, max_length=100)
    price: Optional[float] = Field(default=None, ge=0)
    price_change_24h: Optional[float] = None
    
    # Signal classification
    signal_type: str = Field(
        regex='^(initial_accumulation|strong_accumulation|distribution)$'
    )
    confidence: float = Field(ge=0, le=100)
    
    # Trader metrics
    alpha_tier_count: int = Field(ge=0)
    solid_tier_count: int = Field(ge=0)
    tracking_tier_count: int = Field(ge=0)
    total_wallet_count: int = Field(ge=1)
    
    # Trader wallets (store as JSON arrays)
    alpha_wallets: Optional[List[str]] = Field(default=None, sa_column=Column(JSON))
    solid_wallets: Optional[List[str]] = Field(default=None, sa_column=Column(JSON))
    tracking_wallets: Optional[List[str]] = Field(default=None, sa_column=Column(JSON))
    
    # Activity metrics
    total_volume: Optional[float] = Field(default=None, ge=0)
    avg_position_size: Optional[float] = Field(default=None, ge=0)
    weighted_score: Optional[float] = Field(default=None, ge=0)
    
    # Time windows and detection info
    window_hours: int = Field(ge=1)
    first_transaction_time: datetime
    latest_transaction_time: datetime
    detected_at: datetime = Field(default_factory=datetime.utcnow)
    
    # Notification status
    notification_sent: bool = Field(default=False)
    notification_sent_at: Optional[datetime] = None
    
    # Signal status tracking
    validated: Optional[bool] = None
    price_at_signal: Optional[float] = Field(default=None, ge=0)
    max_price_after_signal: Optional[float] = Field(default=None, ge=0)
    min_price_after_signal: Optional[float] = Field(default=None, ge=0)
    price_24h_after_signal: Optional[float] = Field(default=None, ge=0)
    
    # Metadata
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)
    
    @field_validator('token_address')
    def validate_token_address(cls, v):
        if not re.match(SOLANA_ADDRESS_PATTERN, v):
            raise ValueError('Invalid Solana address format')
        return v
    
    @model_validator(mode='after')
    def validate_wallet_counts(self):
        """Ensure wallet counts match array lengths and total"""
        if self.alpha_wallets and len(self.alpha_wallets) != self.alpha_tier_count:
            raise ValueError('alpha_wallets length must match alpha_tier_count')
        if self.solid_wallets and len(self.solid_wallets) != self.solid_tier_count:
            raise ValueError('solid_wallets length must match solid_tier_count')
        if self.tracking_wallets and len(self.tracking_wallets) != self.tracking_tier_count:
            raise ValueError('tracking_wallets length must match tracking_tier_count')
        
        expected_total = self.alpha_tier_count + self.solid_tier_count + self.tracking_tier_count
        if expected_total != self.total_wallet_count:
            raise ValueError('total_wallet_count must equal sum of tier counts')
        
        return self


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