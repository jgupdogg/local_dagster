"""
Token enrichment assets for fetching metadata, security, and creation info
"""
from datetime import datetime, timedelta
from typing import List, Set, Dict, Optional
from dagster import asset, AssetExecutionContext
from sqlmodel import select, or_, and_
import time

from solana_pipeline.models.solana_models import (
    TokenMetadata, TokenSecurity, TokenCreation,
    HeliusTxnClean
)


@asset(
    group_name="token_enrichment",
    required_resource_keys={"db", "birdeye_api"},
    deps=["processed_transactions"],  # Ensure transactions are processed first
    description="Fetches and stores token metadata from BirdEye API"
)
def fetch_token_metadata(context: AssetExecutionContext):
    """
    Fetches token metadata from BirdEye API for tokens in recent transactions.
    Checks last 100 transactions and updates tokens with missing or stale metadata.
    """
    # Resources
    db = context.resources.db
    birdeye_api = context.resources.birdeye_api
    
    # Time threshold for stale data (7 days)
    stale_threshold = datetime.utcnow() - timedelta(days=7)
    
    updated_count = 0
    failed_count = 0
    
    with db.get_session() as session:
        # Get recent transactions
        recent_txns_query = select(HeliusTxnClean).order_by(
            HeliusTxnClean.timestamp.desc()
        ).limit(100)
        
        recent_txns = session.exec(recent_txns_query).all()
        context.log.info(f"Found {len(recent_txns)} recent transactions")
        
        # Collect unique token addresses
        token_addresses = set()
        for txn in recent_txns:
            if txn.swapfromtoken:
                token_addresses.add(txn.swapfromtoken)
                context.log.debug(f"Added from token: {txn.swapfromtoken[:8]}...")
            if txn.swaptotoken:
                token_addresses.add(txn.swaptotoken)
                context.log.debug(f"Added to token: {txn.swaptotoken[:8]}...")
        
        context.log.info(f"Found {len(token_addresses)} unique tokens from recent transactions")
        
        # Check which tokens need metadata updates in a single query
        existing_metadata_query = select(TokenMetadata).where(
            TokenMetadata.token_address.in_(list(token_addresses))
        )
        existing_metadata = session.exec(existing_metadata_query).all()
        
        # Create a set of addresses that have fresh, complete metadata
        fresh_tokens = set()
        for metadata in existing_metadata:
            if (metadata.created_at >= stale_threshold and 
                metadata.twitter and metadata.twitter != ""):
                fresh_tokens.add(metadata.token_address)
                context.log.debug(f"Token {metadata.token_address[:8]}... has fresh metadata")
            else:
                if metadata.created_at < stale_threshold:
                    context.log.info(f"Token {metadata.token_address[:8]}... metadata is stale")
                elif not metadata.twitter or metadata.twitter == "":
                    context.log.info(f"Token {metadata.token_address[:8]}... missing Twitter data")
        
        # Tokens needing update = all tokens - fresh tokens
        tokens_needing_update = list(token_addresses - fresh_tokens)
        context.log.info(f"Found {len(tokens_needing_update)} tokens needing metadata updates")
    
    # Process tokens one at a time
    for token_address in tokens_needing_update:
        try:
            context.log.info(f"Fetching metadata for {token_address[:8]}...")
            
            # Fetch metadata for single token
            response = birdeye_api.token.get_token_metadata(token_address)
            
            if response.get('success') and response.get('data'):
                token_data = response['data']
                
                try:
                    # Unpack extensions if present
                    extensions = token_data.get('extensions', {}) or {}
                    
                    # Create metadata record with unpacked fields
                    metadata = TokenMetadata(
                        token_address=token_data.get('address', token_address),
                        name=token_data.get('name', ''),
                        symbol=token_data.get('symbol', ''),
                        decimals=token_data.get('decimals'),
                        logo_uri=token_data.get('logo_uri'),
                        # Unpacked extension fields
                        twitter=extensions.get('twitter'),
                        website=extensions.get('website'),
                        description=extensions.get('description'),
                        coingecko_id=extensions.get('coingeckoId'),  # Note: API uses camelCase
                        created_at=datetime.utcnow()
                    )
                    
                    with db.get_session() as session:
                        # Check if exists to update
                        existing = session.exec(
                            select(TokenMetadata).where(
                                TokenMetadata.token_address == token_address
                            )
                        ).first()
                        
                        if existing:
                            # Update existing record
                            existing.name = metadata.name
                            existing.symbol = metadata.symbol
                            existing.decimals = metadata.decimals
                            existing.logo_uri = metadata.logo_uri
                            existing.twitter = metadata.twitter
                            existing.website = metadata.website
                            existing.description = metadata.description
                            existing.coingecko_id = metadata.coingecko_id
                            existing.updated_at = datetime.utcnow()
                            session.add(existing)
                        else:
                            # Insert new record
                            session.add(metadata)
                        
                        session.commit()
                        updated_count += 1
                        context.log.info(f"Updated metadata for {token_address[:8]}... ({metadata.symbol})")
                        
                except Exception as e:
                    context.log.error(f"Failed to save metadata for {token_address}: {e}")
                    failed_count += 1
                    
            else:
                context.log.warning(f"No metadata returned for {token_address[:8]}...")
                failed_count += 1
                
        except Exception as e:
            context.log.error(f"Failed to fetch metadata for {token_address}: {e}")
            failed_count += 1
        
        # Add delay to avoid rate limiting
        time.sleep(0.1)  # 100ms delay between requests
    
    context.log.info(f"Metadata update complete: {updated_count} updated, {failed_count} failed")
    
    # Return nothing - we don't need pickle/IO manager
    pass


@asset(
    group_name="token_enrichment",
    required_resource_keys={"db", "birdeye_api"},
    deps=["processed_transactions"],  # Ensure transactions are processed first
    description="Fetches and stores token security info from BirdEye API"
)
def fetch_token_security(context: AssetExecutionContext):
    """
    Fetches token security data from BirdEye API for tokens in recent transactions.
    Checks last 100 transactions and updates tokens with missing or stale security data.
    """
    # Resources
    db = context.resources.db
    birdeye_api = context.resources.birdeye_api
    
    # Time threshold for stale data (7 days)
    stale_threshold = datetime.utcnow() - timedelta(days=7)
    
    updated_count = 0
    failed_count = 0
    
    with db.get_session() as session:
        # Get recent transactions
        recent_txns_query = select(HeliusTxnClean).order_by(
            HeliusTxnClean.timestamp.desc()
        ).limit(100)
        
        recent_txns = session.exec(recent_txns_query).all()
        context.log.info(f"Found {len(recent_txns)} recent transactions")
        
        # Collect unique token addresses
        token_addresses = set()
        for txn in recent_txns:
            if txn.swapfromtoken:
                token_addresses.add(txn.swapfromtoken)
            if txn.swaptotoken:
                token_addresses.add(txn.swaptotoken)
        
        context.log.info(f"Found {len(token_addresses)} unique tokens from recent transactions")
        
        # Check which tokens need security updates in a single query
        existing_security_query = select(TokenSecurity).where(
            TokenSecurity.token_address.in_(list(token_addresses))
        )
        existing_security = session.exec(existing_security_query).all()
        
        # Create a set of addresses that have fresh security data
        fresh_tokens = set()
        for security in existing_security:
            if security.created_at >= stale_threshold:
                fresh_tokens.add(security.token_address)
                context.log.debug(f"Token {security.token_address[:8]}... has fresh security data")
            else:
                context.log.info(f"Token {security.token_address[:8]}... security data is stale")
        
        # Tokens needing update = all tokens - fresh tokens
        tokens_needing_update = list(token_addresses - fresh_tokens)
        context.log.info(f"Found {len(tokens_needing_update)} tokens needing security updates")
    
    # Process tokens one at a time
    for token_address in tokens_needing_update:
        try:
            context.log.info(f"Fetching security data for {token_address[:8]}...")
            
            # Fetch security data
            response = birdeye_api.token.get_token_security(token_address)
            
            if response.get('success') and response.get('data'):
                data = response['data']
                
                # Create security record
                security = TokenSecurity(
                    token_address=token_address,
                    creator_address=data.get('creatorAddress'),
                    creator_owner_address=data.get('creatorOwnerAddress'),
                    owner_address=data.get('ownerAddress'),
                    owner_of_owner_address=data.get('ownerOfOwnerAddress'),
                    creation_tx=data.get('creationTx'),
                    creation_time=data.get('creationTime'),
                    creation_slot=data.get('creationSlot'),
                    mint_tx=data.get('mintTx'),
                    mint_time=data.get('mintTime'),
                    mint_slot=data.get('mintSlot'),
                    creator_balance=data.get('creatorBalance'),
                    owner_balance=data.get('ownerBalance'),
                    owner_percentage=data.get('ownerPercentage'),
                    creator_percentage=data.get('creatorPercentage'),
                    metaplex_update_authority=data.get('metaplexUpdateAuthority'),
                    metaplex_owner_update_authority=data.get('metaplexOwnerUpdateAuthority'),
                    metaplex_update_authority_balance=data.get('metaplexUpdateAuthorityBalance'),
                    metaplex_update_authority_percent=data.get('metaplexUpdateAuthorityPercent'),
                    mutable_metadata=data.get('mutableMetadata'),
                    top10_holder_balance=data.get('top10HolderBalance'),
                    top10_holder_percent=data.get('top10HolderPercent'),
                    top10_user_balance=data.get('top10UserBalance'),
                    top10_user_percent=data.get('top10UserPercent'),
                    is_true_token=data.get('isTrueToken'),
                    fake_token=data.get('fakeToken'),
                    total_supply=data.get('totalSupply'),
                    pre_market_holder=data.get('preMarketHolder', []),
                    lock_info=data.get('lockInfo'),
                    freezeable=data.get('freezeable'),
                    freeze_authority=data.get('freezeAuthority'),
                    transfer_fee_enable=data.get('transferFeeEnable'),
                    transfer_fee_data=data.get('transferFeeData'),
                    is_token_2022=data.get('isToken2022', False),
                    non_transferable=data.get('nonTransferable'),
                    jup_strict_list=data.get('jupStrictList'),
                    created_at=datetime.utcnow()
                )
                
                with db.get_session() as session:
                    # Check if exists to update
                    existing = session.exec(
                        select(TokenSecurity).where(
                            TokenSecurity.token_address == token_address
                        )
                    ).first()
                    
                    if existing:
                        # Update existing record
                        for key, value in security.dict(exclude={'id'}).items():
                            setattr(existing, key, value)
                        session.add(existing)
                    else:
                        # Insert new record
                        session.add(security)
                    
                    session.commit()
                    updated_count += 1
                    context.log.info(f"Updated security data for {token_address[:8]}...")
                    
        except Exception as e:
            context.log.error(f"Failed to fetch security for {token_address}: {e}")
            failed_count += 1
        
        # Add delay to avoid rate limiting
        time.sleep(0.1)  # 100ms delay between requests
    
    context.log.info(f"Security update complete: {updated_count} updated, {failed_count} failed")
    
    # Return nothing - we don't need pickle/IO manager
    pass


@asset(
    group_name="token_enrichment",
    required_resource_keys={"db", "birdeye_api"},
    deps=["processed_transactions"],  # Ensure transactions are processed first
    description="Fetches and stores token creation info from BirdEye API"
)
def fetch_token_creation(context: AssetExecutionContext):
    """
    Fetches token creation data from BirdEye API for tokens in recent transactions.
    Creation data never changes, so only fetch for tokens without it.
    """
    # Resources
    db = context.resources.db
    birdeye_api = context.resources.birdeye_api
    
    updated_count = 0
    failed_count = 0
    
    with db.get_session() as session:
        # Get recent transactions
        recent_txns_query = select(HeliusTxnClean).order_by(
            HeliusTxnClean.timestamp.desc()
        ).limit(100)
        
        recent_txns = session.exec(recent_txns_query).all()
        context.log.info(f"Found {len(recent_txns)} recent transactions")
        
        # Collect unique token addresses
        token_addresses = set()
        for txn in recent_txns:
            if txn.swapfromtoken:
                token_addresses.add(txn.swapfromtoken)
            if txn.swaptotoken:
                token_addresses.add(txn.swaptotoken)
        
        context.log.info(f"Found {len(token_addresses)} unique tokens from recent transactions")
        
        # Check which tokens need creation info updates in a single query
        existing_creation_query = select(TokenCreation).where(
            TokenCreation.token_address.in_(list(token_addresses))
        )
        existing_creation = session.exec(existing_creation_query).all()
        
        # Create a set of addresses that have creation data
        tokens_with_creation = {creation.token_address for creation in existing_creation}
        
        # Tokens needing update = all tokens - tokens with creation data
        tokens_needing_update = list(token_addresses - tokens_with_creation)
        context.log.info(f"Found {len(tokens_needing_update)} tokens needing creation info")
    
    # Process tokens one at a time
    for token_address in tokens_needing_update:
        try:
            context.log.info(f"Fetching creation data for {token_address[:8]}...")
            
            # Fetch creation data
            response = birdeye_api.token.get_token_creation_info(token_address)
            
            if response.get('success') and response.get('data'):
                data = response['data']
                
                # Create creation record
                creation = TokenCreation(
                    token_address=data.get('tokenAddress', token_address),
                    tx_hash=data.get('txHash'),
                    slot=data.get('slot'),
                    decimals=data.get('decimals'),
                    owner=data.get('owner'),
                    block_unix_time=data.get('blockUnixTime'),
                    block_human_time=data.get('blockHumanTime'),
                    created_at=datetime.utcnow()
                )
                
                with db.get_session() as session:
                    # Check if exists (shouldn't happen for creation data)
                    existing = session.exec(
                        select(TokenCreation).where(
                            TokenCreation.token_address == token_address
                        )
                    ).first()
                    
                    if existing:
                        # Update existing record
                        for key, value in creation.dict(exclude={'id'}).items():
                            setattr(existing, key, value)
                        session.add(existing)
                    else:
                        # Insert new record
                        session.add(creation)
                    
                    session.commit()
                    updated_count += 1
                    context.log.info(f"Updated creation data for {token_address[:8]}...")
                    
        except Exception as e:
            context.log.error(f"Failed to fetch creation info for {token_address}: {e}")
            failed_count += 1
        
        # Add delay to avoid rate limiting
        time.sleep(0.1)  # 100ms delay between requests
    
    context.log.info(f"Creation info update complete: {updated_count} updated, {failed_count} failed")
    
    # Return nothing - we don't need pickle/IO manager
    pass