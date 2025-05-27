def format_active_token_message(token):
    """
    Format a Slack message for an active token notification with emojis
    and without large unfurled links.
    
    Args:
        token (dict): Token data from get_active_tokens query
        
    Returns:
        str: Formatted Slack message
    """
    # Extract values with fallbacks
    token_address = token.get('token_address', '')
    symbol = token.get('symbol') or f"Unknown-{token_address[:6]}"
    name = token.get('name') or f"Unknown Token ({token_address[:8]}...)"
    twitter = token.get('twitter', '').replace('@', '') or 'Unknown'
    token_age_days = token.get('token_age_days')
    snifscore = token.get('snifscore', 0)
    
    # Format token age in a more readable way
    if token_age_days:
        if token_age_days < 30:
            age_display = f"{token_age_days}d"
        elif token_age_days < 365:
            months = token_age_days // 30
            age_display = f"{months}mo"
        else:
            years = token_age_days // 365
            age_display = f"{years}y"
    else:
        age_display = "Unknown"
    
    # Format security score
    security_level = "Unknown"
    if snifscore:
        if snifscore >= 80:
            security_level = "Excellent"
        elif snifscore >= 70:
            security_level = "Good"
        elif snifscore >= 50:
            security_level = "Fair"
        else:
            security_level = "Poor"
    
    # Select emoji based on signal type
    if token.get('signal_type') == 'most_unique_buyers':
        emoji = "🚀"
    elif token.get('signal_type') == 'most_unique_sellers':
        emoji = "⚠️"
    elif token.get('signal_type') == 'most_net_buyers':
        emoji = "💰"
    else:
        emoji = "🔔"
    
    # Create DEX Screener URL
    dex_url = f"https://dexscreener.com/solana/{token_address}"
    
    # For simplicity, we'll use the same buyer/seller counts for all time periods
    # In a real implementation, you'd get different time windows from your query
    buyers = token.get('unique_buyers', 0)
    sellers = token.get('unique_sellers', 0)
    
    # Create the message
    message = f"""{emoji} *<{dex_url}|{name}>* ({symbol}) """
    
    # Add time-based metrics
    message += f"""3h: {buyers} buys, {sellers} sells | """
    message += f"""12h: {buyers} buys, {sellers} sells | """
    message += f"""24h: {buyers} buys, {sellers} sells"""
    
    # Add market cap if available
    market_cap = token.get('market_cap')
    if market_cap:
        message += f"""     💰 ${market_cap:,.0f}"""
    
    # Add Twitter info (without unfurling)
    if twitter and twitter != 'Unknown':
        followers = token.get('twitter_followers', 0)
        smart_followers = followers // 10  # Just a placeholder logic
        message += f"""     🐦 @{twitter} ({smart_followers} Followers)"""
    
    # Add signal type as trader
    signal_type = token.get('signal_type', '').replace('most_', '').replace('_', ' ').title()
    message += f"""     👥 {signal_type}"""
    
    # Add security info
    message += f"""     🛡️ {snifscore} ({security_level})"""
    
    # Add age
    message += f"""     ⏱️ {age_display}"""
    
    return message