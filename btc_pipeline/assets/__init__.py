
"""
Bitcoin pipeline assets
"""

# Import all assets to make them available at package level
# from btc_pipeline.assets.btc_fng import bitcoin_fear_greed_index
from btc_pipeline.assets.btc_oi import bitcoin_derivatives_aggregated

__all__ = [
    # "bitcoin_fear_greed_index",
    "bitcoin_derivatives_aggregated",
]