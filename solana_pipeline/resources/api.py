from dagster import resource, Field, StringSource

@resource(
    config_schema={
        "conn_id": Field(str, default_value="birdeye_default", description="Connection ID"),
    }
)
def birdeye_api_resource(init_context):
    """Resource for BirdEye API connection."""
    # You'll need to import your BirdEyeSDK here
    from utils.crypto.birdseye_sdk import BirdEyeSDK
    
    config = init_context.resource_config
    return BirdEyeSDK(conn_id=config["conn_id"])

@resource(
    config_schema={
        "api_key": Field(StringSource, description="API key for Helius"),
    }
)
def helius_api_resource(init_context):
    """Resource for Helius API connection."""
    # You'll need to import your WebhooksAPI here
    from utils.helius.webhooks_api import WebhooksAPI
    
    config = init_context.resource_config
    return WebhooksAPI(api_key=config["api_key"])