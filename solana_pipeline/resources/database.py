from dagster import resource, Field, StringSource

@resource(
    config_schema={
        "db_name": Field(str, default_value="solana_trending", description="Database name"),
        "host": Field(str, default_value="localhost", description="Database host"),
        "port": Field(int, default_value=5432, description="Database port"),
        "user": Field(str, default_value="postgres", description="Database username"),
        "password": Field(str, default_value="St0ck!adePG", description="Database password"),
    }
)
def db_resource(init_context):
    """Resource for database connection."""
    # You'll need to import your DBManager here
    from utils.tiered_db_manager import DBManager
    
    config = init_context.resource_config
    return DBManager(
        db_name=config["db_name"],
        host=config["host"],
        port=config["port"],
        user=config["user"],
        password=config["password"],
        setup_db=True
    )