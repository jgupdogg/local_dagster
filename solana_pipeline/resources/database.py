"""Database resource for Solana Pipeline."""
from dagster import resource, Field, StringSource
import os
from typing import Optional, List
from datetime import datetime
from sqlmodel import Field as SQLField, Session, SQLModel, create_engine, select
from sqlalchemy import text, inspect
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import sessionmaker, scoped_session
from contextlib import contextmanager
import time
import logging

# Configure logging
logger = logging.getLogger(__name__)

class DBManager:
    """
    Enhanced schema-based tiered database manager for data pipelines.
    Manages bronze/silver/gold schemas within a single PostgreSQL database.
    Supports SQLModel for ORM operations.
    """
    
    # Define tiers
    BRONZE = "bronze"
    SILVER = "silver"
    GOLD = "gold"
    TIERS = [BRONZE, SILVER, GOLD]
    
    def __init__(
        self, 
        db_name: str = "solana_pipeline",
        host: str = "localhost", 
        port: int = 5432,
        user: str = "postgres",
        password: str = None,
        setup_db: bool = False,
        connection_string: str = None
    ):
        """Initialize the database manager with connection to PostgreSQL"""
        # If connection string is provided, use it directly
        if connection_string:
            self.db_url = connection_string
            # Extract db_name from connection string if possible (for logging)
            try:
                self.db_name = connection_string.split("/")[-1]
            except:
                self.db_name = "unknown"
        else:
            # Store connection info
            self.db_name = db_name
            self.host = host
            self.port = port
            self.user = user
            self.password = password
            
            # Check if database exists and create if needed
            self._ensure_database_exists()
            
            # Create connection string
            self.db_url = f"postgresql://{user}:{password}@{host}:{port}/{db_name}"
        
        # Configure connection pooling
        self.engine = create_engine(
            self.db_url, 
            pool_size=5,
            max_overflow=10,
            pool_timeout=30,
            pool_recycle=1800,
            pool_pre_ping=True
        )
        self.Session = scoped_session(sessionmaker(bind=self.engine))
        
        # Retry configuration
        self.max_retries = 3
        self.retry_delay = 5
        self.connection_error_messages = [
            "connection slots are reserved",
            "too many connections",
            "connection timed out",
            "database does not exist"
        ]
        
        # Create schemas if requested
        if setup_db:
            logger.info("Setting up database schemas...")
            self.setup_schemas()
            
        # Log database connection
        logger.info(f"Connected to database '{self.db_name}'")
    
    def _ensure_database_exists(self):
        """Check if the database exists and create it if it doesn't"""
        if not self.password:
            logger.warning("No password provided, skipping database creation check")
            return
            
        # Connect to default 'postgres' database
        postgres_url = f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/postgres"
        try:
            postgres_engine = create_engine(postgres_url)
            with postgres_engine.connect() as conn:
                # Check if database exists
                result = conn.execute(text(
                    "SELECT 1 FROM pg_database WHERE datname = :db_name"
                ), {"db_name": self.db_name})
                
                if result.scalar() is None:
                    # Database doesn't exist, create it
                    # Need to commit transactions before creating a database
                    conn.execute(text("COMMIT"))
                    conn.execute(text(f"CREATE DATABASE {self.db_name}"))
                    logger.info(f"Created database '{self.db_name}'")
                else:
                    logger.debug(f"Database '{self.db_name}' already exists")
                    
        except SQLAlchemyError as e:
            logger.error(f"Error checking/creating database: {e}")
            # Continue anyway - if there's a connection issue, let the main init handle it
        finally:
            postgres_engine.dispose()
    
    @contextmanager
    def get_connection(self):
        """Get a database connection with proper cleanup"""
        connection = None
        try:
            connection = self.engine.connect()
            yield connection
        finally:
            if connection:
                connection.close()
    
    @contextmanager
    def get_session(self):
        """Get a SQLModel session with proper cleanup"""
        session = Session(self.engine)
        try:
            yield session
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()
    
    def setup_schemas(self) -> bool:
        """Create the bronze/silver/gold schemas if they don't exist"""
        try:
            with self.get_connection() as conn:
                # Always create the public schema as a fallback
                conn.execute(text("CREATE SCHEMA IF NOT EXISTS public"))
                logger.info("Created schema 'public' (if not exists)")
                
                # Create other schemas
                for tier in self.TIERS:
                    conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {tier}"))
                    logger.info(f"Created schema '{tier}' (if not exists)")
                return True
        except Exception as e:
            logger.error(f"Failed to setup schemas: {e}")
            return False
    
    def schema_exists(self, schema_name: str) -> bool:
        """Check if a schema exists"""
        try:
            with self.get_connection() as conn:
                result = conn.execute(text(
                    "SELECT schema_name FROM information_schema.schemata WHERE schema_name = :schema"
                ), {"schema": schema_name})
                return result.fetchone() is not None
        except Exception as e:
            logger.error(f"Error checking if schema exists: {e}")
            return False
    
    def table_exists(self, table_name: str, tier: str) -> bool:
        """Check if a table exists in the specified tier"""
        for attempt in range(self.max_retries):
            try:
                inspector = inspect(self.engine)
                tables = inspector.get_table_names(schema=tier)
                return table_name in tables
            except Exception as e:
                if any(err in str(e).lower() for err in self.connection_error_messages) and attempt < self.max_retries - 1:
                    logger.warning(f"Database connection issue, retry {attempt+1}/{self.max_retries}")
                    time.sleep(self.retry_delay)
                else:
                    logger.error(f"Error checking if table exists: {e}")
                    return False
        return False
    
    def create_all_tables(self):
        """Create all tables defined by SQLModel"""
        try:
            # First, make sure schemas exist
            self.setup_schemas()
            
            # Then create all tables
            SQLModel.metadata.create_all(self.engine)
            logger.info("Created all SQLModel tables")
        except Exception as e:
            logger.error(f"Error creating tables: {e}")
            raise
    
    def insert_model(self, model_instance: SQLModel) -> bool:
        """Insert a single SQLModel instance into the database"""
        try:
            with self.get_session() as session:
                session.add(model_instance)
                return True
        except Exception as e:
            logger.error(f"Failed to insert model: {e}")
            return False
    
    def insert_models(self, model_instances: List[SQLModel]) -> bool:
        """Insert multiple SQLModel instances into the database"""
        if not model_instances:
            logger.warning("No model instances provided to insert")
            return False
            
        try:
            with self.get_session() as session:
                session.add_all(model_instances)
                return True
        except Exception as e:
            logger.error(f"Failed to insert models: {e}")
            return False
    
    def query(self, model_class, *filters, **filter_by):
        """Query the database using SQLModel"""
        try:
            with self.get_session() as session:
                statement = select(model_class).filter(*filters).filter_by(**filter_by)
                results = session.exec(statement)
                return results.all()
        except Exception as e:
            logger.error(f"Failed to query database: {e}")
            return []
    
    def quit(self):
        """Close all connections and dispose of engine"""
        self.engine.dispose()
        logger.debug("Database connections closed")

    

@resource(
    config_schema={
        "connection_string": Field(
            StringSource,
            description="Database connection string.",
            is_required=False,
        ),
        "db_name": Field(
            StringSource,
            description="Database name.",
            is_required=False,
            default_value="solana_pipeline",
        ),
        "db_host": Field(
            StringSource,
            description="Database host.",
            is_required=False,
            default_value="localhost",
        ),
        "db_port": Field(
            StringSource,
            description="Database port.",
            is_required=False,
            default_value="5432",
        ),
        "db_user": Field(
            StringSource,
            description="Database user.",
            is_required=False,
            default_value="postgres",
        ),
        "db_password": Field(
            StringSource,
            description="Database password.",
            is_required=False,
        ),
        "setup_db": Field(
            bool,
            description="Whether to set up the database schemas.",
            is_required=False,
            default_value=False,
        ),
    }
)
def db_resource(context):
    """Resource for database connection."""
    # Get connection string from config or environment variables
    connection_string = context.resource_config.get("connection_string")
    
    if not connection_string:
        # Build connection string from environment variables or config
        db_user = context.resource_config.get("db_user") or os.getenv("DB_USER", "postgres")
        db_password = context.resource_config.get("db_password") or os.getenv("DB_PASSWORD")
        db_host = context.resource_config.get("db_host") or os.getenv("DB_HOST", "localhost")
        db_port = context.resource_config.get("db_port") or os.getenv("DB_PORT", "5432")
        db_name = context.resource_config.get("db_name") or os.getenv("DB_NAME", "solana_pipeline")
        setup_db = context.resource_config.get("setup_db", True)  # Set default to True to always setup DB
        
        # Create DBManager with individual parameters
        return DBManager(
            db_name=db_name,
            host=db_host,
            port=int(db_port),
            user=db_user,
            password=db_password,
            setup_db=setup_db
        )
    
    # Create DBManager with connection string
    return DBManager(connection_string=connection_string)