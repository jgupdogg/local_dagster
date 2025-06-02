"""
Database resource for Dagster - Enhanced for financial data pipeline
"""
from dagster import ConfigurableResource, InitResourceContext
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.pool import NullPool
from sqlalchemy.dialects.postgresql import insert
from pydantic import Field, PrivateAttr
import os
from typing import Optional, List, Dict, Any, Type
from contextlib import contextmanager
from decimal import Decimal
import logging

# Import SQLModel directly to access its metadata
from sqlmodel import SQLModel

logger = logging.getLogger(__name__)


class DatabaseResource(ConfigurableResource):
    """
    Enhanced Dagster resource for database connections with financial data support.
    """
    
    # Configuration fields
    database_url: Optional[str] = Field(
        default=None,
        description="Database connection URL. If not provided, uses DATABASE_URL env var"
    )
    
    echo: bool = Field(
        default=False,
        description="Whether to echo SQL statements"
    )
    
    setup_db: bool = Field(
        default=False,
        description="Whether to create schemas and tables on startup"
    )
    
    pool_size: int = Field(
        default=10,  # Increased for financial data workloads
        description="Number of connections to maintain in pool"
    )
    
    max_overflow: int = Field(
        default=20,  # Increased for peak loads
        description="Maximum overflow connections allowed"
    )
    
    pool_pre_ping: bool = Field(
        default=True,
        description="Test connections before using them"
    )
    
    statement_timeout: int = Field(
        default=300000,  # 5 minutes in milliseconds
        description="Query timeout for PostgreSQL in milliseconds"
    )
    
    # Private attributes for runtime state
    _engine: Optional[object] = PrivateAttr(default=None)
    _session_factory: Optional[sessionmaker] = PrivateAttr(default=None)
    
    def setup_for_execution(self, context: InitResourceContext) -> None:
        """Setup database connection when resource is initialized."""
        # Get database URL
        db_url = self.database_url or os.getenv(
            "DATABASE_URL",
            "postgresql://user:password@localhost:5432/crypto_data"
        )
        
        # Configure connection arguments based on database type
        connect_args = {}
        if "postgresql" in db_url:
            connect_args = {
                "connect_timeout": 10,
                "options": f"-c statement_timeout={self.statement_timeout}"
            }
        
        # Create engine with enhanced configuration
        self._engine = create_engine(
            db_url,
            echo=self.echo,
            pool_pre_ping=self.pool_pre_ping,
            poolclass=NullPool if "sqlite" in db_url else None,
            pool_size=self.pool_size if "sqlite" not in db_url else None,
            max_overflow=self.max_overflow if "sqlite" not in db_url else None,
            connect_args=connect_args if connect_args else None,
        )
        
        # Create session factory
        self._session_factory = sessionmaker(
            autocommit=False,
            autoflush=False,
            bind=self._engine
        )
        
        # Create schemas and tables if requested
        if self.setup_db:
            self._setup_database_schema(context)
    
    def _setup_database_schema(self, context: InitResourceContext) -> None:
        """Create database schemas and tables."""
        # Create schemas first (PostgreSQL specific)
        if "postgresql" in str(self._engine.url):
            context.log.info("Creating database schemas...")
            with self._engine.connect() as conn:
                conn.execute(text("CREATE SCHEMA IF NOT EXISTS bronze"))
                conn.execute(text("CREATE SCHEMA IF NOT EXISTS silver"))
                conn.execute(text("CREATE SCHEMA IF NOT EXISTS gold"))
                conn.commit()
            context.log.info("Database schemas created")
        
        # Create tables
        context.log.info("Creating database tables...")
        SQLModel.metadata.create_all(bind=self._engine)
        context.log.info("Database tables created")
    
    def teardown_after_execution(self, context: InitResourceContext) -> None:
        """Cleanup database connections."""
        if self._engine:
            self._engine.dispose()
            context.log.info("Database connections closed")
    
    def get_session(self) -> Session:
        """Get a new database session."""
        if not self._session_factory:
            raise RuntimeError("Database not initialized")
        return self._session_factory()
    
    @contextmanager
    def session_scope(self):
        """
        Provide a transactional scope for database operations.
        
        Usage:
            with db.session_scope() as session:
                session.add(record)
                # Automatically commits on success, rolls back on exception
        """
        session = self.get_session()
        try:
            yield session
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()
    
    def bulk_insert_ignore_conflicts(
        self, 
        model_class: Type[SQLModel], 
        records: List[Dict[str, Any]],
        conflict_columns: Optional[List[str]] = None
    ) -> int:
        """
        Bulk insert records ignoring conflicts (duplicates).
        """
        if not records:
            return 0
        
        # Clean records - remove None values
        cleaned_records = []
        for record in records:
            # Remove None values from the record
            cleaned_record = {k: v for k, v in record.items() if v is not None}
            cleaned_records.append(cleaned_record)
        
        # Convert model instances to dicts if needed
        if hasattr(records[0], 'dict'):
            records = [r.dict(exclude={'id', 'created_at', 'updated_at'}) for r in records]
        
        # Ensure Decimal types are properly handled
        for record in records:
            for key, value in record.items():
                if isinstance(value, float) and key in [
                    'open_interest_usd', 'volume_24h', 'funding_rate', 
                    'liquidations_usd', 'long_short_ratio'
                ]:
                    record[key] = Decimal(str(value))
        
        with self.session_scope() as session:
            # For PostgreSQL, use INSERT ... ON CONFLICT DO NOTHING
            if "postgresql" in str(self._engine.url):
                stmt = insert(model_class.__table__).values(records)
                
                if conflict_columns:
                    # Use specified columns for conflict detection
                    stmt = stmt.on_conflict_do_nothing(index_elements=conflict_columns)
                else:
                    # Use all unique constraints
                    stmt = stmt.on_conflict_do_nothing()
                
                result = session.execute(stmt)
                return result.rowcount
            else:
                # Fallback for non-PostgreSQL databases
                # Insert one by one, ignoring duplicates
                inserted = 0
                for record in records:
                    try:
                        session.add(model_class(**record))
                        session.flush()
                        inserted += 1
                    except Exception as e:
                        session.rollback()
                        logger.debug(f"Skipping duplicate record: {e}")
                        continue
                
                return inserted
    
    def bulk_upsert(
        self,
        model_class: Type[SQLModel],
        records: List[Dict[str, Any]],
        conflict_columns: List[str],
        update_columns: List[str]
    ) -> int:
        """
        Bulk upsert (insert or update) records.
        
        Args:
            model_class: The SQLModel class to upsert into
            records: List of dictionaries containing record data
            conflict_columns: Columns to check for conflicts
            update_columns: Columns to update on conflict
            
        Returns:
            Number of records affected
        """
        if not records or "postgresql" not in str(self._engine.url):
            return 0
        
        # Convert model instances to dicts if needed
        if hasattr(records[0], 'dict'):
            records = [r.dict(exclude={'id', 'created_at'}) for r in records]
        
        with self.session_scope() as session:
            stmt = insert(model_class.__table__).values(records)
            
            # Create update dict for ON CONFLICT DO UPDATE
            update_dict = {col: stmt.excluded[col] for col in update_columns}
            
            stmt = stmt.on_conflict_do_update(
                index_elements=conflict_columns,
                set_=update_dict
            )
            
            result = session.execute(stmt)
            return result.rowcount
    
    def rollback(self) -> None:
        """Rollback the current transaction."""
        if self._session_factory is not None:
            session = self._session_factory()
            try:
                session.rollback()
            finally:
                session.close()
    
    def commit(self) -> None:
        """Commit the current transaction."""
        if self._session_factory is not None:
            session = self._session_factory()
            try:
                session.commit()
            finally:
                session.close()
    
    # Make the resource callable to get a session
    def __call__(self) -> Session:
        """Allow resource to be called directly to get a session."""
        return self.get_session()
    
    # Context manager support for sessions
    def __enter__(self) -> Session:
        """Enter context manager - return a session."""
        self._session = self.get_session()
        return self._session
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Exit context manager - close the session."""
        if hasattr(self, '_session'):
            if exc_type is not None:
                self._session.rollback()
            self._session.close()
            delattr(self, '_session')
    
    @property
    def bind(self):
        """Get the engine bind for raw SQL queries."""
        return self._engine
    
    @property
    def engine(self):
        """Get the SQLAlchemy engine."""
        return self._engine


# Create a pre-configured instance optimized for financial data
db_resource = DatabaseResource(
    echo=False,
    setup_db=True,
    pool_size=10,
    max_overflow=20,
    pool_pre_ping=True,
    statement_timeout=300000  # 5 minutes
)