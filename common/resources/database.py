"""
Database resource for Dagster - Fixed for frozen model
"""
from dagster import ConfigurableResource, InitResourceContext
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.pool import NullPool
from pydantic import Field, PrivateAttr
import os
from typing import Optional

# Import SQLModel directly to access its metadata
from sqlmodel import SQLModel 


class DatabaseResource(ConfigurableResource):
    """
    Dagster resource for database connections using SQLAlchemy.
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
        description="Whether to create tables on startup"
    )
    
    pool_size: int = Field(
        default=5,
        description="Number of connections to maintain in pool"
    )
    
    max_overflow: int = Field(
        default=10,
        description="Maximum overflow connections allowed"
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
        
        # Create engine
        self._engine = create_engine(
            db_url,
            echo=self.echo,
            poolclass=NullPool if "sqlite" in db_url else None,
            pool_size=self.pool_size if "sqlite" not in db_url else None,
            max_overflow=self.max_overflow if "sqlite" not in db_url else None,
        )
        
        # Create session factory
        self._session_factory = sessionmaker(
            autocommit=False,
            autoflush=False,
            bind=self._engine
        )
        
        # Create tables if requested
        if self.setup_db:
            context.log.info("Creating database tables...")
            # Use SQLModel.metadata directly
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
    
    
    def rollback(self) -> None:
        """Rollback the current transaction."""
        if self._session_factory is not None:
            # Get current session and rollback
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


# Create a pre-configured instance
db_resource = DatabaseResource(
    echo=False,
    setup_db=True,
    pool_size=5,
    max_overflow=10
)