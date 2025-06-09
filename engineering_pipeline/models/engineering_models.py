"""
Engineering pipeline SQLModel models
"""
from sqlmodel import SQLModel, Field
from datetime import datetime
from typing import Optional, Dict, Any
from sqlalchemy import Column, JSON, TIMESTAMP, text


class EmmaPublicSolicitations(SQLModel, table=True):
    """Model for storing EMMA public solicitations raw data"""
    __tablename__ = "emma_public_solicitations"
    __table_args__ = {"schema": "bronze"}
    
    id: Optional[int] = Field(default=None, primary_key=True)
    timestamp: datetime = Field(
        sa_column=Column(TIMESTAMP(timezone=True), nullable=False),
        description="When the data was scraped"
    )
    value: Dict[str, Any] = Field(
        sa_column=Column(JSON, nullable=False),
        description="Raw JSON response from the API"
    )
    created_at: datetime = Field(
        default_factory=datetime.utcnow,
        sa_column=Column(TIMESTAMP(timezone=True), nullable=False, server_default=text("CURRENT_TIMESTAMP")),
        description="When the record was created"
    )