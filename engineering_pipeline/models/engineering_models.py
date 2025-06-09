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


class EmmaPublicContracts(SQLModel, table=True):
    """Model for storing EMMA public contracts raw data"""
    __tablename__ = "emma_public_contracts"
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


class EmmaPublicSolicitationsSilver(SQLModel, table=True):
    """Model for storing parsed EMMA public solicitations data"""
    __tablename__ = "emma_public_solicitations_silver"
    __table_args__ = {"schema": "silver"}
    
    id: Optional[int] = Field(default=None, primary_key=True)
    solicitation_id: str = Field(
        nullable=False, 
        index=True,
        description="The BPM#### ID from EMMA"
    )
    title: Optional[str] = Field(default=None, description="Solicitation title")
    detail_url: Optional[str] = Field(default=None, description="URL to detailed solicitation page")
    status: Optional[str] = Field(default=None, description="Current status")
    due_close_date: Optional[str] = Field(default=None, description="Due/close date")
    publish_date: Optional[str] = Field(default=None, description="Publish date")
    main_category: Optional[str] = Field(default=None, description="Main category")
    solicitation_type: Optional[str] = Field(default=None, description="Type of solicitation")
    issuing_agency: Optional[str] = Field(default=None, description="Agency issuing the solicitation")
    auto_opening: Optional[str] = Field(default=None, description="Auto opening setting")
    round_number: Optional[str] = Field(default=None, description="Round number")
    award_status: Optional[str] = Field(default=None, description="Award status")
    procurement_officer: Optional[str] = Field(default=None, description="Procurement officer/buyer")
    authority: Optional[str] = Field(default=None, description="Authority")
    sub_agency: Optional[str] = Field(default=None, description="Sub agency")
    processed: bool = Field(default=False, description="Whether this record has been processed")
    created_at: datetime = Field(
        default_factory=datetime.utcnow,
        sa_column=Column(TIMESTAMP(timezone=True), nullable=False, server_default=text("CURRENT_TIMESTAMP")),
        description="When the record was created"
    )
    updated_at: datetime = Field(
        default_factory=datetime.utcnow,
        sa_column=Column(TIMESTAMP(timezone=True), nullable=False),
        description="When the record was last modified"
    )
    source_timestamp: datetime = Field(
        sa_column=Column(TIMESTAMP(timezone=True), nullable=False),
        description="Timestamp from the bronze record"
    )
    source_bronze_id: int = Field(
        description="Reference to the bronze record ID"
    )


class EmmaPublicContractsSilver(SQLModel, table=True):
    """Model for storing parsed EMMA public contracts data"""
    __tablename__ = "emma_public_contracts_silver"
    __table_args__ = {"schema": "silver"}
    
    id: Optional[int] = Field(default=None, primary_key=True)
    contract_code: str = Field(
        nullable=False, 
        index=True,
        description="The CTR#### code from EMMA"
    )
    contract_title: Optional[str] = Field(default=None, description="Contract title")
    detail_url: Optional[str] = Field(default=None, description="URL to detailed contract page")
    vendor: Optional[str] = Field(default=None, description="Vendor name")
    contract_type: Optional[str] = Field(default=None, description="Type of contract")
    effective_date: Optional[str] = Field(default=None, description="Contract effective date")
    expiration_date: Optional[str] = Field(default=None, description="Contract expiration date")
    linked_solicitation: Optional[str] = Field(default=None, description="Linked solicitation")
    publish_date: Optional[str] = Field(default=None, description="Publish date")
    public_solicitation_id: Optional[str] = Field(default=None, description="Public solicitation ID")
    processed: bool = Field(default=False, description="Whether this record has been processed")
    created_at: datetime = Field(
        default_factory=datetime.utcnow,
        sa_column=Column(TIMESTAMP(timezone=True), nullable=False, server_default=text("CURRENT_TIMESTAMP")),
        description="When the record was created"
    )
    updated_at: datetime = Field(
        default_factory=datetime.utcnow,
        sa_column=Column(TIMESTAMP(timezone=True), nullable=False),
        description="When the record was last modified"
    )
    source_timestamp: datetime = Field(
        sa_column=Column(TIMESTAMP(timezone=True), nullable=False),
        description="Timestamp from the bronze record"
    )
    source_bronze_id: int = Field(
        description="Reference to the bronze record ID"
    )


class EmmaSolicitationsGold(SQLModel, table=True):
    """Model for storing comprehensive EMMA solicitation details scraped from detail pages"""
    __tablename__ = "emma_solicitations_gold"
    __table_args__ = {"schema": "gold"}
    
    id: Optional[int] = Field(default=None, primary_key=True)
    solicitation_id: str = Field(
        nullable=False, 
        index=True,
        unique=True,
        description="The BPM#### ID from EMMA (unique)"
    )
    source_silver_id: Optional[int] = Field(default=None, description="Reference to silver record")
    detail_url: str = Field(nullable=False, description="Source detail URL")
    
    # Main Details
    title: Optional[str] = Field(default=None, description="Solicitation title")
    bmp_id: Optional[str] = Field(default=None, description="BMP ID")
    alternate_id: Optional[str] = Field(default=None, description="Alternate ID parsed from title")
    lot_number: Optional[str] = Field(default=None, description="Lot number")
    round_number: Optional[str] = Field(default=None, description="Round number")
    status: Optional[str] = Field(default=None, description="Current status")
    due_date: Optional[str] = Field(default=None, description="Due/close date")
    solicitation_type: Optional[str] = Field(default=None, description="Type of solicitation")
    main_category: Optional[str] = Field(default=None, description="Main category")
    issuing_agency: Optional[str] = Field(default=None, description="Issuing agency")
    procurement_officer: Optional[str] = Field(default=None, description="Procurement officer")
    email: Optional[str] = Field(default=None, description="Contact email")
    solicitation_summary: Optional[str] = Field(default=None, description="Solicitation summary")
    additional_instructions: Optional[str] = Field(default=None, description="Additional instructions")
    project_cost_class: Optional[str] = Field(default=None, description="Project cost class")
    
    # Participation Goals
    mbe_participation_pct: Optional[str] = Field(default=None, description="MBE participation percentage")
    african_american_pct: Optional[str] = Field(default=None, description="African American participation percentage")
    asian_american_pct: Optional[str] = Field(default=None, description="Asian American participation percentage")
    hispanic_american_pct: Optional[str] = Field(default=None, description="Hispanic American participation percentage")
    women_owned_pct: Optional[str] = Field(default=None, description="Women-owned participation percentage")
    dbe_participation_pct: Optional[str] = Field(default=None, description="DBE participation percentage")
    sbe_participation_pct: Optional[str] = Field(default=None, description="SBE participation percentage")
    vsbe_participation_pct: Optional[str] = Field(default=None, description="VSBE participation percentage")
    small_business_reserve: Optional[str] = Field(default=None, description="Small Business Reserve (SBR) designation")
    
    # Links and Attachments (JSON)
    solicitation_links: Optional[str] = Field(
        sa_column=Column(JSON),
        default=None,
        description="Navigation links as JSON array"
    )
    attachments: Optional[str] = Field(
        sa_column=Column(JSON),
        default=None,
        description="Document attachments as JSON array"
    )
    
    # Processing Metadata
    processed_at: datetime = Field(
        default_factory=datetime.utcnow,
        sa_column=Column(TIMESTAMP(timezone=True), nullable=False),
        description="When the record was processed"
    )
    created_at: datetime = Field(
        default_factory=datetime.utcnow,
        sa_column=Column(TIMESTAMP(timezone=True), nullable=False, server_default=text("CURRENT_TIMESTAMP")),
        description="When the record was created"
    )
    updated_at: datetime = Field(
        default_factory=datetime.utcnow,
        sa_column=Column(TIMESTAMP(timezone=True), nullable=False),
        description="When the record was last modified"
    )


class EmmaContractsGold(SQLModel, table=True):
    """Model for storing comprehensive EMMA contract details scraped from detail pages"""
    __tablename__ = "emma_contracts_gold"
    __table_args__ = {"schema": "gold"}
    
    id: Optional[int] = Field(default=None, primary_key=True)
    contract_id: str = Field(
        nullable=False, 
        index=True,
        unique=True,
        description="The CTR#### ID from EMMA (unique)"
    )
    source_silver_id: Optional[int] = Field(default=None, description="Reference to silver record")
    detail_url: str = Field(nullable=False, description="Source detail URL")
    
    # Core Contract Metadata
    contract_title: Optional[str] = Field(default=None, description="Full contract title")
    alternate_id: Optional[str] = Field(default=None, description="Alternate contract reference ID")
    contract_type: Optional[str] = Field(default=None, description="Contract type (Individual, Master, etc.)")
    effective_date: Optional[str] = Field(default=None, description="Contract effective date (MM/DD/YYYY)")
    expiration_date: Optional[str] = Field(default=None, description="Contract expiration date (MM/DD/YYYY)")
    
    # Financial Details
    contract_amount: Optional[str] = Field(default=None, description="Contract amount as string")
    currency: Optional[str] = Field(default=None, description="Currency (e.g., USD)")
    
    # Vendor & Agency Info
    vendor_name: Optional[str] = Field(default=None, description="Vendor/supplier name")
    procurement_officer: Optional[str] = Field(default=None, description="Procurement officer name")
    contact_email: Optional[str] = Field(default=None, description="Contact email address")
    agency_org: Optional[str] = Field(default=None, description="Agency/organization name")
    commodities: Optional[str] = Field(default=None, description="Commodities/services description")
    
    # Participation & Compliance Goals
    vsbe_goal_percentage: Optional[str] = Field(default=None, description="VSBE participation goal percentage")
    
    # Linkage & Content
    linked_solicitation: Optional[str] = Field(default=None, description="Linked solicitation ID")
    contract_scope: Optional[str] = Field(default=None, description="Contract scope (plain text)")
    documents_available: Optional[str] = Field(default=None, description="Available documents")
    
    # Processing Metadata
    processed_at: datetime = Field(
        default_factory=datetime.utcnow,
        sa_column=Column(TIMESTAMP(timezone=True), nullable=False),
        description="When the record was processed"
    )
    created_at: datetime = Field(
        default_factory=datetime.utcnow,
        sa_column=Column(TIMESTAMP(timezone=True), nullable=False, server_default=text("CURRENT_TIMESTAMP")),
        description="When the record was created"
    )
    updated_at: datetime = Field(
        default_factory=datetime.utcnow,
        sa_column=Column(TIMESTAMP(timezone=True), nullable=False),
        description="When the record was last modified"
    )