# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Development Commands

### Starting Dagster
```bash
# Activate conda environment and start Dagster dev server
source ~/miniconda3/etc/profile.d/conda.sh
conda activate dagster-env
cd /home/jgupdogg/dev/dagster
dagster dev -w workspace.yaml
```

### DBT Commands
DBT models must be compiled before starting Dagster:
```bash
cd dbt
dbt parse  # Generate manifest.json required by Dagster
dbt run    # Run all models
dbt test   # Run tests
dbt build  # Run and test all models
```

### Database Setup
```bash
# Run the schema setup script to create tiered schemas
python setup_tiered_schema.py
```

## Architecture Overview

This is a multi-pipeline Dagster project for cryptocurrency data analysis with two main pipelines:

### 1. Solana Pipeline (`solana_pipeline/`)
Analyzes Solana blockchain activity to detect trading opportunities:

- **Data Flow**: External APIs → Bronze assets → Silver (Python + DBT) → Gold (DBT marts) → Notifications/Reports
- **Key Integration Points**:
  - BirdEye API for token data (requires `BIRDEYE_API_KEY` env var)
  - Helius webhooks for real-time transactions
  - DBT for SQL transformations in `dbt/models/solana/`
  - Slack notifications via `SlackResource`
  - Google Sheets export via custom resource

- **Critical Assets**:
  - `webhook_staging_data`: Entry point for webhook processing
  - `mart_active_tokens`: Final DBT model combining all signals
  - `active_token_notification`: Sends alpha alerts to Slack

### 2. BTC Pipeline (`btc_pipeline/`)
Monitors Bitcoin market indicators:

- **Data Sources**: Web scraping via `UnifiedScraperResource`
- **Assets**: Fear & Greed Index, derivatives data (OI, funding rates, liquidations)
- **Storage**: SQLModel-based tables with automatic schema creation

### 3. Engineering Pipeline (`engineering_pipeline/`)
Scrapes EMMA Maryland public solicitations data:

- **Data Sources**: Web scraping EMMA Maryland government site
- **Assets**: `emma_public_solicitations` - captures raw HTML from public solicitations page
- **Storage**: PostgreSQL table `engineering.bronze.emma_public_solicitations`
- **Database**: Uses dedicated "engineering" database with bronze schema
- **Schedule**: On-demand scraping of https://emma.maryland.gov/page.aspx/en/rfp/request_browse_public

### Shared Resources (`common/resources/`)

- **DatabaseResource**: 
  - PostgreSQL connection with SQLModel ORM
  - Handles bulk operations with conflict resolution
  - Auto-creates schemas: bronze, silver, gold
  - Connection string from `DATABASE_URL` env var

- **UnifiedScraperResource**:
  - Selenium-based web scraper with retry logic
  - Configurable for headless/visible modes

### Key Architectural Patterns

1. **Medallion Architecture**: Raw data (Bronze) → Cleaned (Silver) → Analytics (Gold)
2. **Hybrid Processing**: Python assets for complex logic, DBT for SQL transformations
3. **Resource Isolation**: Each pipeline has dedicated resources configured in definitions.py
4. **Webhook Pattern**: Staging → Processing → Marking as processed to avoid duplicates
5. **Bulk Operations**: Use `bulk_upsert()` for efficient database writes

### Environment Variables Required
- `DATABASE_URL`: PostgreSQL connection string
- `BIRDEYE_API_KEY`: For Solana token data
- `SLACK_BOT_TOKEN`: For notifications
- `GOOGLE_SHEETS_CREDENTIALS`: Service account JSON

### Job Scheduling
- `whale_tracking_job`: Every 6 hours
- `webhook_processing_job`: Every 30 minutes  
- `btc_market_indicators_job`: Hourly
- `google_sheets_export_job`: Twice daily (8 AM, 8 PM)
- `emma_scraping_job`: On-demand EMMA public solicitations data collection

### DBT Integration
- DBT project in `dbt/` directory with Solana-specific models
- Models reference bronze/silver schemas created by Python assets
- `mart_active_tokens` is the key analytical model combining multiple data sources
- Always run `dbt parse` after modifying DBT files to regenerate manifest.json