# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.9.0-beta] - 2025-08-31

### 🚀 Major Features

- **Flexible API**: `execute_upsert_workflow()` now accepts both direct data (API responses) and CSV file paths
- **Serverless ETL Ready**: Perfect for AWS Lambda, Google Cloud Functions, and other serverless platforms
- **Intelligent Conflict Resolution**: Automatic detection of primary keys and unique constraints for optimal upsert strategy
- **Complete Workflow Orchestration**: Single function handles schema inspection, column matching, deduplication, and upsert

### ✨ New Features

- **Direct Data Input**: Pass API response data directly without CSV intermediaries
- **CSV Helper Method**: `UpsertResult.read_csv_to_dict_list()` for data processing workflows
- **Automatic Schema Introspection**: Zero-configuration table analysis
- **Smart Column Matching**: Automatic mapping between data and table columns
- **Conflict Strategy Detection**: Three-tier strategy (Primary Key → Unique Constraints → Insert Only)
- **Data Deduplication**: Removes duplicates based on conflict resolution strategy
- **Comprehensive Results**: Detailed statistics on inserted, updated, and processed rows

### 🏗️ Architecture

- **Modular Design**: Clean separation between upsert engine and conflict resolution
- **Core Components**:
  - `upsert_engine.py`: Main public API and workflow orchestration
  - `conflict_resolver.py`: Conflict strategy detection and resolution primitives
  - `schema_inspector.py`: Database introspection and metadata
  - `column_matcher.py`: Data-to-schema column matching
  - `temp_staging.py`: Temporary table management

### 🔧 Technical Improvements

- **Native PostgreSQL Upserts**: Uses `INSERT...ON CONFLICT` for optimal performance
- **Temporary Table Strategy**: Efficient staging and deduplication
- **Type-Aware NULL Handling**: Proper handling of different data types
- **Connection Management**: Environment variable support and custom connections
- **Error Handling**: Comprehensive exception handling with `PgsqlUpserterError`

### 📊 Use Cases

- **API Data Ingestion**: Facebook Ads, Google Ads, LinkedIn Ads, and other marketing APIs
- **Serverless ETL Pipelines**: Event-driven data processing in cloud functions
- **Data Warehouse Loading**: Batch and real-time data integration
- **Traditional CSV Processing**: File-based data workflows

### 🎯 Breaking Changes

- Initial beta release - no breaking changes from previous versions

### 📦 Dependencies

- Python 3.11+
- PostgreSQL 12+
- psycopg2
- Standard library only (no external dependencies beyond database driver)

### 🔮 Coming Soon

- Container-based testing environment
- Performance optimizations for large datasets
- Additional conflict resolution strategies
- Streaming data support
- Enhanced logging and monitoring
