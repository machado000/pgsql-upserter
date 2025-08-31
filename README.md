# pgsql_upserter

[![PyPI version](https://img.shields.io/pypi/v/pgsql-upserter)](https://pypi.org/project/pgsql-upserter/)
[![License](https://img.shields.io/github/license/machado000/pgsql-upserter)](https://github.com/machado000/pgsql-upserter/blob/main/LICENSE)
[![Issues](https://img.shields.io/github/issues/machado000/pgsql-upserter)](https://github.com/machado000/pgsql-upserter/issues)
[![Last Commit](https://img.shields.io/github/last-commit/machado000/pgsql-upserter)](https://github.com/machado000/pgsql-upserter/commits/main)

A powerful, production-ready PostgreSQL upsert utility with automatic schema introspection and intelligent conflict resolution. Perfect for serverless ETL pipelines and data integration workflows.

## 🚀 Key Features

- **Zero Configuration**: Automatic schema detection and column matching
- **Intelligent Conflict Resolution**: Automatically detects primary keys and unique constraints
- **Production Tested**: Handles deduplication, data validation, and error recovery
- **Flexible Input**: Supports both direct data (API responses) and CSV files

## 📦 Installation

```bash
pip install pgsql-upserter
```

## 🎯 Quick Start

### Serverless ETL (Recommended)

Perfect for AWS Lambda, Google Cloud Functions, or any API-driven ETL:

```python
from pgsql_upserter import execute_upsert_workflow, create_connection_from_env

# Your API response data (Facebook Ads, Google Ads, etc.)
api_data = [
    {
        'account_id': '123456789',
        'campaign_id': 'camp_001', 
        'impressions': 1000,
        'clicks': 50,
        'spend': 25.50,
        'date_start': '2025-08-31'
    }
    # ... more records
]

# One function call does everything!
connection = create_connection_from_env()
result = execute_upsert_workflow(
    connection=connection,
    data=api_data,  # Direct API data
    target_table='ads_metrics'
)

print(f"✅ {result.total_affected} rows processed")
print(f"📈 {result.rows_inserted} inserted, {result.rows_updated} updated")
```

### CSV File Processing

```python
# Automatic CSV processing
result = execute_upsert_workflow(
    connection=connection,
    data='path/to/data.csv',  # File path
    target_table='ads_metrics'
)
```

## 🔧 Environment Setup

Set your PostgreSQL connection via environment variables:

```bash
export PGHOST=your-host
export PGPORT=5432
export PGDATABASE=your-db
export PGUSER=your-user
export PGPASSWORD=your-password
```

Or use a connection string:
```bash
export DATABASE_URL=postgresql://user:pass@host:port/dbname
```

## 🧠 How It Works

1. **Schema Introspection**: Analyzes your table structure automatically
2. **Column Matching**: Maps your data columns to table columns
3. **Conflict Detection**: Finds primary keys and unique constraints  
4. **Data Deduplication**: Removes duplicates using conflict resolution strategy
5. **Intelligent Upsert**: Uses PostgreSQL's native `INSERT...ON CONFLICT`

## 🎯 Perfect For

- **API Data Ingestion**: Facebook Ads, Google Ads, LinkedIn Ads APIs
- **Serverless ETL**: AWS Lambda, Google Cloud Functions, Azure Functions
- **Data Warehousing**: Loading data into analytics databases
- **Real-time Sync**: Keeping databases in sync with external sources
- **Batch Processing**: Traditional CSV and file-based workflows

## 📊 Automatic Conflict Resolution

The library automatically chooses the best upsert strategy:

1. **Primary Key**: Uses table's primary key if available in data
2. **Unique Constraints**: Combines all unique constraints for conflict detection  
3. **Insert Only**: Falls back to simple insert if no conflicts possible

## 🔍 Advanced Usage

### Data Processing Before Upsert

```python
from pgsql_upserter import UpsertResult

# Read and process CSV data
csv_data = UpsertResult.read_csv_to_dict_list('data.csv')

# Filter or transform data
filtered_data = [row for row in csv_data if float(row.get('spend', 0)) > 10.0]

# Upsert processed data
result = execute_upsert_workflow(
    connection=connection,
    data=filtered_data,
    target_table='ads_metrics'
)
```

### Custom Connection

```python
import psycopg2
from pgsql_upserter import execute_upsert_workflow

connection = psycopg2.connect(
    host="localhost",
    database="mydb",
    user="user",
    password="password"
)

result = execute_upsert_workflow(
    connection=connection,
    data=your_data,
    target_table='your_table',
    schema_name='public'  # optional, defaults to 'public'
)
```

## 🛡️ Error Handling

The library provides comprehensive error handling and validation:

```python
from pgsql_upserter import execute_upsert_workflow, PgsqlUpserterError

try:
    result = execute_upsert_workflow(connection, data, 'my_table')
    print(f"Success: {result.total_affected} rows processed")
except PgsqlUpserterError as e:
    print(f"Upsert failed: {e}")
```

## 📋 Requirements

- Python 3.11+
- PostgreSQL 12+
- psycopg2-binary

## 🤝 Contributing

Issues and pull requests are welcome! Please see our contributing guidelines.

## 📄 License

MIT License - see LICENSE file for details.

## 🔗 Links

- [PyPI Package](https://pypi.org/project/pgsql-upserter/)
- [Source Code](https://github.com/machadoo000/pgsql-upserter)
- [Issues](https://github.com/machadoo000/pgsql-upserter/issues)
