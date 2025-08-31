"""Tests for temp_staging module with real data and permanent table validation."""

import csv
from pgsql_upserter.config import test_connection
from pgsql_upserter.schema_inspector import inspect_table_schema
from pgsql_upserter.column_matcher import match_columns
from pgsql_upserter.temp_staging import create_and_populate_temp_table, convert_temp_to_permanent


def test_temp_staging_with_full_csv_data():
    """Test temp staging with full CSV data and convert to permanent table for validation."""
    try:
        # Setup
        connection = test_connection()
        schema = inspect_table_schema(connection, 'meta_ads_metrics', 'public')

        # Load ALL test data from CSV
        data_list = []
        with open('tests/test_data.csv', 'r') as f:
            reader = csv.DictReader(f)
            data_list = list(reader)

        print(f"Loaded {len(data_list)} rows from test_data.csv")

        # Match columns
        column_result = match_columns(data_list, schema)
        matched_columns = column_result['matched_columns']

        print(f"Matched {len(matched_columns)} columns")
        print(f"Ignored columns: {column_result['ignored_columns']}")
        print(f"Missing columns: {column_result['missing_columns']}")

        # Create temp table and populate with progress tracking
        print("\n=== CREATING TEMP TABLE AND INSERTING DATA ===")
        temp_table_name, rows_inserted = create_and_populate_temp_table(
            connection, 'meta_ads_metrics', data_list, matched_columns, 'public', show_progress=True
        )

        print(f"Temporary table created: {temp_table_name}")
        print(f"Rows inserted: {rows_inserted}")

        # Verify data in temp table
        with connection.cursor() as cursor:
            cursor.execute(f"SELECT COUNT(*) FROM {temp_table_name}")
            temp_count = cursor.fetchone()[0]
            print(f"Verified temp table count: {temp_count} rows")

            # Show sample data from temp table
            cursor.execute(f"""
                SELECT account_id, campaign_id, spend, impressions, date_start
                FROM {temp_table_name}
                LIMIT 5
            """)
            sample_rows = cursor.fetchall()
            print("Sample temp table data (first 5 rows):")
            for row in sample_rows:
                print(f"  {row}")

        # Convert to permanent table for database validation
        permanent_table_name = "test_staging_validation"
        print(f"\n=== CONVERTING TO PERMANENT TABLE: {permanent_table_name} ===")

        convert_temp_to_permanent(connection, temp_table_name, permanent_table_name, 'public')

        # Verify permanent table
        with connection.cursor() as cursor:
            cursor.execute(f"SELECT COUNT(*) FROM public.{permanent_table_name}")
            permanent_count = cursor.fetchone()[0]
            print(f"Permanent table '{permanent_table_name}' created with {permanent_count} rows")

            # Show table structure
            cursor.execute(f"""
                SELECT column_name, data_type, is_nullable
                FROM information_schema.columns
                WHERE table_schema = 'public' AND table_name = '{permanent_table_name}'
                ORDER BY ordinal_position
                LIMIT 10
            """)
            columns_info = cursor.fetchall()
            print("Table structure (first 10 columns):")
            for col_info in columns_info:
                print(f"  {col_info[0]} ({col_info[1]}) - nullable: {col_info[2]}")

            # Show sample data from permanent table
            cursor.execute(f"""
                SELECT account_id, campaign_id, spend, impressions, date_start
                FROM public.{permanent_table_name}
                ORDER BY account_id, campaign_id, date_start
                LIMIT 10
            """)
            permanent_sample = cursor.fetchall()
            print("Sample permanent table data (first 10 rows):")
            for row in permanent_sample:
                print(f"  {row}")

            # Data validation checks
            print("\n=== DATA VALIDATION CHECKS ===")

            # Check for NULL values in key columns
            cursor.execute(f"""
                SELECT
                    COUNT(*) as total_rows,
                    COUNT(account_id) as non_null_account_id,
                    COUNT(campaign_id) as non_null_campaign_id,
                    COUNT(spend) as non_null_spend,
                    COUNT(date_start) as non_null_date_start
                FROM public.{permanent_table_name}
            """)
            null_check = cursor.fetchone()
            print(f"NULL check - Total: {null_check[0]}, Account ID: {null_check[1]}, "
                  f"Campaign ID: {null_check[2]}, Spend: {null_check[3]}, Date Start: {null_check[4]}")

            # Check data types and ranges
            cursor.execute(f"""
                SELECT
                    MIN(spend) as min_spend,
                    MAX(spend) as max_spend,
                    AVG(spend) as avg_spend,
                    MIN(date_start) as min_date,
                    MAX(date_start) as max_date
                FROM public.{permanent_table_name}
                WHERE spend IS NOT NULL AND date_start IS NOT NULL
            """)
            range_check = cursor.fetchone()
            print(f"Data ranges - Spend: {range_check[0]} to {range_check[1]} (avg: {range_check[2]:.2f})")
            print(f"Date range: {range_check[3]} to {range_check[4]}")

        connection.close()

        print(f"\n✅ SUCCESS: Data successfully loaded into permanent table 'public.{permanent_table_name}'")
        print("You can now validate the data directly in your database by querying:")
        print(f"  SELECT * FROM public.{permanent_table_name} LIMIT 10;")

    except Exception as e:
        print(f"❌ ERROR during testing: {e}")
        raise


def test_null_handling_validation():
    """Test NULL handling with specific test cases."""
    try:
        connection = test_connection()

        # Test data with various NULL representations
        test_data_with_nulls = [
            {'account_id': '123', 'campaign_id': '', 'spend': 'null', 'impressions': '100'},
            {'account_id': 'none', 'campaign_id': '456', 'spend': '-', 'impressions': 'NaN'},
            {'account_id': '789', 'campaign_id': 'NA', 'spend': '25.50', 'impressions': ''},
            {'account_id': '', 'campaign_id': '999', 'spend': '0', 'impressions': 'null'}
        ]

        print("\n=== TESTING NULL HANDLING ===")
        temp_table_name, rows_inserted = create_and_populate_temp_table(
            connection, 'meta_ads_metrics', test_data_with_nulls,
            ['account_id', 'campaign_id', 'spend', 'impressions'], 'public'
        )

        # Convert to permanent table for inspection
        null_test_table = "test_null_handling_validation"
        convert_temp_to_permanent(connection, temp_table_name, null_test_table, 'public')

        # Check NULL conversion results
        with connection.cursor() as cursor:
            cursor.execute(f"""
                SELECT account_id, campaign_id, spend, impressions
                FROM public.{null_test_table}
                ORDER BY COALESCE(account_id, 'zzz')
            """)
            null_results = cursor.fetchall()
            print(f"NULL conversion results in 'public.{null_test_table}':")
            for row in null_results:
                print(f"  {row}")

        connection.close()
        print(f"✅ NULL handling test completed. Check 'public.{null_test_table}' in database.")

    except Exception as e:
        print(f"❌ ERROR during NULL testing: {e}")
        raise


if __name__ == "__main__":
    test_temp_staging_with_full_csv_data()
    test_null_handling_validation()
