"""Test temp staging with progress tracking and permanent table conversion."""

import csv
from pgsql_upserter.config import test_connection
from pgsql_upserter.schema_inspector import inspect_table_schema
from pgsql_upserter.column_matcher import match_columns
from pgsql_upserter.temp_staging import (
    create_and_populate_temp_table,
    convert_temp_to_permanent,
    bulk_insert_to_temp,
    create_temp_table
)


def test_progress_and_permanent_conversion():
    """Test progress tracking and permanent table conversion."""
    try:
        # Setup
        connection = test_connection()
        schema = inspect_table_schema(connection, 'meta_ads_metrics', 'public')

        # Load test data (duplicate it to simulate larger dataset)
        data_list = []
        with open('tests/test_data.csv', 'r') as f:
            reader = csv.DictReader(f)
            base_data = list(reader)[:5]  # Take first 5 rows

        # Duplicate data to simulate larger dataset (5000 rows total)
        for i in range(1000):
            for row in base_data:
                new_row = row.copy()
                new_row['id'] = str(int(row['id']) + i * 1000)  # Make IDs unique
                data_list.append(new_row)

        # Match columns
        column_result = match_columns(data_list, schema)
        matched_columns = column_result['matched_columns']

        print(f"=== TESTING WITH {len(data_list)} ROWS ===")

        # Test 1: Progress tracking
        print("\n=== TESTING PROGRESS TRACKING ===")
        temp_table_name, rows_inserted = create_and_populate_temp_table(
            connection, 'meta_ads_metrics', data_list, matched_columns, 'public', show_progress=True
        )
        print(f"Completed: {temp_table_name}, {rows_inserted} rows")

        # Test 2: Convert to permanent table
        print("\n=== TESTING PERMANENT TABLE CONVERSION ===")
        convert_temp_to_permanent(connection, temp_table_name, 'debug_staging_test', 'public')

        # Verify permanent table
        with connection.cursor() as cursor:
            cursor.execute("SELECT COUNT(*) FROM public.debug_staging_test")
            permanent_count = cursor.fetchone()[0]
            print(f"Permanent table has {permanent_count} rows")

            # Show sample data
            cursor.execute("SELECT account_id, campaign_id, spend FROM public.debug_staging_test LIMIT 3")
            sample_rows = cursor.fetchall()
            print(f"Sample data: {sample_rows}")

        connection.close()
        print("\nProgress and permanent conversion tests completed!")

    except Exception as e:
        print(f"Error during testing: {e}")
        raise


def test_progress_with_smaller_dataset():
    """Test that progress doesn't show for small datasets."""
    try:
        connection = test_connection()

        # Small dataset (should not show progress)
        small_data = [
            {'account_id': '123', 'campaign_id': '456'},
            {'account_id': '789', 'campaign_id': '012'}
        ]

        print("\n=== TESTING SMALL DATASET (NO PROGRESS) ===")
        temp_table = create_temp_table(connection, 'meta_ads_metrics', 'public')
        rows_inserted = bulk_insert_to_temp(
            connection, temp_table, small_data, ['account_id', 'campaign_id'], show_progress=True
        )
        print(f"Small dataset: {rows_inserted} rows (no progress shown)")

        connection.close()

    except Exception as e:
        print(f"Error during small dataset test: {e}")
        raise


if __name__ == "__main__":
    test_progress_and_permanent_conversion()
    test_progress_with_smaller_dataset()
