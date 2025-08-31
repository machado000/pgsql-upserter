"""Test column_matcher with real data from test_data.csv."""

import csv
from pgsql_upserter.config import test_connection
from pgsql_upserter.schema_inspector import inspect_table_schema
from pgsql_upserter.column_matcher import match_columns


def test_real_data_matching():
    """Test column matching with real test data and meta_ads_metrics table."""
    try:
        # Get database connection and schema
        connection = test_connection()
        schema = inspect_table_schema(connection, 'meta_ads_metrics', 'public')

        # Load test data
        data_list = []
        with open('tests/test_data.csv', 'r') as f:
            reader = csv.DictReader(f)
            for i, row in enumerate(reader):
                data_list.append(row)
                if i >= 4:  # Only load first 5 rows for testing
                    break

        print(f"Loaded {len(data_list)} rows from test_data.csv")
        print(f"First row keys: {list(data_list[0].keys())}")
        print(f"Table valid columns: {len(schema.valid_columns)} columns")

        # Test column matching
        result = match_columns(data_list, schema)

        print("\n=== COLUMN MATCHING RESULTS ===")
        print(f"Matched columns ({len(result['matched_columns'])}): {result['matched_columns']}")
        print(f"Ignored columns ({len(result['ignored_columns'])}): {result['ignored_columns']}")
        print(f"Missing columns ({len(result['missing_columns'])}): {result['missing_columns']}")

        # Test with ignore list
        print("\n=== WITH IGNORE LIST ===")
        ignore_list = ['view_content', 'add_to_cart', 'initiate_checkout', 'add_payment_info', 'purchase']
        result_with_ignore = match_columns(data_list, schema, ignore_columns=ignore_list)

        print(
            f"Matched columns ({len(result_with_ignore['matched_columns'])}): {result_with_ignore['matched_columns']}")
        print(
            f"Ignored columns ({len(result_with_ignore['ignored_columns'])}): {result_with_ignore['ignored_columns']}")
        print(
            f"Missing columns ({len(result_with_ignore['missing_columns'])}): {result_with_ignore['missing_columns']}")

        connection.close()

    except Exception as e:
        print(f"Error during testing: {e}")


if __name__ == "__main__":
    test_real_data_matching()
