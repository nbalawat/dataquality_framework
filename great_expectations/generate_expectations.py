import yaml
import re
from great_expectations.core.batch import BatchRequest
from great_expectations.data_context import DataContext
from great_expectations.exceptions import DataContextError

def sanitize_name(name):
    """Sanitize names to be used as expectation suite names."""
    return re.sub(r'[^a-zA-Z0-9_]', '_', name)

def load_config(config_file):
    """Load the YAML configuration file."""
    with open(config_file, 'r') as file:
        return yaml.safe_load(file)

def generate_expectations_from_config(config):
    context = DataContext()

    datasource_name = "my_bigquery_datasource"  # Update if necessary

    for table_config in config['tables']:
        dataset = table_config['dataset']
        table = table_config['table']
        checks = table_config.get('checks', {})

        data_asset_name = f"{dataset}.{table}"
        suite_name = sanitize_name(f"{dataset}_{table}_suite")
        
        # Create or load the expectation suite
        try:
            suite = context.create_expectation_suite(suite_name, overwrite_existing=True)
        except DataContextError:
            suite = context.get_expectation_suite(suite_name)
        
        # Define the batch request
        batch_request = BatchRequest(
            datasource_name=datasource_name,
            data_connector_name="default_inferred_data_connector_name",
            data_asset_name=data_asset_name,
        )
        
        # Get a validator
        validator = context.get_validator(
            batch_request=batch_request,
            expectation_suite_name=suite_name,
        )

        # Apply null checks
        for check in checks.get('null_checks', []):
            column = check['column']
            threshold = check.get('threshold', 0.0)
            mostly = 1.0 - threshold
            filter_condition = check.get('filter')

            if filter_condition:
                # Apply filter using a custom SQL query
                validator.execution_engine.engine.execute(f"SET @@session.max_execution_time=10000")
                validator = validator.clone_with_new_batch_request(
                    batch_request=batch_request,
                    custom_sql=f"SELECT * FROM `{data_asset_name}` WHERE {filter_condition}"
                )

            validator.expect_column_values_to_not_be_null(
                column,
                mostly=mostly
            )

        # Apply uniqueness checks
        for check in checks.get('uniqueness_checks', []):
            columns = check['columns']
            threshold = check.get('threshold', 0.0)
            mostly = 1.0 - threshold
            filter_condition = check.get('filter')

            if filter_condition:
                validator.execution_engine.engine.execute(f"SET @@session.max_execution_time=10000")
                validator = validator.clone_with_new_batch_request(
                    batch_request=batch_request,
                    custom_sql=f"SELECT * FROM `{data_asset_name}` WHERE {filter_condition}"
                )

            if len(columns) == 1:
                validator.expect_column_values_to_be_unique(
                    column=columns[0],
                    mostly=mostly
                )
            else:
                validator.expect_compound_columns_to_be_unique(
                    column_list=columns
                )

        # Apply conditional checks
        for check in checks.get('conditional_checks', []):
            condition = check['condition']
            threshold = check.get('threshold', 0.0)
            mostly = 1.0 - threshold
            description = check.get('description', '')
            filter_condition = check.get('filter')

            if filter_condition:
                validator.execution_engine.engine.execute(f"SET @@session.max_execution_time=10000")
                validator = validator.clone_with_new_batch_request(
                    batch_request=batch_request,
                    custom_sql=f"SELECT * FROM `{data_asset_name}` WHERE {filter_condition}"
                )

            validator.expect_condition_to_be_true(
                condition=condition,
                mostly=mostly,
                result_format="SUMMARY"
            )

        # Save the expectation suite
        validator.save_expectation_suite()

if __name__ == "__main__":
    config = load_config('data_quality_config.yaml')
    generate_expectations_from_config(config)
