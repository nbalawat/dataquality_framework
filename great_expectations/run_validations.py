import os
import yaml
from great_expectations.data_context import DataContext

def load_config(config_file):
    with open(config_file, 'r') as file:
        return yaml.safe_load(file)

def run_validations(config):
    context = DataContext()

    for table_config in config['tables']:
        dataset = table_config['dataset']
        table = table_config['table']
        data_asset_name = f"{dataset}.{table}"
        expectation_suite_name = f"{dataset}_{table}_suite".replace('.', '_')

        # Render the checkpoint configuration
        checkpoint_config = {
            "name": f"{dataset}_{table}_checkpoint",
            "config_version": 1.0,
            "class_name": "SimpleCheckpoint",
            "run_name_template": "%Y%m%d-%H%M%S-my-run-name-template",
            "validations": [
                {
                    "batch_request": {
                        "datasource_name": "my_bigquery_datasource",
                        "data_connector_name": "default_inferred_data_connector_name",
                        "data_asset_name": data_asset_name,
                    },
                    "expectation_suite_name": expectation_suite_name,
                }
            ],
        }

        # Add the checkpoint to the context
        context.add_checkpoint(**checkpoint_config)

        # Run the checkpoint
        results = context.run_checkpoint(checkpoint_name=checkpoint_config["name"])

        # Handle the results as needed
        if not results["success"]:
            print(f"Validation failed for {data_asset_name}")
        else:
            print(f"Validation passed for {data_asset_name}")

if __name__ == "__main__":
    config = load_config('data_quality_config.yaml')
    run_validations(config)
