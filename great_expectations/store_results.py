from google.cloud import bigquery

def store_results_in_bigquery(validation_results):
    client = bigquery.Client()
    table_id = 'your_project.your_dataset.data_quality_results'

    rows_to_insert = []

    for result in validation_results:
        # Parse the result and prepare the row
        row = {
            'run_id': result['run_id'],
            'dataset': result['dataset'],
            'table': result['table'],
            'check_type': result['check_type'],
            'check_name': result['check_name'],
            'status': result['success'],
            # Add other fields as needed
        }
        rows_to_insert.append(row)

    errors = client.insert_rows_json(table_id, rows_to_insert)
    if errors:
        print(f"Failed to insert rows: {errors}")
    else:
        print("Validation results stored in BigQuery.")
