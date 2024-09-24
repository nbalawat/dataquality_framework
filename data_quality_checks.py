import json
import logging
import time
from datetime import datetime
from google.cloud import bigquery
from google.api_core.exceptions import ServerError

# Set up logging if not already configured
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s',
    handlers=[logging.StreamHandler()]
)

def execute_query_with_retries(query, retries=3):
    """Execute a BigQuery SQL query with retry logic."""
    client = bigquery.Client()
    for attempt in range(retries):
        try:
            query_job = client.query(query)
            return query_job.result().to_dataframe()
        except ServerError as e:
            logging.warning(f"ServerError on attempt {attempt + 1}: {e}")
            time.sleep(2 ** attempt)
        except Exception as e:
            logging.error(f"Failed to execute query: {e}")
            raise
    logging.error("Max retries exceeded.")
    raise Exception("Max retries exceeded.")

def evaluate_threshold(metric_value, threshold):
    """Determine pass or fail based on the threshold."""
    return 'pass' if metric_value <= threshold else 'fail'

def generate_null_check_query(dataset, table, column, filter_condition=None):
    """Generate SQL for null checks with optional filter."""
    where_clause = f"WHERE {filter_condition}" if filter_condition else ""
    query = f"""
    SELECT
      COUNT(*) AS total_rows,
      SUM(CASE WHEN {column} IS NULL THEN 1 ELSE 0 END) AS null_count
    FROM `{dataset}.{table}`
    {where_clause};
    """
    return query

def generate_uniqueness_check_query(dataset, table, columns, filter_condition=None):
    """Generate SQL for uniqueness checks with optional filter."""
    columns_list = ', '.join(columns)
    cast_columns = ', '.join([f'CAST({col} AS STRING)' for col in columns])
    where_clause = f"WHERE {filter_condition}" if filter_condition else ""
    query = f"""
    SELECT
      COUNT(*) AS total_rows,
      COUNT(DISTINCT CONCAT({cast_columns})) AS unique_count
    FROM `{dataset}.{table}`
    {where_clause};
    """
    return query

def generate_conditional_check_query(dataset, table, condition, filter_condition=None):
    """Generate SQL for conditional checks with optional filter."""
    where_clause = f"WHERE {filter_condition}" if filter_condition else ""
    query = f"""
    SELECT
      COUNT(*) AS total_rows,
      SUM(CASE WHEN NOT ({condition}) THEN 1 ELSE 0 END) AS failure_count
    FROM `{dataset}.{table}`
    {where_clause};
    """
    return query

def generate_group_count_query(dataset, table, group_by_columns, filter_condition=None):
    """Generate SQL to count rows per group with optional filter."""
    group_by_clause = ', '.join(group_by_columns)
    where_clause = f"WHERE {filter_condition}" if filter_condition else ""
    query = f"""
    SELECT
      {group_by_clause},
      COUNT(*) AS row_count
    FROM `{dataset}.{table}`
    {where_clause}
    GROUP BY {group_by_clause};
    """
    return query

def execute_null_checks(null_checks, run_id):
    """Execute null checks and collect results."""
    results = []
    for check in null_checks:
        query = generate_null_check_query(
            check['dataset'],
            check['table'],
            check['column'],
            check.get('filter')
        )
        check_name = f"Null check on {check['column']}"
        try:
            result_df = execute_query_with_retries(query)
            total_rows = result_df['total_rows'][0]
            null_count = result_df['null_count'][0]
            status = evaluate_threshold(null_count, check['threshold'])
            results.append({
                'run_id': run_id,
                'dataset': check['dataset'],
                'table': check['table'],
                'check_type': 'null_check',
                'check_name': check_name,
                'columns': check['column'],
                'condition': None,
                'group_by_columns': None,
                'group_values': None,
                'threshold': check['threshold'],
                'metric_name': 'null_count',
                'metric_value': null_count,
                'expected_value': None,
                'total_rows': total_rows,
                'status': status,
                'filter_condition': check.get('filter'),
                'generated_sql': query,
                'error_message': None,
                'timestamp': datetime.utcnow().isoformat() + 'Z'
            })
        except Exception as e:
            logging.error(f"Null check failed for {check['table']}.{check['column']}: {e}")
            results.append({
                'run_id': run_id,
                'dataset': check['dataset'],
                'table': check['table'],
                'check_type': 'null_check',
                'check_name': check_name,
                'columns': check['column'],
                'condition': None,
                'group_by_columns': None,
                'group_values': None,
                'threshold': check['threshold'],
                'metric_name': 'null_count',
                'metric_value': None,
                'expected_value': None,
                'total_rows': None,
                'status': 'error',
                'filter_condition': check.get('filter'),
                'generated_sql': query,
                'error_message': str(e),
                'timestamp': datetime.utcnow().isoformat() + 'Z'
            })
    return results

def execute_uniqueness_checks(uniqueness_checks, run_id):
    """Execute uniqueness checks and collect results."""
    results = []
    for check in uniqueness_checks:
        query = generate_uniqueness_check_query(
            check['dataset'],
            check['table'],
            check['columns'],
            check.get('filter')
        )
        check_name = f"Uniqueness check on {', '.join(check['columns'])}"
        try:
            result_df = execute_query_with_retries(query)
            total_rows = result_df['total_rows'][0]
            unique_count = result_df['unique_count'][0]
            duplicates = total_rows - unique_count
            status = evaluate_threshold(duplicates, check['threshold'])
            results.append({
                'run_id': run_id,
                'dataset': check['dataset'],
                'table': check['table'],
                'check_type': 'uniqueness_check',
                'check_name': check_name,
                'columns': ', '.join(check['columns']),
                'condition': None,
                'group_by_columns': None,
                'group_values': None,
                'threshold': check['threshold'],
                'metric_name': 'duplicate_count',
                'metric_value': duplicates,
                'expected_value': None,
                'total_rows': total_rows,
                'status': status,
                'filter_condition': check.get('filter'),
                'generated_sql': query,
                'error_message': None,
                'timestamp': datetime.utcnow().isoformat() + 'Z'
            })
        except Exception as e:
            logging.error(f"Uniqueness check failed for {check['table']}.{check['columns']}: {e}")
            results.append({
                'run_id': run_id,
                'dataset': check['dataset'],
                'table': check['table'],
                'check_type': 'uniqueness_check',
                'check_name': check_name,
                'columns': ', '.join(check['columns']),
                'condition': None,
                'group_by_columns': None,
                'group_values': None,
                'threshold': check['threshold'],
                'metric_name': 'duplicate_count',
                'metric_value': None,
                'expected_value': None,
                'total_rows': None,
                'status': 'error',
                'filter_condition': check.get('filter'),
                'generated_sql': query,
                'error_message': str(e),
                'timestamp': datetime.utcnow().isoformat() + 'Z'
            })
    return results

def execute_conditional_checks(conditional_checks, run_id):
    """Execute conditional checks and collect results."""
    results = []
    for check in conditional_checks:
        query = generate_conditional_check_query(
            check['dataset'],
            check['table'],
            check['condition'],
            check.get('filter')
        )
        check_name = check.get('description', f"Conditional check: {check['condition']}")
        try:
            result_df = execute_query_with_retries(query)
            total_rows = result_df['total_rows'][0]
            failure_count = result_df['failure_count'][0]
            status = evaluate_threshold(failure_count, check['threshold'])
            results.append({
                'run_id': run_id,
                'dataset': check['dataset'],
                'table': check['table'],
                'check_type': 'conditional_check',
                'check_name': check_name,
                'columns': None,
                'condition': check['condition'],
                'group_by_columns': None,
                'group_values': None,
                'threshold': check['threshold'],
                'metric_name': 'failure_count',
                'metric_value': failure_count,
                'expected_value': None,
                'total_rows': total_rows,
                'status': status,
                'filter_condition': check.get('filter'),
                'generated_sql': query,
                'error_message': None,
                'timestamp': datetime.utcnow().isoformat() + 'Z'
            })
        except Exception as e:
            logging.error(f"Conditional check failed for {check['table']}: {e}")
            results.append({
                'run_id': run_id,
                'dataset': check['dataset'],
                'table': check['table'],
                'check_type': 'conditional_check',
                'check_name': check_name,
                'columns': None,
                'condition': check['condition'],
                'group_by_columns': None,
                'group_values': None,
                'threshold': check['threshold'],
                'metric_name': 'failure_count',
                'metric_value': None,
                'expected_value': None,
                'total_rows': None,
                'status': 'error',
                'filter_condition': check.get('filter'),
                'generated_sql': query,
                'error_message': str(e),
                'timestamp': datetime.utcnow().isoformat() + 'Z'
            })
    return results

def collect_and_store_current_counts(group_configs, run_id):
    """Collect current row counts per group and store in BigQuery."""
    client = bigquery.Client()
    table_id = 'your_project.your_dataset.row_count_history'  # Update this
    errors = []
    for group in group_configs:
        dataset = group['dataset']
        table = group['table']
        columns = group['columns']
        filter_condition = group.get('filter')
        query = generate_group_count_query(dataset, table, columns, filter_condition)
        try:
            result_df = execute_query_with_retries(query)
            # Prepare rows for insertion
            rows_to_insert = []
            for index, row in result_df.iterrows():
                group_values = {col: row[col] for col in columns}
                rows_to_insert.append({
                    'run_id': run_id,
                    'dataset': dataset,
                    'table': table,
                    'group_by_columns': ','.join(columns),
                    'group_values': json.dumps(group_values),
                    'row_count': row['row_count'],
                    'filter_condition': filter_condition,
                    'timestamp': datetime.utcnow()
                })
            # Insert into BigQuery
            errors.extend(client.insert_rows_json(table_id, rows_to_insert))
        except Exception as e:
            logging.error(f"Failed to collect/store counts for {table}: {e}")
    if errors:
        logging.error(f"Errors occurred during insertion: {errors}")

def get_historical_counts(dataset, table, group_by_columns, group_values,
                          historical_data_points):
    """Retrieve historical counts for a specific group."""
    client = bigquery.Client()
    table_id = 'your_project.your_dataset.row_count_history'  # Update this
    group_values_json = json.dumps(group_values)
    query = f"""
    SELECT
      row_count
    FROM `{table_id}`
    WHERE dataset = '{dataset}'
      AND table = '{table}'
      AND group_by_columns = '{','.join(group_by_columns)}'
      AND group_values = '{group_values_json}'
    ORDER BY timestamp DESC
    LIMIT {historical_data_points};
    """
    result_df = execute_query_with_retries(query)
    return result_df['row_count'].tolist()

def analyze_group_anomalies(group_config, run_id):
    """Analyze anomalies for a specific group configuration."""
    dataset = group_config['dataset']
    table = group_config['table']
    columns = group_config['columns']
    anomaly_threshold = group_config['anomaly_threshold']
    historical_data_points = group_config.get('historical_data_points', 7)
    minimum_data_points = group_config.get('minimum_data_points', 5)
    filter_condition = group_config.get('filter')

    # Get current counts
    query = generate_group_count_query(dataset, table, columns, filter_condition)
    try:
        current_counts_df = execute_query_with_retries(query)
    except Exception as e:
        logging.error(f"Failed to get current counts for anomaly detection: {e}")
        # Handle error appropriately
        return []

    results = []
    for index, row in current_counts_df.iterrows():
        group_values = {col: row[col] for col in columns}
        current_count = row['row_count']
        # Retrieve historical counts
        historical_counts = get_historical_counts(
            dataset,
            table,
            columns,
            group_values,
            historical_data_points
        )
        # Determine status
        if len(historical_counts) < minimum_data_points:
            status = 'insufficient_data'
            expected_count = None
        else:
            expected_count = sum(historical_counts) / len(historical_counts)
            change = abs(current_count - expected_count) / expected_count
            status = 'anomaly' if change >= anomaly_threshold else 'normal'
        # Prepare result
        check_name = f"Anomaly detection on {', '.join(columns)}"
        result = {
            'run_id': run_id,
            'dataset': dataset,
            'table': table,
            'check_type': 'anomaly_detection',
            'check_name': check_name,
            'columns': None,
            'condition': None,
            'group_by_columns': ', '.join(columns),
            'group_values': json.dumps(group_values),
            'threshold': anomaly_threshold,
            'metric_name': 'row_count',
            'metric_value': current_count,
            'expected_value': expected_count,
            'total_rows': None,
            'status': status,
            'filter_condition': filter_condition,
            'generated_sql': query,
            'error_message': None,
            'timestamp': datetime.utcnow().isoformat() + 'Z'
        }
        results.append(result)
    return results

def insert_results_into_bigquery(results):
    """Insert the results into a unified BigQuery table."""
    client = bigquery.Client()
    table_id = 'your_project.your_dataset.data_quality_results'  # Update this
    errors = client.insert_rows_json(table_id, results)
    if errors:
        logging.error(f"Failed to insert results into BigQuery: {errors}")
        raise Exception(f"Insertion errors: {errors}")
    else:
        logging.info(f"Results successfully inserted into {table_id}.")

def record_run_metadata(run_id, start_time, end_time, status, error_message=None):
    """Record metadata about the run in BigQuery."""
    client = bigquery.Client()
    table_id = 'your_project.your_dataset.data_quality_runs'  # Update this
    row = {
        'run_id': run_id,
        'start_time': start_time,
        'end_time': end_time,
        'status': status,
        'error_message': error_message
    }
    errors = client.insert_rows_json(table_id, [row])
    if errors:
        logging.error(f"Failed to record run metadata: {errors}")
    else:
        logging.info(f"Run metadata recorded for run ID: {run_id}")
