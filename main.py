import logging
from datetime import datetime

from config_loader import (
    load_config,
    parse_null_checks,
    parse_uniqueness_checks,
    parse_conditional_checks,
    parse_group_anomaly_detection
)

from data_quality_checks import (
    execute_null_checks,
    execute_uniqueness_checks,
    execute_conditional_checks,
    collect_and_store_current_counts,
    analyze_group_anomalies,
    insert_results_into_bigquery,
    record_run_metadata
)

def main():
    run_id = f"run_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}"
    start_time = datetime.utcnow()
    logging.info(f"Data quality validation started with run ID: {run_id}")
    try:
        # Load configuration
        config = load_config('config.yaml')

        # Parse checks
        null_checks = parse_null_checks(config)
        uniqueness_checks = parse_uniqueness_checks(config)
        conditional_checks = parse_conditional_checks(config)
        group_configs = parse_group_anomaly_detection(config)

        results = []

        # Execute null checks
        results.extend(execute_null_checks(null_checks, run_id))

        # Execute uniqueness checks
        results.extend(execute_uniqueness_checks(uniqueness_checks, run_id))

        # Execute conditional checks
        results.extend(execute_conditional_checks(conditional_checks, run_id))

        # Collect and store current counts for anomaly detection
        collect_and_store_current_counts(group_configs, run_id)

        # Analyze group anomalies
        for group_config in group_configs:
            results.extend(analyze_group_anomalies(group_config, run_id))

        # Insert all results into the unified data_quality_results table
        insert_results_into_bigquery(results)

        end_time = datetime.utcnow()
        record_run_metadata(run_id, start_time, end_time, 'success')
        logging.info(f"Data quality validation completed successfully for run ID: {run_id}")

    except Exception as e:
        end_time = datetime.utcnow()
        error_message = str(e)
        record_run_metadata(run_id, start_time, end_time, 'failure', error_message)
        logging.exception(f"Data quality validation failed for run ID: {run_id}")
        # Optionally, send notifications or alerts

if __name__ == "__main__":
    main()
