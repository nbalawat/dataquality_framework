import yaml
import logging

def load_config(config_file):
    """Load and parse the configuration file."""
    try:
        with open(config_file, 'r') as file:
            config = yaml.safe_load(file)
            logging.info(f"Configuration loaded from {config_file}")
            return config
    except Exception as e:
        logging.error(f"Failed to load configuration: {e}")
        raise

def parse_null_checks(config):
    """Parse null checks from configuration."""
    null_checks = []
    for table_config in config['tables']:
        dataset = table_config['dataset']
        table = table_config['table']
        for check in table_config['checks'].get('null_checks', []):
            null_checks.append({
                'dataset': dataset,
                'table': table,
                'column': check['column'],
                'threshold': check['threshold'],
                'filter': check.get('filter')
            })
    return null_checks

def parse_uniqueness_checks(config):
    """Parse uniqueness checks from configuration."""
    uniqueness_checks = []
    for table_config in config['tables']:
        dataset = table_config['dataset']
        table = table_config['table']
        for check in table_config['checks'].get('uniqueness_checks', []):
            uniqueness_checks.append({
                'dataset': dataset,
                'table': table,
                'columns': check['columns'],
                'threshold': check['threshold'],
                'filter': check.get('filter')
            })
    return uniqueness_checks

def parse_conditional_checks(config):
    """Parse conditional checks from configuration."""
    conditional_checks = []
    for table_config in config['tables']:
        dataset = table_config['dataset']
        table = table_config['table']
        for check in table_config['checks'].get('conditional_checks', []):
            conditional_checks.append({
                'dataset': dataset,
                'table': table,
                'condition': check['condition'],
                'description': check.get('description', ''),
                'threshold': check['threshold'],
                'filter': check.get('filter')
            })
    return conditional_checks

def parse_group_anomaly_detection(config):
    """Parse group anomaly detection settings from configuration."""
    group_configs = []
    for table_config in config['tables']:
        dataset = table_config['dataset']
        table = table_config['table']
        trend_analysis = table_config.get('trend_analysis', {})
        group_detection = trend_analysis.get('group_anomaly_detection', {})
        historical_data_points = group_detection.get('historical_data_points', 7)
        minimum_data_points = group_detection.get('minimum_data_points', 5)
        for group in group_detection.get('groups', []):
            group_configs.append({
                'dataset': dataset,
                'table': table,
                'columns': group['columns'],
                'anomaly_threshold': group['anomaly_threshold'],
                'historical_data_points': historical_data_points,
                'minimum_data_points': minimum_data_points,
                'filter': group.get('filter')
            })
    return group_configs
