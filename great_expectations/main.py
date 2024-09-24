import generate_expectations
import run_validations

if __name__ == "__main__":
    config = generate_expectations.load_config('data_quality_config.yaml')
    generate_expectations.generate_expectations_from_config(config)
    run_validations.run_validations(config)
