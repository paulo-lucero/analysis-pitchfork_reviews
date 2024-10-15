from pathlib import Path
import tomllib


def check_if_valid(config_data: dict):
    for config_key, config_val in config_data.items():
        if not isinstance(config_val, str):
            continue

        if not config_val.strip():
            raise ValueError(f'Configuration: \'{config_key}\' can\'t be empty string')
        

def get_db_config(toml_file: str):
    config_path = Path(toml_file).resolve()

    if not config_path.is_file():
        raise FileNotFoundError(f'Database config file isn\'t found {str(config_path)}')
    

    with open(str(config_path), 'rb') as config_file:
        config_db = tomllib.load(config_file)
        check_if_valid(config_db)

        return config_db