from pathlib import Path
import yaml


def load_yaml_config(config_path: str | Path) -> dict:
    config_path = Path(config_path)

    if not config_path.exists():
        raise FileNotFoundError(f"Config file not found: {config_path}")

    with open(config_path, "r", encoding="utf-8") as file:
        return yaml.safe_load(file)


def get_project_root(config: dict) -> Path:
    return Path(config.get("project_root", ".")).resolve()