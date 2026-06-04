from smartcity.utils.config import load_yaml_config, get_project_root


def main():
    config = load_yaml_config("configs/paths.yaml")
    root = get_project_root(config)

    print("Project root:", root)
    print("Raw data path:", root / config["data"]["raw"])
    print("Interim data path:", root / config["data"]["interim"])
    print("Processed data path:", root / config["data"]["processed"])
    print("Reports path:", root / config["outputs"]["reports"])


if __name__ == "__main__":
    main()