
import json
from pathlib import Path
from datetime import datetime, timezone

import requests
import yaml


COLLECT_DIR = Path(__file__).resolve().parent
DISCOVERY_DIR = COLLECT_DIR / "discovery"

CONFIG_PATH = r"config\config.yaml"


def load_config():
    with open(CONFIG_PATH, "r", encoding="utf-8") as file:
        return yaml.safe_load(file)


def fetch_metadata(source_config):

    metadata_url = (
        source_config["base_url"]
        + source_config["metadata_file"]
    )

    response = requests.get(
        metadata_url,
        timeout=60
    )

    response.raise_for_status()

    metadata = response.json()

    if not isinstance(metadata, list):
        raise ValueError("Unexpected metadata structure")

    return metadata, response.content


def select_inventory_files(metadata, target_files):

    indexed_files = {
        item["FileName"]: item
        for item in metadata
    }

    selected = []

    for filename in target_files:

        if filename not in indexed_files:
            raise ValueError(
                f"Inventory file not found: {filename}"
            )

        item = indexed_files[filename]

        selected.append({
            "file_name": item["FileName"],
            "file_size": item["FileSize"],
            "file_type": item["FileType"],
            "last_modified": item["LastModified"]
        })

    return selected


def save_discovery_results(
    metadata_content,
    selected_files,
    output_path
):

    output_path.mkdir(
        parents=True,
        exist_ok=True
    )

    # Preserve the original metadata response
    (output_path / "metadata.json").write_bytes(
        metadata_content
    )

    manifest = {
        "source": "godaddy",
        "discovered_at": datetime.now(
            timezone.utc
        ).isoformat(),
        "selected_files": selected_files
    }

    with open(
        output_path / "discovery_manifest.json",
        "w",
        encoding="utf-8"
    ) as file:

        json.dump(
            manifest,
            file,
            indent=4
        )


def main():

    config = load_config()

    source_config = config["source"]

    print("Starting GoDaddy inventory discovery...")

    metadata, metadata_content = fetch_metadata(
        source_config
    )

    print(f"Discovered {len(metadata)} inventory entries.")

    selected_files = select_inventory_files(
        metadata,
        source_config["target_files"]
    )

    save_discovery_results(
        metadata_content,
        selected_files,
        DISCOVERY_DIR
    )

    print("\nSelected inventory files:")

    for item in selected_files:
        print(
            f"- {item['file_name']} | "
            f"{item['file_size']:,} bytes | "
            f"{item['last_modified']}"
        )

    print("\nDiscovery completed successfully!")


if __name__ == "__main__":
    main()
