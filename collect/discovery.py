
import argparse
import json
import logging
import sys
from datetime import date, datetime, timezone
from pathlib import Path

import requests
import yaml
from google.cloud import storage


# Project paths
PROJECT_ROOT = Path(__file__).resolve().parents[1]
CONFIG_PATH = PROJECT_ROOT / "config" / "config.yaml"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger(__name__)


def load_config():
    """Load the project configuration."""

    if not CONFIG_PATH.exists():
        raise FileNotFoundError(
            f"Configuration file not found: {CONFIG_PATH}"
        )

    with open(CONFIG_PATH, "r", encoding="utf-8") as file:
        config = yaml.safe_load(file)

    if not isinstance(config, dict):
        raise ValueError("Invalid configuration structure")

    required_sections = ["source", "storage"]

    for section in required_sections:
        if section not in config:
            raise ValueError(
                f"Missing configuration section: {section}"
            )

    return config


def fetch_metadata(source_config):
    """Fetch the original GoDaddy metadata response."""

    metadata_url = (
        source_config["base_url"].rstrip("/")
        + "/"
        + source_config["metadata_file"].lstrip("/")
    )

    logger.info("Fetching metadata from %s", metadata_url)

    response = requests.get(
        metadata_url,
        timeout=60,
    )

    response.raise_for_status()

    metadata = response.json()

    if not isinstance(metadata, list):
        raise ValueError("Unexpected metadata structure")

    return metadata, response.content


def select_inventory_files(metadata, target_files):
    """Select the configured inventory files from metadata."""

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
            "last_modified": item["LastModified"],
        })

    return selected


def upload_to_gcs(
    bucket_name,
    object_name,
    content,
    content_type,
):
    """Upload content to Google Cloud Storage."""

    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(object_name)

    blob.upload_from_string(
        content,
        content_type=content_type,
    )

    logger.info(
        "Uploaded gs://%s/%s",
        bucket_name,
        object_name,
    )


def save_discovery_results(
    bucket_name,
    raw_prefix,
    snapshot_date,
    metadata_content,
    selected_files,
):
    """Save metadata and discovery manifest to GCS."""

    snapshot_prefix = (
        f"{raw_prefix.strip('/')}/{snapshot_date}"
    )

    metadata_object = (
        f"{snapshot_prefix}/metadata.json"
    )

    manifest_object = (
        f"{snapshot_prefix}/discovery_manifest.json"
    )

    # Preserve the original GoDaddy metadata response.
    upload_to_gcs(
        bucket_name,
        metadata_object,
        metadata_content,
        "application/json",
    )

    manifest = {
        "source": "godaddy",
        "snapshot_date": snapshot_date,
        "discovered_at": datetime.now(
            timezone.utc
        ).isoformat(),
        "selected_files": selected_files,
    }

    manifest_content = json.dumps(
        manifest,
        indent=4,
    )

    upload_to_gcs(
        bucket_name,
        manifest_object,
        manifest_content,
        "application/json",
    )


def main():
    parser = argparse.ArgumentParser(
        description="Discover GoDaddy auction inventory files."
    )

    parser.add_argument(
        "--snapshot-date",
        default=datetime.now(timezone.utc).date().isoformat(),
        help="Collection date in YYYY-MM-DD format.",
    )

    args = parser.parse_args()

    # Validate snapshot date.
    snapshot_date = date.fromisoformat(
        args.snapshot_date
    ).isoformat()

    config = load_config()

    source_config = config["source"]
    storage_config = config["storage"]

    bucket_name = storage_config["bucket"]
    raw_prefix = storage_config.get(
        "raw_prefix",
        "raw/snapshots",
    )

    logger.info("Starting GoDaddy inventory discovery")
    logger.info("Snapshot date: %s", snapshot_date)

    metadata, metadata_content = fetch_metadata(
        source_config
    )

    logger.info(
        "Discovered %s metadata entries",
        len(metadata),
    )

    selected_files = select_inventory_files(
        metadata,
        source_config["target_files"],
    )

    save_discovery_results(
        bucket_name,
        raw_prefix,
        snapshot_date,
        metadata_content,
        selected_files,
    )

    logger.info("Selected inventory files:")

    for item in selected_files:
        logger.info(
            "%s | %s bytes | source last_modified: %s",
            item["file_name"],
            item["file_size"],
            item["last_modified"],
        )

    logger.info(
        "Discovery completed successfully for snapshot %s",
        snapshot_date,
    )


if __name__ == "__main__":
    try:
        main()
    except Exception:
        logger.exception("Discovery pipeline failed")
        sys.exit(1)
