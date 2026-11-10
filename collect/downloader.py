
"""
Download GoDaddy inventory ZIP files and upload validated archives to GCS.

Run from the project root:
    python -m collect.downloader --snapshot-date YYYY-MM-DD
"""

import argparse
import json
import logging
import os
import sys
import tempfile
import zipfile

from datetime import date, datetime, timezone
from pathlib import Path
from urllib.parse import quote

import requests
import yaml

from google.cloud import storage
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


# Project configuration
PROJECT_ROOT = Path(__file__).resolve().parents[1]
CONFIG_PATH = PROJECT_ROOT / "config" / "config.yaml"

CHUNK_SIZE = 1024 * 1024
TIMEOUT = (15, 120)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger(__name__)


def load_config():
    """Load project configuration."""

    if not CONFIG_PATH.exists():
        raise FileNotFoundError(
            f"Configuration file not found: {CONFIG_PATH}"
        )

    with CONFIG_PATH.open("r", encoding="utf-8") as file:
        config = yaml.safe_load(file)

    if not isinstance(config, dict):
        raise ValueError("Invalid configuration structure")

    if "source" not in config or "storage" not in config:
        raise ValueError(
            "Configuration must contain source and storage sections"
        )

    return config


def create_session():
    """Create an HTTP session with retry behavior."""

    retry = Retry(
        total=4,
        connect=4,
        read=4,
        backoff_factor=2,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
    )

    adapter = HTTPAdapter(max_retries=retry)

    session = requests.Session()
    session.mount("https://", adapter)
    session.mount("http://", adapter)

    return session


def load_manifest(bucket, manifest_path):
    """Read and validate the discovery manifest from GCS."""

    blob = bucket.blob(manifest_path)

    if not blob.exists():
        raise FileNotFoundError(
            f"Discovery manifest not found: "
            f"gs://{bucket.name}/{manifest_path}"
        )

    manifest = json.loads(blob.download_as_text())

    selected_files = manifest.get("selected_files")

    if not isinstance(selected_files, list) or not selected_files:
        raise ValueError(
            "Manifest must contain a non-empty selected_files list"
        )

    return selected_files


def is_valid_zip(file_path):
    """Check ZIP integrity, including CRC checks."""

    try:
        with zipfile.ZipFile(file_path, "r") as archive:
            return archive.testzip() is None

    except (zipfile.BadZipFile, OSError):
        return False


def download_and_upload(
    session,
    bucket,
    source_config,
    raw_prefix,
    snapshot_date,
    file_info,
    temporary_dir,
):
    """Download, validate and upload one inventory archive."""

    filename = file_info.get("file_name")
    expected_size = file_info.get("file_size")

    if (
        not isinstance(filename, str)
        or not filename
        or Path(filename).name != filename
        or "/" in filename
        or "\\" in filename
    ):
        raise ValueError(
            f"Invalid filename in manifest: {filename!r}"
        )

    if (
        not isinstance(expected_size, int)
        or isinstance(expected_size, bool)
        or expected_size <= 0
    ):
        raise ValueError(
            f"Invalid file_size for {filename}: {expected_size!r}"
        )

    base_url = source_config["base_url"].rstrip("/")
    url = f"{base_url}/{quote(filename, safe='')}"

    destination = Path(temporary_dir) / filename
    partial_file = Path(temporary_dir) / f"{filename}.part"

    logger.info("Downloading %s", filename)
    logger.info("Source: %s", url)

    try:
        downloaded_size = 0

        with session.get(
            url,
            stream=True,
            timeout=TIMEOUT,
        ) as response:

            response.raise_for_status()

            with partial_file.open("wb") as output:
                for chunk in response.iter_content(
                    chunk_size=CHUNK_SIZE
                ):
                    if chunk:
                        output.write(chunk)
                        downloaded_size += len(chunk)

        if downloaded_size != expected_size:
            raise ValueError(
                f"Size mismatch for {filename}: "
                f"expected {expected_size:,} bytes, "
                f"received {downloaded_size:,} bytes"
            )

        if not is_valid_zip(partial_file):
            raise zipfile.BadZipFile(
                f"ZIP integrity check failed: {filename}"
            )

        # Rename only after successful validation.
        os.replace(partial_file, destination)

        logger.info(
            "Download validated: %s (%s bytes)",
            filename,
            downloaded_size,
        )

        # Upload the validated original ZIP archive to GCS.
        object_name = (
            f"{raw_prefix.strip('/')}/"
            f"{snapshot_date}/"
            f"{filename}"
        )

        blob = bucket.blob(object_name)

        blob.upload_from_filename(
            str(destination),
            content_type="application/zip",
            checksum="auto",
        )

        logger.info(
            "Uploaded gs://%s/%s",
            bucket.name,
            object_name,
        )

        return {
            "file_name": filename,
            "file_size": downloaded_size,
            "gcs_uri": f"gs://{bucket.name}/{object_name}",
            "status": "success",
        }

    finally:
        # Remove temporary files regardless of success or failure.
        for temporary_file in (partial_file, destination):
            if temporary_file.exists():
                temporary_file.unlink()


def main():
    """Download all selected archives and upload them to GCS."""

    parser = argparse.ArgumentParser(
        description="Download GoDaddy inventory archives to GCS."
    )

    parser.add_argument(
        "--snapshot-date",
        default=datetime.now(timezone.utc).date().isoformat(),
        help="Collection date in YYYY-MM-DD format.",
    )

    args = parser.parse_args()

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

    snapshot_prefix = (
        f"{raw_prefix.strip('/')}/{snapshot_date}"
    )

    manifest_path = (
        f"{snapshot_prefix}/discovery_manifest.json"
    )

    logger.info("Starting GoDaddy inventory download")
    logger.info("Snapshot date: %s", snapshot_date)

    # Authenticate using Application Default Credentials.
    client = storage.Client()
    bucket = client.bucket(bucket_name)

    selected_files = load_manifest(
        bucket,
        manifest_path,
    )

    logger.info(
        "Loaded %d files from discovery manifest",
        len(selected_files),
    )

    session = create_session()

    successes = []
    failures = []

    # Temporary local storage is used only during execution.
    with tempfile.TemporaryDirectory(
        prefix="godaddy_download_"
    ) as temporary_dir:

        for file_info in selected_files:

            filename = file_info.get(
                "file_name",
                "<unknown>",
            )

            try:
                result = download_and_upload(
                    session=session,
                    bucket=bucket,
                    source_config=source_config,
                    raw_prefix=raw_prefix,
                    snapshot_date=snapshot_date,
                    file_info=file_info,
                    temporary_dir=temporary_dir,
                )

                successes.append(result)

            except Exception as error:
                failures.append((filename, str(error)))

                logger.exception(
                    "Failed to process %s",
                    filename,
                )

    logger.info(
        "Download summary: %d successful, %d failed",
        len(successes),
        len(failures),
    )

    for result in successes:
        logger.info(
            "%s | %s bytes | %s",
            result["file_name"],
            result["file_size"],
            result["gcs_uri"],
        )

    if failures:
        for filename, error in failures:
            logger.error("%s: %s", filename, error)

        raise RuntimeError(
            f"{len(failures)} inventory archive(s) failed"
        )

    logger.info(
        "All inventory archives uploaded successfully "
        "for snapshot %s",
        snapshot_date,
    )


if __name__ == "__main__":
    try:
        main()
    except Exception:
        logger.exception("Downloader pipeline failed")
        sys.exit(1)
