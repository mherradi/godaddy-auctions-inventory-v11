
"""
Extract GoDaddy inventory ZIP archives from GCS.

Run from the project root:
    python -m collect.extractor --snapshot-date YYYY-MM-DD
"""

import argparse
import json
import logging
import tempfile
import zipfile

from datetime import date, datetime, timezone
from pathlib import Path, PurePosixPath

import yaml
from google.cloud import storage


# --------------------------------------------------
# Configuration
# --------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parents[1]
CONFIG_PATH = PROJECT_ROOT / "config" / "config.yaml"

CHUNK_SIZE = 1024 * 1024  # 1 MB
ALLOWED_EXTENSION = ".json"


# --------------------------------------------------
# Logging
# --------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

logger = logging.getLogger(__name__)


# --------------------------------------------------
# Helpers
# --------------------------------------------------

def utc_now():
    """Return the current UTC timestamp."""
    return datetime.now(timezone.utc).isoformat()


def load_config():
    """Load project configuration."""

    with CONFIG_PATH.open("r", encoding="utf-8") as file:
        config = yaml.safe_load(file)

    if not isinstance(config, dict):
        raise ValueError(
            "Configuration file must contain a YAML mapping."
        )

    return config


def get_json_member(bucket, archive_object):
    """Find the single JSON member inside a GCS ZIP archive."""

    # Use a temporary directory rather than NamedTemporaryFile.
    # This avoids Windows file-locking issues during GCS downloads.
    with tempfile.TemporaryDirectory(
        prefix="godaddy_inspect_"
    ) as temporary_dir:

        archive_path = Path(temporary_dir) / "archive.zip"

        blob = bucket.blob(archive_object)
        blob.download_to_filename(str(archive_path))

        with zipfile.ZipFile(archive_path, "r") as zip_file:

            members = [
                item
                for item in zip_file.infolist()
                if not item.is_dir()
                and PurePosixPath(item.filename).suffix.lower()
                == ALLOWED_EXTENSION
            ]

    if len(members) != 1:
        raise ValueError(
            f"Expected exactly one JSON file in "
            f"{archive_object}; found {len(members)}."
        )

    member = members[0]
    member_path = PurePosixPath(member.filename)

    # Prevent unsafe paths and unexpected directory structures.
    if (
        member_path.is_absolute()
        or len(member_path.parts) != 1
        or member.filename in ("", ".", "..")
        or "\\" in member.filename
    ):
        raise ValueError(
            f"Unexpected path inside archive: {member.filename}"
        )

    return member


def load_previous_report(bucket, report_object):
    """Load the extraction report for this snapshot, if available."""

    blob = bucket.blob(report_object)

    if not blob.exists():
        return {}

    try:
        report = json.loads(blob.download_as_text())

        return {
            item["archive"]: item
            for item in report.get("extractions", [])
            if "archive" in item
        }

    except (json.JSONDecodeError, OSError, TypeError):
        logger.warning(
            "Previous extraction report could not be read."
        )
        return {}


def extract_archive(
    bucket,
    archive_object,
    output_prefix,
    previous_report,
):
    """Validate and extract one GCS ZIP archive."""

    archive_name = PurePosixPath(archive_object).name

    logger.info("Processing archive: %s", archive_name)

    # Retrieve the archive and its metadata.
    archive_blob = bucket.get_blob(archive_object)

    if archive_blob is None:
        raise FileNotFoundError(
            f"GCS archive not found: "
            f"gs://{bucket.name}/{archive_object}"
        )

    if archive_blob.size is None:
        raise RuntimeError(
            f"Could not retrieve archive size: {archive_object}"
        )

    archive_size = int(archive_blob.size)

    # Locate the JSON member inside the archive.
    member = get_json_member(
        bucket,
        archive_object,
    )

    destination_object = (
        f"{output_prefix.rstrip('/')}/{member.filename}"
    )

    destination_blob = bucket.blob(destination_object)

    # Skip extraction only when the previous report and GCS object
    # confirm that the existing output matches this archive.
    previous = previous_report.get(archive_name, {})

    can_skip = (
        destination_blob.exists()
        and destination_blob.size is not None
        and int(destination_blob.size) == member.file_size
        and previous.get("member") == member.filename
        and previous.get("archive_size") == archive_size
        and previous.get("member_size") == member.file_size
        and previous.get("output_object") == destination_object
        and previous.get("status") in (
            "success",
            "skipped_existing",
        )
    )

    if can_skip:
        logger.info(
            "Already extracted; skipping: %s",
            destination_object,
        )

        return {
            "archive": archive_name,
            "member": member.filename,
            "archive_size": archive_size,
            "member_size": member.file_size,
            "output_object": destination_object,
            "status": "skipped_existing",
            "extracted_at": previous.get("extracted_at"),
        }

    # Download and extract using a temporary directory.
    with tempfile.TemporaryDirectory(
        prefix="godaddy_extract_"
    ) as temporary_dir:

        archive_path = Path(temporary_dir) / archive_name
        extracted_path = Path(temporary_dir) / member.filename

        archive_blob.download_to_filename(
            str(archive_path)
        )

        with zipfile.ZipFile(archive_path, "r") as zip_file:

            # Validate ZIP CRC before extraction.
            bad_member = zip_file.testzip()

            if bad_member is not None:
                raise zipfile.BadZipFile(
                    f"Corrupt member found: {bad_member}"
                )

            logger.info(
                "Extracting: %s",
                member.filename,
            )

            bytes_written = 0

            with zip_file.open(member, "r") as source:
                with extracted_path.open("wb") as target:

                    while True:
                        chunk = source.read(CHUNK_SIZE)

                        if not chunk:
                            break

                        target.write(chunk)
                        bytes_written += len(chunk)

            # Verify extracted size.
            if bytes_written != member.file_size:
                raise ValueError(
                    f"Size mismatch for {member.filename}: "
                    f"expected {member.file_size}, "
                    f"received {bytes_written}."
                )

        # Upload only after extraction and validation succeed.
        destination_blob.upload_from_filename(
            str(extracted_path),
            content_type="application/json",
            checksum="auto",
        )

    logger.info(
        "Successfully extracted %s (%s bytes)",
        member.filename,
        bytes_written,
    )

    return {
        "archive": archive_name,
        "member": member.filename,
        "archive_size": archive_size,
        "member_size": bytes_written,
        "output_object": destination_object,
        "status": "success",
        "extracted_at": utc_now(),
    }


# --------------------------------------------------
# Main
# --------------------------------------------------

def main():
    """Extract all inventory archives for one snapshot."""

    parser = argparse.ArgumentParser(
        description="Extract GoDaddy inventory archives from GCS."
    )

    parser.add_argument(
        "--snapshot-date",
        default=datetime.now(timezone.utc).date().isoformat(),
        help="Snapshot date in YYYY-MM-DD format.",
    )

    args = parser.parse_args()

    snapshot_date = date.fromisoformat(
        args.snapshot_date
    ).isoformat()

    config = load_config()

    storage_config = config["storage"]

    bucket_name = storage_config["bucket"]

    raw_prefix = storage_config.get(
        "raw_prefix",
        "raw/snapshots",
    ).strip("/")

    processed_prefix = storage_config.get(
        "processed_prefix",
        "processed/snapshots",
    ).strip("/")

    raw_snapshot_prefix = (
        f"{raw_prefix}/{snapshot_date}"
    )

    output_prefix = (
        f"{processed_prefix}/{snapshot_date}"
    )

    manifest_object = (
        f"{raw_snapshot_prefix}/discovery_manifest.json"
    )

    report_object = (
        f"{output_prefix}/extraction_manifest.json"
    )

    # Connect to Google Cloud Storage using Application Default
    # Credentials locally, and the attached service account in Cloud Run.
    client = storage.Client()
    bucket = client.bucket(bucket_name)

    # Load the discovery manifest.
    manifest_blob = bucket.blob(manifest_object)

    if not manifest_blob.exists():
        raise FileNotFoundError(
            f"Discovery manifest not found: "
            f"gs://{bucket_name}/{manifest_object}"
        )

    discovery_manifest = json.loads(
        manifest_blob.download_as_text()
    )

    selected_files = discovery_manifest.get(
        "selected_files"
    )

    if (
        not isinstance(selected_files, list)
        or not selected_files
    ):
        raise ValueError(
            "Discovery manifest contains no selected inventory files."
        )

    # Load the previous extraction report for idempotency.
    previous_report = load_previous_report(
        bucket,
        report_object,
    )

    results = []
    failures = []

    logger.info(
        "Found %d archive(s) for snapshot %s.",
        len(selected_files),
        snapshot_date,
    )

    for file_info in selected_files:

        try:
            filename = file_info["file_name"]

            if (
                not isinstance(filename, str)
                or not filename
                or PurePosixPath(filename).name != filename
                or "\\" in filename
                or filename in (".", "..")
            ):
                raise ValueError(
                    f"Invalid archive filename: {filename!r}"
                )

            archive_object = (
                f"{raw_snapshot_prefix}/{filename}"
            )

            result = extract_archive(
                bucket=bucket,
                archive_object=archive_object,
                output_prefix=output_prefix,
                previous_report=previous_report,
            )

            results.append(result)

        except Exception as error:

            logger.exception(
                "Extraction failed for %s",
                file_info.get(
                    "file_name",
                    "unknown archive",
                ),
            )

            failures.append({
                "archive": file_info.get(
                    "file_name",
                    "unknown",
                ),
                "error": str(error),
                "status": "failed",
                "extracted_at": utc_now(),
            })

    # Build extraction report.
    report = {
        "source": "godaddy",
        "snapshot_date": snapshot_date,
        "generated_at": utc_now(),
        "total_archives": len(selected_files),
        "successful": sum(
            item["status"] in (
                "success",
                "skipped_existing",
            )
            for item in results
        ),
        "failed": len(failures),
        "extractions": results + failures,
    }

    # Upload extraction manifest to GCS.
    report_blob = bucket.blob(report_object)

    report_blob.upload_from_string(
        json.dumps(report, indent=4),
        content_type="application/json",
    )

    logger.info("----------------------------------")
    logger.info("EXTRACTION SUMMARY")
    logger.info("Archives processed: %d", len(selected_files))
    logger.info("Successful: %d", report["successful"])
    logger.info("Failed: %d", report["failed"])
    logger.info(
        "Report: gs://%s/%s",
        bucket_name,
        report_object,
    )
    logger.info("----------------------------------")

    if failures:
        raise SystemExit(1)


if __name__ == "__main__":
    try:
        main()
    except Exception:
        logger.exception("Extractor pipeline failed.")
        raise SystemExit(1)
