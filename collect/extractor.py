
"""
Extract GoDaddy inventory ZIP archives.

Run from the project root:
    python -m collect.extractor
"""

import json
import logging
import os
import zipfile

from datetime import datetime, timezone
from pathlib import Path


# --------------------------------------------------
# Paths
# --------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parent.parent

RAW_DIR = PROJECT_ROOT / "data" / "raw"
OUTPUT_DIR = PROJECT_ROOT / "data" / "processed" 

REPORT_PATH = OUTPUT_DIR / "extraction_manifest.json"

# Only extract JSON files from the inventory archives.
ALLOWED_EXTENSION = ".json"

CHUNK_SIZE = 1024 * 1024  # 1 MB


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


def load_previous_report():
    """Load the previous extraction report if available."""
    if not REPORT_PATH.exists():
        return {}

    try:
        with REPORT_PATH.open("r", encoding="utf-8") as file:
            report = json.load(file)

        return {
            item["archive"]: item
            for item in report.get("extractions", [])
            if "archive" in item
        }

    except (json.JSONDecodeError, OSError):
        logger.warning("Previous extraction report could not be read.")
        return {}


def find_archives():
    """Find ZIP archives in the raw data directory."""
    if not RAW_DIR.exists():
        raise FileNotFoundError(
            f"Raw data directory does not exist: {RAW_DIR}"
        )

    archives = sorted(RAW_DIR.glob("*.zip"))

    if not archives:
        raise FileNotFoundError(
            f"No ZIP archives found in: {RAW_DIR}"
        )

    return archives


def get_json_member(archive):
    """Find the JSON member inside an archive."""
    with zipfile.ZipFile(archive, "r") as zip_file:
        members = [
            item
            for item in zip_file.infolist()
            if not item.is_dir()
            and Path(item.filename).suffix.lower() == ALLOWED_EXTENSION
        ]

    if len(members) != 1:
        raise ValueError(
            f"Expected exactly one JSON file in {archive.name}; "
            f"found {len(members)}."
        )

    member = members[0]

    # Prevent unsafe paths and unexpected directory structures.
    if Path(member.filename).name != member.filename:
        raise ValueError(
            f"Unexpected path inside archive: {member.filename}"
        )

    return member


def extract_archive(archive, previous_report):
    """Validate and extract one ZIP archive."""

    logger.info("Processing archive: %s", archive.name)

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # Inspect the archive and locate its JSON member.
    member = get_json_member(archive)
    destination = OUTPUT_DIR / member.filename
    temporary_file = OUTPUT_DIR / f"{member.filename}.part"

    # Validate archive CRC before extracting.
    with zipfile.ZipFile(archive, "r") as zip_file:
        bad_member = zip_file.testzip()

        if bad_member is not None:
            raise zipfile.BadZipFile(
                f"Corrupt member found: {bad_member}"
            )

        # Skip an already extracted file if its size matches
        # and the previous report confirms the same archive/member.
        previous = previous_report.get(archive.name, {})

        can_skip = (
            destination.exists()
            and destination.stat().st_size == member.file_size
            and previous.get("member") == member.filename
            and previous.get("archive_size") == archive.stat().st_size
            and previous.get("member_size") == member.file_size
        )

        if can_skip:
            logger.info(
                "Already extracted; skipping: %s",
                destination.name,
            )

            return {
                "archive": archive.name,
                "member": member.filename,
                "archive_size": archive.stat().st_size,
                "member_size": member.file_size,
                "status": "skipped_existing",
                "extracted_at": previous.get("extracted_at"),
            }

        # Remove an old incomplete extraction, if present.
        if temporary_file.exists():
            temporary_file.unlink()

        logger.info("Extracting: %s", member.filename)

        try:
            bytes_written = 0

            with zip_file.open(member, "r") as source:
                with temporary_file.open("wb") as target:

                    while True:
                        chunk = source.read(CHUNK_SIZE)

                        if not chunk:
                            break

                        target.write(chunk)
                        bytes_written += len(chunk)

            if bytes_written != member.file_size:
                raise ValueError(
                    f"Size mismatch for {member.filename}: "
                    f"expected {member.file_size}, "
                    f"received {bytes_written}."
                )

            # Publish the completed file only after validation.
            os.replace(temporary_file, destination)

        except Exception:
            if temporary_file.exists():
                temporary_file.unlink()
            raise

    logger.info(
        "Successfully extracted %s (%s bytes)",
        destination.name,
        bytes_written,
    )

    return {
        "archive": archive.name,
        "member": member.filename,
        "archive_size": archive.stat().st_size,
        "member_size": bytes_written,
        "status": "success",
        "extracted_at": utc_now(),
    }


# --------------------------------------------------
# Main
# --------------------------------------------------

def main():
    """Extract all available GoDaddy inventory archives."""

    archives = find_archives()
    previous_report = load_previous_report()

    results = []
    failures = []

    logger.info("Found %d ZIP archive(s).", len(archives))

    for archive in archives:
        try:
            result = extract_archive(
                archive,
                previous_report,
            )

            results.append(result)

        except Exception as error:
            logger.exception(
                "Extraction failed for %s",
                archive.name,
            )

            failures.append({
                "archive": archive.name,
                "error": str(error),
                "status": "failed",
                "extracted_at": utc_now(),
            })

    # Save extraction report.
    report = {
        "source": "godaddy",
        "generated_at": utc_now(),
        "total_archives": len(archives),
        "successful": sum(
            item["status"] in ("success", "skipped_existing")
            for item in results
        ),
        "failed": len(failures),
        "extractions": results + failures,
    }

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    report_temp = REPORT_PATH.with_suffix(".json.tmp")

    with report_temp.open("w", encoding="utf-8") as file:
        json.dump(report, file, indent=4)

    os.replace(report_temp, REPORT_PATH)

    logger.info("----------------------------------")
    logger.info("EXTRACTION SUMMARY")
    logger.info("Archives processed: %d", len(archives))
    logger.info("Successful: %d", report["successful"])
    logger.info("Failed: %d", report["failed"])
    logger.info("Report: %s", REPORT_PATH)
    logger.info("----------------------------------")

    if failures:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
