
"""
Transform GoDaddy inventory JSON files into standardized Parquet.

Inputs:
    gs://<bucket>/processed/snapshots/YYYY-MM-DD/

Outputs:
    gs://<bucket>/transformed/snapshots/YYYY-MM-DD/
        stg_auction_inventory.parquet
        transformation_report.json

Run from the project root:
    python -m collect.transform --snapshot-date YYYY-MM-DD
"""

import argparse
import json
import logging
import re
import sqlite3
import sys
import tempfile

from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from urllib.parse import urlparse

import ijson
import pyarrow as pa
import pyarrow.parquet as pq
import yaml

from google.cloud import storage


# ============================================================
# 1. PROJECT PATHS AND CONFIGURATION
# ============================================================

# This script is located at project_root/collect/transform.py.
PROJECT_ROOT = Path(__file__).resolve().parents[1]
CONFIG_PATH = PROJECT_ROOT / "config" / "config.yaml"

BATCH_SIZE = 10_000
MAX_ERROR_EXAMPLES = 100

DATASETS = [
    {
        "file": "all_biddable_auctions.json",
        "source_dataset": "all_biddable_auctions",
        "expected_auction_type": "Bid",
        "standardized_auction_type": "Bid",
    },
    {
        "file": "closeout_listings.json",
        "source_dataset": "closeout_listings",
        "expected_auction_type": "BuyNow",
        "standardized_auction_type": "Buy",
    },
]


# ============================================================
# 2. LOGGING
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)

logger = logging.getLogger(__name__)


# ============================================================
# 3. PARQUET OUTPUT SCHEMA
# ============================================================

SCHEMA = pa.schema([
    # Business fields
    pa.field("domain_name", pa.string()),
    pa.field("listing_url", pa.string()),
    pa.field("auction_type", pa.string()),
    pa.field("auction_end_time", pa.timestamp("us", tz="UTC")),
    pa.field("price", pa.decimal128(38, 9)),
    pa.field("valuation", pa.decimal128(38, 9)),
    pa.field("monthly_parking_revenue", pa.decimal128(38, 9)),
    pa.field("number_of_bids", pa.int64()),
    pa.field("domain_age", pa.int64()),
    pa.field("pageviews", pa.int64()),
    pa.field("is_adult", pa.bool_()),

    # Technical lineage fields
    pa.field("listing_id", pa.string()),
    pa.field("domain_name_normalized", pa.string()),
    pa.field("source_dataset", pa.string()),
    pa.field("snapshot_date", pa.date32()),
    pa.field("ingested_at", pa.timestamp("us", tz="UTC")),
])


# ============================================================
# 4. CONFIGURATION AND GCS HELPERS
# ============================================================

def load_config():
    """Load project configuration."""

    with CONFIG_PATH.open("r", encoding="utf-8") as file:
        config = yaml.safe_load(file)

    if not isinstance(config, dict):
        raise ValueError(
            "Configuration file must contain a YAML mapping."
        )

    if "storage" not in config:
        raise ValueError(
            "Missing storage configuration in config.yaml."
        )

    return config


def utc_now():
    """Return the current UTC timestamp."""

    return datetime.now(timezone.utc)


def upload_json(bucket, object_name, payload):
    """Upload a JSON report to GCS."""

    blob = bucket.blob(object_name)

    blob.upload_from_string(
        json.dumps(
            payload,
            indent=4,
            ensure_ascii=False,
            default=str,
        ),
        content_type="application/json",
    )


def load_discovery_manifest(bucket, object_name):
    """Load and validate the discovery manifest."""

    blob = bucket.get_blob(object_name)

    if blob is None:
        raise FileNotFoundError(
            f"Discovery manifest not found: "
            f"gs://{bucket.name}/{object_name}"
        )

    manifest = json.loads(blob.download_as_text())

    expected_archives = {
        "all_biddable_auctions.json.zip",
        "closeout_listings.json.zip",
    }

    selected_files = manifest.get("selected_files", [])

    discovered_archives = {
        item.get("file_name")
        for item in selected_files
        if isinstance(item, dict)
    }

    missing_datasets = expected_archives - discovered_archives

    if missing_datasets:
        raise ValueError(
            "Discovery manifest is missing expected archives: "
            + ", ".join(sorted(missing_datasets))
        )

    return manifest


# ============================================================
# 5. DOMAIN AND LISTING ID HELPERS
# ============================================================

def normalize_domain(domain_name):
    """
    Normalize domain names for comparison.

    - Remove leading/trailing whitespace.
    - Convert to lowercase.
    - Remove trailing dots.
    - Preserve punycode.
    """

    if not isinstance(domain_name, str):
        raise ValueError("Domain name must be a string.")

    normalized = domain_name.strip().lower().rstrip(".")

    if not normalized:
        raise ValueError("Domain name is empty.")

    if " " in normalized:
        raise ValueError(
            f"Domain name contains spaces: {domain_name}"
        )

    if "." not in normalized:
        raise ValueError(
            f"Domain name does not contain a dot: {domain_name}"
        )

    return normalized


def parse_listing_id(listing_url):
    """Extract the numeric GoDaddy listing ID from its URL."""

    if not isinstance(listing_url, str):
        raise ValueError("Listing URL must be a string.")

    parsed_url = urlparse(listing_url.strip())
    hostname = (parsed_url.hostname or "").lower()

    valid_host = (
        hostname == "godaddy.com"
        or hostname.endswith(".godaddy.com")
    )

    if parsed_url.scheme != "https" or not valid_host:
        raise ValueError(
            f"Invalid GoDaddy listing URL: {listing_url}"
        )

    path_parts = parsed_url.path.rstrip("/").split("/")

    if not path_parts:
        raise ValueError(
            f"Listing URL has no path: {listing_url}"
        )

    final_path_segment = path_parts[-1]
    match = re.search(r"-(\d+)$", final_path_segment)

    if not match:
        raise ValueError(
            f"Could not extract listing ID from URL: {listing_url}"
        )

    return match.group(1)


# ============================================================
# 6. DATA TYPE CONVERSION HELPERS
# ============================================================

def parse_currency(value, field_name, required=False):
    """Convert currency strings into Decimal values."""

    if value is None or value == "":
        if required:
            raise ValueError(
                f"Required currency field is missing: {field_name}"
            )
        return None

    if isinstance(value, bool):
        raise ValueError(
            f"Invalid currency value for {field_name}: {value}"
        )

    try:
        cleaned = str(value).strip()
        cleaned = cleaned.replace("$", "").replace(",", "")

        amount = Decimal(cleaned)

        if not amount.is_finite():
            raise ValueError(
                f"Non-finite currency value for {field_name}: {value}"
            )

        if amount < 0:
            raise ValueError(
                f"Negative currency value for {field_name}: {value}"
            )

        return amount.quantize(Decimal("0.000000001"))

    except (InvalidOperation, ValueError) as error:
        raise ValueError(
            f"Invalid currency value for {field_name}: {value}"
        ) from error


def parse_integer(value, field_name, required=False):
    """Convert integer-like values to non-negative integers."""

    if value is None or value == "":
        if required:
            raise ValueError(
                f"Required integer field is missing: {field_name}"
            )
        return None

    if isinstance(value, bool):
        raise ValueError(
            f"Invalid integer value for {field_name}: {value}"
        )

    try:
        if isinstance(value, Decimal):
            if value != value.to_integral_value():
                raise ValueError(
                    f"Non-integer value for {field_name}: {value}"
                )

            result = int(value)

        elif isinstance(value, int):
            result = value

        elif isinstance(value, str):
            result = int(value.strip())

        else:
            raise ValueError(
                f"Unsupported integer type for {field_name}: "
                f"{type(value).__name__}"
            )

        if result < 0:
            raise ValueError(
                f"Negative integer value for {field_name}: {value}"
            )

        return result

    except (ValueError, TypeError) as error:
        raise ValueError(
            f"Invalid integer value for {field_name}: {value}"
        ) from error


def parse_timestamp(value):
    """Parse an ISO timestamp and convert it to UTC."""

    if not isinstance(value, str) or not value.strip():
        raise ValueError(
            f"Invalid or missing auctionEndTime: {value}"
        )

    try:
        timestamp = datetime.fromisoformat(
            value.strip().replace("Z", "+00:00")
        )

        if timestamp.tzinfo is None:
            raise ValueError(
                f"Timestamp has no timezone: {value}"
            )

        return timestamp.astimezone(timezone.utc)

    except ValueError as error:
        raise ValueError(
            f"Invalid auction timestamp: {value}"
        ) from error


def parse_boolean(value, field_name):
    """Validate boolean values and preserve missing values as None."""

    if value is None:
        return None

    if isinstance(value, bool):
        return value

    raise ValueError(
        f"Invalid boolean value for {field_name}: {value}"
    )


# ============================================================
# 7. RECORD TRANSFORMATION
# ============================================================

def transform_record(
    record,
    settings,
    snapshot_date,
    ingested_at,
):
    """Transform one raw record into the standardized schema."""

    if not isinstance(record, dict):
        raise ValueError("Expected a JSON object.")

    domain_name = record.get("domainName")

    if not isinstance(domain_name, str) or not domain_name.strip():
        raise ValueError("Missing or invalid domainName.")

    listing_url = record.get("link")

    if not isinstance(listing_url, str) or not listing_url.strip():
        raise ValueError("Missing or invalid link.")

    source_auction_type = record.get("auctionType")

    if source_auction_type != settings["expected_auction_type"]:
        raise ValueError(
            f"Unexpected auction type: {source_auction_type}. "
            f"Expected: {settings['expected_auction_type']}"
        )

    transformed = {
        # Business fields
        "domain_name": domain_name.strip(),

        "listing_url": listing_url.strip(),

        "auction_type": settings["standardized_auction_type"],

        "auction_end_time": parse_timestamp(
            record.get("auctionEndTime")
        ),

        "price": parse_currency(
            record.get("price"),
            "price",
            required=True,
        ),

        "valuation": parse_currency(
            record.get("valuation"),
            "valuation",
        ),

        "monthly_parking_revenue": parse_currency(
            record.get("monthlyParkingRevenue"),
            "monthlyParkingRevenue",
        ),

        "number_of_bids": parse_integer(
            record.get("numberOfBids"),
            "numberOfBids",
            required=True,
        ),

        "domain_age": parse_integer(
            record.get("domainAge"),
            "domainAge",
        ),

        "pageviews": parse_integer(
            record.get("pageviews"),
            "pageviews",
        ),

        "is_adult": parse_boolean(
            record.get("isAdult"),
            "isAdult",
        ),

        # Technical lineage fields
        "listing_id": parse_listing_id(listing_url),

        "domain_name_normalized": normalize_domain(domain_name),

        "source_dataset": settings["source_dataset"],

        "snapshot_date": snapshot_date,

        "ingested_at": ingested_at,
    }

    return transformed


# ============================================================
# 8. QUALITY CHECKS
# ============================================================

def create_quality_database(database_path):
    """
    Create temporary SQLite tables for duplicate checks.

    Domain names and listing IDs are checked across both datasets.
    """

    connection = sqlite3.connect(database_path)

    connection.execute("""
        CREATE TABLE quality_keys (
            domain_name_normalized TEXT PRIMARY KEY,
            listing_id TEXT NOT NULL UNIQUE,
            source_dataset TEXT NOT NULL
        )
    """)

    connection.commit()

    return connection


def validate_unique_record(connection, transformed_record):
    """Reject duplicate normalized domains or listing IDs."""

    try:
        connection.execute(
            """
            INSERT INTO quality_keys (
                domain_name_normalized,
                listing_id,
                source_dataset
            )
            VALUES (?, ?, ?)
            """,
            (
                transformed_record["domain_name_normalized"],
                transformed_record["listing_id"],
                transformed_record["source_dataset"],
            ),
        )

    except sqlite3.IntegrityError as error:
        raise ValueError(
            "Duplicate domain or listing ID detected: "
            f"{transformed_record['domain_name_normalized']} / "
            f"{transformed_record['listing_id']}"
        ) from error


# ============================================================
# 9. DATASET TRANSFORMATION
# ============================================================

def process_dataset(
    writer,
    bucket,
    temporary_dir,
    processed_prefix,
    settings,
    snapshot_date,
    ingested_at,
    quality_connection,
    report,
):
    """Download, stream, validate and transform one dataset."""

    filename = settings["file"]

    input_object = (
        f"{processed_prefix.rstrip('/')}/{filename}"
    )

    input_blob = bucket.get_blob(input_object)

    if input_blob is None:
        raise FileNotFoundError(
            f"Input dataset not found: "
            f"gs://{bucket.name}/{input_object}"
        )

    input_file = Path(temporary_dir) / filename

    logger.info("----------------------------------")
    logger.info("Processing dataset: %s", filename)
    logger.info("Snapshot date: %s", snapshot_date)
    logger.info("GCS input: %s", input_object)

    input_blob.download_to_filename(str(input_file))

    logger.info(
        "Downloaded %s (%s bytes)",
        filename,
        input_file.stat().st_size,
    )

    dataset_report = {
        "input_object": input_object,
        "input_size_bytes": input_file.stat().st_size,
        "records_read": 0,
        "records_written": 0,
        "records_rejected": 0,
        "duplicate_domain_or_listing_id": 0,
        "errors": [],
    }

    batch = []

    with input_file.open("rb") as source:

        records = ijson.items(source, "data.item")

        for record_number, record in enumerate(records, start=1):

            dataset_report["records_read"] += 1

            try:
                transformed_record = transform_record(
                    record=record,
                    settings=settings,
                    snapshot_date=snapshot_date,
                    ingested_at=ingested_at,
                )

                validate_unique_record(
                    quality_connection,
                    transformed_record,
                )

                batch.append(transformed_record)

            except Exception as error:

                dataset_report["records_rejected"] += 1

                if "Duplicate domain or listing ID" in str(error):
                    dataset_report[
                        "duplicate_domain_or_listing_id"
                    ] += 1

                if len(dataset_report["errors"]) < MAX_ERROR_EXAMPLES:
                    dataset_report["errors"].append({
                        "record_number": record_number,
                        "error": str(error),
                    })

            if len(batch) >= BATCH_SIZE:

                table = pa.Table.from_pylist(
                    batch,
                    schema=SCHEMA,
                )

                writer.write_table(table)

                dataset_report["records_written"] += len(batch)

                batch.clear()

                logger.info(
                    "%s | Read: %s | Written: %s | Rejected: %s",
                    settings["source_dataset"],
                    f"{dataset_report['records_read']:,}",
                    f"{dataset_report['records_written']:,}",
                    f"{dataset_report['records_rejected']:,}",
                )

    # Write the remaining records.
    if batch:

        table = pa.Table.from_pylist(
            batch,
            schema=SCHEMA,
        )

        writer.write_table(table)

        dataset_report["records_written"] += len(batch)

        batch.clear()

    quality_connection.commit()

    report["datasets"][settings["source_dataset"]] = dataset_report

    logger.info(
        "Completed %s | Read: %s | Written: %s | Rejected: %s",
        settings["source_dataset"],
        f"{dataset_report['records_read']:,}",
        f"{dataset_report['records_written']:,}",
        f"{dataset_report['records_rejected']:,}",
    )

    # Remove the downloaded JSON after processing this dataset.
    input_file.unlink()

    return dataset_report


# ============================================================
# 10. REPORT GENERATION
# ============================================================

def write_report(bucket, report_object, report):
    """Upload the transformation report to GCS."""

    upload_json(
        bucket,
        report_object,
        report,
    )


# ============================================================
# 11. MAIN PIPELINE
# ============================================================

def main():

    parser = argparse.ArgumentParser(
        description="Transform GoDaddy inventory JSON into Parquet."
    )

    parser.add_argument(
        "--snapshot-date",
        required=True,
        help="Collection snapshot date in YYYY-MM-DD format.",
    )

    args = parser.parse_args()

    snapshot_date = date.fromisoformat(
        args.snapshot_date
    )

    ingested_at = utc_now()

    config = load_config()
    storage_config = config["storage"]

    bucket_name = storage_config["bucket"]

    processed_prefix = storage_config.get(
        "processed_prefix",
        "processed/snapshots",
    ).strip("/")

    transformed_prefix = storage_config.get(
        "transformed_prefix",
        "transformed/snapshots",
    ).strip("/")

    processed_snapshot_prefix = (
        f"{processed_prefix}/{snapshot_date.isoformat()}"
    )

    transformed_snapshot_prefix = (
        f"{transformed_prefix}/{snapshot_date.isoformat()}"
    )

    discovery_manifest_object = (
        f"{processed_snapshot_prefix}/../discovery_manifest.json"
    )

    # Discovery manifest actually lives in raw, not processed.
    raw_prefix = storage_config.get(
        "raw_prefix",
        "raw/snapshots",
    ).strip("/")

    discovery_manifest_object = (
        f"{raw_prefix}/{snapshot_date.isoformat()}/"
        "discovery_manifest.json"
    )

    output_object = (
        f"{transformed_snapshot_prefix}/"
        "stg_auction_inventory.parquet"
    )

    report_object = (
        f"{transformed_snapshot_prefix}/"
        "transformation_report.json"
    )

    report = {
        "source": "godaddy",
        "snapshot_date": snapshot_date.isoformat(),
        "generated_at_utc": ingested_at.isoformat(),
        "status": "running",
        "input_prefix": processed_snapshot_prefix,
        "output_object": output_object,
        "datasets": {},
        "total_records_read": 0,
        "total_records_written": 0,
        "total_records_rejected": 0,
        "quality_checks": {},
        "errors": [],
    }

    logger.info("==================================")
    logger.info("GODADDY INVENTORY TRANSFORMATION")
    logger.info("==================================")
    logger.info("Project root: %s", PROJECT_ROOT)
    logger.info("Snapshot date: %s", snapshot_date)
    logger.info("GCS input: %s", processed_snapshot_prefix)
    logger.info("GCS output: %s", output_object)

    client = storage.Client()
    bucket = client.bucket(bucket_name)

    writer = None
    quality_connection = None
    transformation_succeeded = False

    try:

        # ----------------------------------------------------
        # Validate discovery manifest
        # ----------------------------------------------------

        manifest = load_discovery_manifest(
            bucket,
            discovery_manifest_object,
        )

        report["discovery_manifest"] = discovery_manifest_object

        # ----------------------------------------------------
        # Temporary local workspace
        # ----------------------------------------------------

        with tempfile.TemporaryDirectory(
            prefix="godaddy_transform_",
            ignore_cleanup_errors=True,
        ) as temporary_dir:

            database_path = (
                Path(temporary_dir) / "quality_checks.sqlite"
            )

            parquet_path = (
                Path(temporary_dir) / "stg_auction_inventory.parquet"
            )

            quality_connection = create_quality_database(
                database_path
            )

            # ------------------------------------------------
            # Initialize Parquet writer
            # ------------------------------------------------

            writer = pq.ParquetWriter(
                parquet_path,
                SCHEMA,
                compression="snappy",
            )

            # ------------------------------------------------
            # Process both datasets
            # ------------------------------------------------

            for settings in DATASETS:

                dataset_report = process_dataset(
                    writer=writer,
                    bucket=bucket,
                    temporary_dir=temporary_dir,
                    processed_prefix=processed_snapshot_prefix,
                    settings=settings,
                    snapshot_date=snapshot_date,
                    ingested_at=ingested_at,
                    quality_connection=quality_connection,
                    report=report,
                )

                report["total_records_read"] += (
                    dataset_report["records_read"]
                )

                report["total_records_written"] += (
                    dataset_report["records_written"]
                )

                report["total_records_rejected"] += (
                    dataset_report["records_rejected"]
                )

            # --------------------------------------------
            # Close writer before validating Parquet output
            # --------------------------------------------

            writer.close()
            writer = None

            quality_connection.close()
            quality_connection = None

            # ------------------------------------------------
            # Integrated quality checks
            # ------------------------------------------------

            parquet_file = pq.ParquetFile(parquet_path)

            parquet_rows = parquet_file.metadata.num_rows
            parquet_schema = parquet_file.schema_arrow

            report["quality_checks"] = {
                "both_datasets_processed": (
                    len(report["datasets"]) == len(DATASETS)
                ),
                "all_records_written": (
                    report["total_records_read"]
                    == report["total_records_written"]
                ),
                "zero_rejected_records": (
                    report["total_records_rejected"] == 0
                ),
                "non_empty_output": parquet_rows > 0,
                "parquet_row_count_matches": (
                    parquet_rows == report["total_records_written"]
                ),
                "parquet_schema_matches": (
                    parquet_schema.equals(SCHEMA)
                ),
            }

            failed_checks = [
                name
                for name, passed in report["quality_checks"].items()
                if not passed
            ]

            if failed_checks:
                raise RuntimeError(
                    "Transformation quality checks failed: "
                    + ", ".join(failed_checks)
                )

            # ------------------------------------------------
            # Publish only after all checks succeed
            # ------------------------------------------------

            output_blob = bucket.blob(output_object)

            output_blob.upload_from_filename(
                str(parquet_path),
                content_type="application/vnd.apache.parquet",
                checksum="auto",
            )

            transformation_succeeded = True
            report["status"] = "success"
            report["output_size_bytes"] = parquet_path.stat().st_size
            report["output_rows"] = parquet_rows

            logger.info(
                "Parquet uploaded: gs://%s/%s",
                bucket_name,
                output_object,
            )

    except Exception as error:

        report["status"] = "failed"

        report["errors"].append({
            "error": str(error),
            "type": type(error).__name__,
        })

        logger.exception("Transformation failed.")

    finally:

        if writer is not None:
            try:
                writer.close()
            except Exception:
                logger.exception(
                    "Error while closing Parquet writer."
                )

        if quality_connection is not None:
            try:
                quality_connection.close()
            except Exception:
                logger.exception(
                    "Error while closing quality database."
                )

        report["completed_at_utc"] = utc_now().isoformat()

        try:
            write_report(
                bucket,
                report_object,
                report,
            )

            logger.info(
                "Transformation report: gs://%s/%s",
                bucket_name,
                report_object,
            )

        except Exception:
            logger.exception(
                "Could not upload transformation report."
            )

        # ----------------------------------------------------
        # Final summary
        # ----------------------------------------------------

        logger.info("----------------------------------")
        logger.info("TRANSFORMATION SUMMARY")
        logger.info("----------------------------------")
        logger.info("Status: %s", report["status"])
        logger.info(
            "Records read: %s",
            f"{report['total_records_read']:,}",
        )
        logger.info(
            "Records written: %s",
            f"{report['total_records_written']:,}",
        )
        logger.info(
            "Records rejected: %s",
            f"{report['total_records_rejected']:,}",
        )

        logger.info(
            "Output: gs://%s/%s",
            bucket_name,
            output_object,
        )

        logger.info("----------------------------------")

    return 0 if transformation_succeeded else 1


# ============================================================
# 12. SCRIPT ENTRY POINT
# ============================================================

if __name__ == "__main__":
    sys.exit(main())
