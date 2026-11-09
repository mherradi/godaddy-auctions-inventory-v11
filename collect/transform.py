import json
import logging
import re
import sys
from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from urllib.parse import urlparse

import ijson
import pyarrow as pa
import pyarrow.parquet as pq


# ============================================================
# 1. PROJECT PATHS AND CONFIGURATION
# ============================================================

# File location:
# project_root/collect/transform/transform.py
#
# parents[0] = collect/transform
# parents[1] = collect
# parents[2] = project root

PROJECT_ROOT = Path(__file__).resolve().parents[2]

INPUT_DIR = PROJECT_ROOT / "data" / "processed"
OUTPUT_DIR = PROJECT_ROOT / "data" / "transformed"

DISCOVERY_MANIFEST = (
    PROJECT_ROOT
    / "collect"
    / "discovery"
    / "discovery_manifest.json"
)

OUTPUT_FILE = OUTPUT_DIR / "stg_auction_inventory.parquet"
TEMP_OUTPUT_FILE = OUTPUT_DIR / "stg_auction_inventory.tmp.parquet"
REPORT_FILE = OUTPUT_DIR / "transformation_report.json"

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
    handlers=[
        logging.StreamHandler(sys.stdout)
    ],
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
# 4. METADATA LOOKUP
# ============================================================

def normalize_filename(filename):
    """
    Normalize filenames so that:
        inventory.json
        inventory.json.zip

    are treated as the same inventory file.
    """

    filename = Path(filename).name.lower()

    if filename.endswith(".zip"):
        filename = filename[:-4]

    return filename


def find_source_last_modified(file_name):
    """
    Find the source file's last_modified timestamp
    in discovery_manifest.json.

    Supports both .json and .json.zip filenames.
    """

    if not DISCOVERY_MANIFEST.exists():
        raise FileNotFoundError(
            f"Discovery manifest not found: {DISCOVERY_MANIFEST}"
        )

    with open(
        DISCOVERY_MANIFEST,
        "r",
        encoding="utf-8"
    ) as file:

        manifest = json.load(file)

    requested_name = normalize_filename(file_name)

    selected_files = manifest.get("selected_files", [])

    for entry in selected_files:

        manifest_name = entry.get("file_name", "")

        if normalize_filename(manifest_name) == requested_name:

            last_modified = entry.get("last_modified")

            if not last_modified:
                raise ValueError(
                    f"File found in manifest, but last_modified "
                    f"is missing: {manifest_name}"
                )

            return last_modified

    raise ValueError(
        f"No matching file found in discovery manifest for: "
        f"{file_name}"
    )


def get_snapshot_date(file_name):
    """
    Convert the source last_modified timestamp into a UTC date.

    This uses GoDaddy's manifest last_modified value as the
    snapshot-date proxy. It does not use the transformation
    execution date.
    """

    last_modified = find_source_last_modified(file_name)

    try:
        timestamp = datetime.fromisoformat(
            last_modified.replace("Z", "+00:00")
        )

        if timestamp.tzinfo is None:
            raise ValueError(
                f"Timestamp has no timezone: {last_modified}"
            )

        return timestamp.astimezone(timezone.utc).date()

    except (ValueError, TypeError) as error:
        raise ValueError(
            f"Invalid last_modified timestamp for {file_name}: "
            f"{last_modified}"
        ) from error


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
    """
    Extract the numeric GoDaddy listing ID from the URL.

    Example:
    https://www.godaddy.com/domain-auctions/example-com-123456789

    Returns:
        123456789
    """

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
    """
    Convert currency strings such as:
        '$1'
        '$1,323'
        '$0'

    into Decimal values.

    Missing optional values become None.
    """

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
    """
    Convert integer-like values to Python integers.

    Decimal values are accepted only when they represent
    whole numbers.
    """

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
    """
    Parse an ISO timestamp and convert it to UTC.

    Example:
        2026-11-08T16:00:00Z
    """

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
    """
    Accept boolean values and preserve missing values as None.
    """

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
    ingested_at
):
    """
    Transform one raw GoDaddy inventory record into
    the standardized staging schema.
    """

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
            required=True
        ),

        "valuation": parse_currency(
            record.get("valuation"),
            "valuation"
        ),

        "monthly_parking_revenue": parse_currency(
            record.get("monthlyParkingRevenue"),
            "monthlyParkingRevenue"
        ),

        "number_of_bids": parse_integer(
            record.get("numberOfBids"),
            "numberOfBids",
            required=True
        ),

        "domain_age": parse_integer(
            record.get("domainAge"),
            "domainAge"
        ),

        "pageviews": parse_integer(
            record.get("pageviews"),
            "pageviews"
        ),

        "is_adult": parse_boolean(
            record.get("isAdult"),
            "isAdult"
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
# 8. DATASET TRANSFORMATION
# ============================================================

def process_dataset(
    writer,
    settings,
    snapshot_date,
    ingested_at,
    report
):
    """
    Stream one JSON dataset and write transformed records
    to the Parquet writer in batches.
    """

    input_file = INPUT_DIR / settings["file"]

    if not input_file.exists():
        raise FileNotFoundError(
            f"Input dataset not found: {input_file}"
        )

    logger.info("----------------------------------")
    logger.info("Processing dataset: %s", settings["file"])
    logger.info("Snapshot date: %s", snapshot_date)
    logger.info("Input file: %s", input_file)

    dataset_report = {
        "records_read": 0,
        "records_written": 0,
        "records_rejected": 0,
        "errors": [],
    }

    batch = []

    with open(input_file, "rb") as source:

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

                batch.append(transformed_record)

            except Exception as error:

                dataset_report["records_rejected"] += 1

                if len(dataset_report["errors"]) < MAX_ERROR_EXAMPLES:
                    dataset_report["errors"].append({
                        "record_number": record_number,
                        "error": str(error),
                    })

            if len(batch) >= BATCH_SIZE:

                table = pa.Table.from_pylist(
                    batch,
                    schema=SCHEMA
                )

                writer.write_table(table)

                dataset_report["records_written"] += len(batch)

                batch.clear()

                logger.info(
                    "%s | Read: %s | Written: %s | Rejected: %s",
                    settings["source_dataset"],
                    dataset_report["records_read"],
                    dataset_report["records_written"],
                    dataset_report["records_rejected"],
                )

    # Write the remaining records in the final batch.
    if batch:

        table = pa.Table.from_pylist(
            batch,
            schema=SCHEMA
        )

        writer.write_table(table)

        dataset_report["records_written"] += len(batch)

        batch.clear()

    report["datasets"][settings["source_dataset"]] = dataset_report

    logger.info(
        "Completed %s | Read: %s | Written: %s | Rejected: %s",
        settings["source_dataset"],
        dataset_report["records_read"],
        dataset_report["records_written"],
        dataset_report["records_rejected"],
    )

    return dataset_report


# ============================================================
# 9. REPORT GENERATION
# ============================================================

def write_report(report):
    """
    Write transformation_report.json.
    """

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    with open(
        REPORT_FILE,
        "w",
        encoding="utf-8"
    ) as file:

        json.dump(
            report,
            file,
            indent=4,
            default=str
        )


# ============================================================
# 10. MAIN PIPELINE
# ============================================================

def main():

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    ingested_at = datetime.now(timezone.utc)

    report = {
        "generated_at_utc": ingested_at.isoformat(),
        "status": "running",
        "input_directory": str(INPUT_DIR),
        "output_file": str(OUTPUT_FILE),
        "datasets": {},
        "total_records_read": 0,
        "total_records_written": 0,
        "total_records_rejected": 0,
        "errors": [],
    }

    writer = None
    transformation_succeeded = False

    logger.info("==================================")
    logger.info("GODADDY INVENTORY TRANSFORMATION")
    logger.info("==================================")

    logger.info("Project root: %s", PROJECT_ROOT)
    logger.info("Input directory: %s", INPUT_DIR)
    logger.info("Output directory: %s", OUTPUT_DIR)
    logger.info("Discovery manifest: %s", DISCOVERY_MANIFEST)

    try:

        # ----------------------------------------------------
        # Check project inputs
        # ----------------------------------------------------

        if not INPUT_DIR.exists():
            raise FileNotFoundError(
                f"Input directory does not exist: {INPUT_DIR}"
            )

        if not DISCOVERY_MANIFEST.exists():
            raise FileNotFoundError(
                f"Discovery manifest does not exist: "
                f"{DISCOVERY_MANIFEST}"
            )

        # ----------------------------------------------------
        # Remove an abandoned temporary file from an earlier
        # failed run. The final output file is not touched.
        # ----------------------------------------------------

        if TEMP_OUTPUT_FILE.exists():
            TEMP_OUTPUT_FILE.unlink()

        # ----------------------------------------------------
        # Initialize Parquet writer
        # ----------------------------------------------------

        writer = pq.ParquetWriter(
            TEMP_OUTPUT_FILE,
            SCHEMA,
            compression="snappy",
        )

        # ----------------------------------------------------
        # Process each dataset
        # ----------------------------------------------------

        for settings in DATASETS:

            logger.info(
                "Looking up metadata for %s",
                settings["file"]
            )

            snapshot_date = get_snapshot_date(
                settings["file"]
            )

            dataset_report = process_dataset(
                writer=writer,
                settings=settings,
                snapshot_date=snapshot_date,
                ingested_at=ingested_at,
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

        # ----------------------------------------------------
        # Close Parquet writer
        # ----------------------------------------------------

        writer.close()
        writer = None

        # ----------------------------------------------------
        # Fail if any records were rejected.
        # Keep the previous successful output untouched.
        # ----------------------------------------------------

        if report["total_records_rejected"] > 0:

            raise RuntimeError(
                f"Transformation rejected "
                f"{report['total_records_rejected']} record(s). "
                f"Review the transformation report before publishing."
            )

        # ----------------------------------------------------
        # Check that output contains records
        # ----------------------------------------------------

        if report["total_records_written"] == 0:

            raise RuntimeError(
                "No records were written. "
                "The input JSON files may be empty or have an "
                "unexpected structure."
            )

        # ----------------------------------------------------
        # Publish the completed Parquet file
        # ----------------------------------------------------

        TEMP_OUTPUT_FILE.replace(OUTPUT_FILE)

        transformation_succeeded = True
        report["status"] = "success"

        logger.info("----------------------------------")
        logger.info("Transformation completed successfully.")
        logger.info("Output: %s", OUTPUT_FILE)

    except Exception as error:

        report["status"] = "failed"

        report["errors"].append({
            "error": str(error),
            "type": type(error).__name__,
        })

        logger.exception("Transformation failed.")

    finally:

        # Close writer if an exception occurred during processing.
        if writer is not None:

            try:
                writer.close()

            except Exception:
                logger.exception(
                    "Error while closing Parquet writer."
                )

        # Remove incomplete temporary output.
        if TEMP_OUTPUT_FILE.exists():

            try:
                TEMP_OUTPUT_FILE.unlink()

            except OSError:
                logger.exception(
                    "Could not remove temporary Parquet file."
                )

        # Write final report.
        try:
            write_report(report)

        except Exception:
            logger.exception(
                "Could not write transformation report."
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
            report["total_records_read"]
        )

        logger.info(
            "Records written: %s",
            report["total_records_written"]
        )

        logger.info(
            "Records rejected: %s",
            report["total_records_rejected"]
        )

        logger.info("Report: %s", REPORT_FILE)

        if not transformation_succeeded:
            logger.error(
                "Transformation did not publish a new output file."
            )

        logger.info("----------------------------------")

    return 0 if transformation_succeeded else 1


# ============================================================
# 11. SCRIPT ENTRY POINT
# ============================================================

if __name__ == "__main__":
    sys.exit(main())