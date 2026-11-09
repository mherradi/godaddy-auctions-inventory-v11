
import decimal
import json
import logging
import re
import sqlite3
import tempfile
from collections import Counter
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from urllib.parse import urlparse

import ijson


# ---------------------------------------------------------
# PATHS AND CONFIGURATION
# ---------------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parent.parent
PROCESSED_DIR = PROJECT_ROOT / "data" / "processed"
REPORT_PATH = PROCESSED_DIR / "quality_report.json"

DATASETS = {
    "all_biddable_auctions": {
        "file": PROCESSED_DIR / "all_biddable_auctions.json",
        "expected_auction_type": "Bid",
    },
    "closeout_listings": {
        "file": PROCESSED_DIR / "closeout_listings.json",
        "expected_auction_type": "BuyNow",
    },
}

REQUIRED_FIELDS = [
    "domainName",
    "link",
    "auctionType",
    "auctionEndTime",
    "price",
    "numberOfBids",
]

OPTIONAL_FIELDS = [
    "domainAge",
    "monthlyParkingRevenue",
]

EXPECTED_TYPES = {
    "domainName": str,
    "link": str,
    "auctionType": str,
    "auctionEndTime": str,
    "price": str,
    "valuation": str,
    "monthlyParkingRevenue": str,
    "semrushTopReferringDomains": str,
    "numberOfBids": int,
    "domainAge": int,
    "pageviews": int,
    "majesticTf": int,
    "majesticCf": int,
    "majesticBacklinks": int,
    "majesticReferringDomains": int,
    "exactMatchTlds": int,
    "keywordRegistrations": int,
    "developedTlds": int,
    "semrushAs": int,
    "semrushReferringDomains": int,
    "semrushBacklinks": int,
    "semrushIndexedPages": int,
    "semrushSearchVolume": int,
    "semrushCpc": (int, float),
    "isAdult": bool,
}

MAX_EXAMPLES_PER_CHECK = 10

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger(__name__)


# ---------------------------------------------------------
# VALIDATION HELPERS
# ---------------------------------------------------------

def normalize_domain(value):
    """Normalize a domain for duplicate and overlap checks."""
    if not isinstance(value, str):
        return None

    return value.strip().lower().rstrip(".")


def is_valid_domain(value):
    """
    Basic domain validation.
    Supports ordinary domains and internationalized domains
    represented in ASCII/Punycode form.
    """
    domain = normalize_domain(value)

    if not domain or len(domain) > 253:
        return False

    if " " in domain or "/" in domain or "://" in domain:
        return False

    labels = domain.split(".")

    if len(labels) < 2:
        return False

    label_pattern = re.compile(
        r"^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$"
    )

    return all(label_pattern.fullmatch(label) for label in labels)


def parse_currency(value):
    """Convert strings such as '$1,323' into Decimal values."""
    if not isinstance(value, str):
        return None

    cleaned = value.strip().replace("$", "").replace(",", "")

    if not cleaned:
        return None

    try:
        return Decimal(cleaned)
    except InvalidOperation:
        return None


def is_valid_timestamp(value):
    """Check whether a timestamp is parseable and timezone-aware."""
    if not isinstance(value, str) or not value.strip():
        return False

    try:
        parsed = datetime.fromisoformat(
            value.strip().replace("Z", "+00:00")
        )
        return parsed.tzinfo is not None
    except ValueError:
        return False


def extract_listing_id(value):
    """Extract a numeric listing ID from the end of a GoDaddy URL path."""
    if not isinstance(value, str):
        return None

    try:
        parsed = urlparse(value)

        if parsed.scheme != "https":
            return None

        if parsed.hostname not in {
            "godaddy.com",
            "www.godaddy.com",
        }:
            return None
    
        match = re.search(r"-(\d+)/?$", parsed.path)
        return match.group(1) if match else None

    except ValueError:
        return None


# ---------------------------------------------------------
# REPORT HELPERS
# ---------------------------------------------------------

def new_dataset_report():
    return {
        "records_processed": 0,
        "missing_required_fields": Counter(),
        "missing_optional_fields": Counter(),
        "null_fields": Counter(),
        "type_errors": Counter(),
        "invalid_domains": 0,
        "invalid_links": 0,
        "missing_listing_ids": 0,
        "invalid_timestamps": 0,
        "invalid_prices": 0,
        "negative_prices": 0,
        "negative_numeric_values": Counter(),
        "unexpected_auction_types": Counter(),
        "examples": {},
    }


def record_issue(report, issue_name, example):
    examples = report["examples"].setdefault(issue_name, [])

    if len(examples) < MAX_EXAMPLES_PER_CHECK:
        examples.append(example)


def serialize_counter(counter):
    return dict(counter)


# ---------------------------------------------------------
# DATASET QUALITY CHECKS
# ---------------------------------------------------------

def profile_dataset(dataset_name, config, connection):
    file_path = config["file"]

    if not file_path.exists():
        raise FileNotFoundError(f"Dataset not found: {file_path}")

    report = new_dataset_report()
    expected_auction_type = config["expected_auction_type"]

    connection.execute("""
        CREATE TABLE IF NOT EXISTS domain_keys (
            dataset TEXT NOT NULL,
            domain TEXT NOT NULL
        )
    """)

    logger.info("Checking dataset: %s", dataset_name)

    with file_path.open("rb") as source:
        records = ijson.items(source, "data.item")

        for record_number, record in enumerate(records, start=1):
            report["records_processed"] += 1

            if not isinstance(record, dict):
                record_issue(
                    report,
                    "invalid_record_structure",
                    {"record_number": record_number},
                )
                continue

            # Required and optional fields
            for field in REQUIRED_FIELDS:
                if field not in record:
                    report["missing_required_fields"][field] += 1
                    record_issue(
                        report,
                        "missing_required_fields",
                        {
                            "record_number": record_number,
                            "field": field,
                        },
                    )
                elif record[field] is None:
                    report["null_fields"][field] += 1

            for field in OPTIONAL_FIELDS:
                if field not in record:
                    report["missing_optional_fields"][field] += 1
                elif record[field] is None:
                    report["null_fields"][field] += 1

            # Field type checks
            for field, expected_type in EXPECTED_TYPES.items():
                if field not in record or record[field] is None:
                    continue

                value = record[field]

                # bool is a subclass of int in Python, so use exact checks.
                if expected_type is int:
                    valid_type = type(value) is int
                elif expected_type is bool:
                    valid_type = type(value) is bool
                elif expected_type is str:
                    valid_type = isinstance(value, str)
                else:
                    valid_type = type(value) in (int, float, Decimal)

                if not valid_type:
                    report["type_errors"][field] += 1
                    record_issue(
                        report,
                        "type_errors",
                        {
                            "record_number": record_number,
                            "field": field,
                            "actual_type": type(value).__name__,
                        },
                    )

            # Domain validation and normalized duplicate tracking
            domain = record.get("domainName")
            normalized_domain = normalize_domain(domain)

            if not is_valid_domain(domain):
                report["invalid_domains"] += 1
                record_issue(
                    report,
                    "invalid_domains",
                    {
                        "record_number": record_number,
                        "domainName": domain,
                    },
                )
            elif normalized_domain:
                connection.execute(
                    "INSERT INTO domain_keys VALUES (?, ?)",
                    (dataset_name, normalized_domain),
                )

            # Link and listing ID validation
            link = record.get("link")
            listing_id = extract_listing_id(link)

            if listing_id is None:
                report["invalid_links"] += 1
                report["missing_listing_ids"] += 1
                record_issue(
                    report,
                    "invalid_links",
                    {
                        "record_number": record_number,
                        "link": link,
                    },
                )

            # Auction type validation
            auction_type = record.get("auctionType")

            if auction_type != expected_auction_type:
                report["unexpected_auction_types"][
                    str(auction_type)
                ] += 1

            # Timestamp validation
            if not is_valid_timestamp(record.get("auctionEndTime")):
                report["invalid_timestamps"] += 1
                record_issue(
                    report,
                    "invalid_timestamps",
                    {
                        "record_number": record_number,
                        "auctionEndTime": record.get("auctionEndTime"),
                    },
                )

            # Price validation
            price = record.get("price")
            parsed_price = parse_currency(price)

            if parsed_price is None:
                report["invalid_prices"] += 1
                record_issue(
                    report,
                    "invalid_prices",
                    {
                        "record_number": record_number,
                        "price": price,
                    },
                )
            elif parsed_price < 0:
                report["negative_prices"] += 1
                record_issue(
                    report,
                    "negative_prices",
                    {
                        "record_number": record_number,
                        "price": price,
                    },
                )

            # Basic non-negative checks for numeric metrics
            numeric_fields = [
                "numberOfBids",
                "domainAge",
                "pageviews",
                "majesticTf",
                "majesticCf",
                "majesticBacklinks",
                "majesticReferringDomains",
                "exactMatchTlds",
                "keywordRegistrations",
                "developedTlds",
                "semrushAs",
                "semrushReferringDomains",
                "semrushBacklinks",
                "semrushIndexedPages",
                "semrushSearchVolume",
                "semrushCpc",
            ]

            for field in numeric_fields:
                value = record.get(field)

                if (
                    type(value) in (int, float)
                    and value < 0
                ):
                    report["negative_numeric_values"][field] += 1

            if report["records_processed"] % 100000 == 0:
                logger.info(
                    "%s: processed %s records",
                    dataset_name,
                    report["records_processed"],
                )

    connection.commit()

    # Duplicate domains after normalization
    duplicate_result = connection.execute(
        """
        SELECT COUNT(*)
        FROM (
            SELECT domain
            FROM domain_keys
            WHERE dataset = ?
            GROUP BY domain
            HAVING COUNT(*) > 1
        )
        """,
        (dataset_name,),
    ).fetchone()

    report["normalized_duplicate_domains"] = duplicate_result[0]

    return report


# ---------------------------------------------------------
# MAIN
# ---------------------------------------------------------

def main():
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

    logger.info("Starting GoDaddy data quality checks.")

    with tempfile.TemporaryDirectory() as temp_dir:
        database_path = Path(temp_dir) / "quality_checks.sqlite"

        connection = sqlite3.connect(database_path)

        try:
            dataset_reports = {}

            for dataset_name, config in DATASETS.items():
                dataset_reports[dataset_name] = profile_dataset(
                    dataset_name,
                    config,
                    connection,
                )

            # Exact normalized overlap between the two datasets
            overlap_count = connection.execute(
                """
                SELECT COUNT(*)
                FROM (
                    SELECT DISTINCT a.domain
                    FROM domain_keys a
                    INNER JOIN domain_keys b
                        ON a.domain = b.domain
                    WHERE a.dataset = ?
                      AND b.dataset = ?
                )
                """,
                (
                    "all_biddable_auctions",
                    "closeout_listings",
                ),
            ).fetchone()[0]

        finally:
            connection.close()

    # Convert Counters into JSON-compatible dictionaries
    for dataset_report in dataset_reports.values():
        for key, value in list(dataset_report.items()):
            if isinstance(value, Counter):
                dataset_report[key] = serialize_counter(value)

    total_records = sum(
        report["records_processed"]
        for report in dataset_reports.values()
    )

    quality_report = {
        "generated_at_utc": (
        datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
        ),
        "source": "GoDaddy Auctions",
        "total_records_processed": total_records,
        "datasets": dataset_reports,
        "cross_dataset_checks": {
            "normalized_domain_overlap": overlap_count,
        },
        "notes": [
            "Original extracted JSON files were not modified.",
            "Domain duplicate checks use lowercase and trimmed domain names.",
            "Listing IDs are extracted from numeric IDs at the end of GoDaddy URL paths.",
            "Price validation currently expects dollar-formatted strings.",
            "A quality issue is a signal for investigation, not automatic proof of bad source data.",
        ],
    }

    with REPORT_PATH.open("w", encoding="utf-8") as report_file:
        json.dump(
            quality_report,
            report_file,
            indent=4,
            ensure_ascii=False,
        )

    logger.info("----------------------------------")
    logger.info("QUALITY CHECK SUMMARY")
    logger.info("Total records: %s", total_records)

    for dataset_name, report in dataset_reports.items():
        logger.info(
            "%s: %s records | invalid domains: %s | invalid links: %s",
            dataset_name,
            report["records_processed"],
            report["invalid_domains"],
            report["invalid_links"],
        )

    logger.info(
        "Normalized cross-dataset domain overlap: %s",
        overlap_count,
    )
    logger.info("Report: %s", REPORT_PATH)
    logger.info("----------------------------------")


if __name__ == "__main__":
    main()
