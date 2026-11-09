
"""
Profile GoDaddy auction inventory JSON files.

Features:
- Streaming JSON parsing
- Field and data type profiling
- Missing and null value counts
- Numeric statistics
- Auction type distribution
- Duplicate domain detection
- Cross-dataset domain overlap

Run from the project root:
    python -m collect.profiler
"""

import json
import logging
import os
import sqlite3
import tempfile

from collections import Counter
from decimal import Decimal
from pathlib import Path

import ijson

from datetime import datetime, timezone

# --------------------------------------------------
# Configuration
# --------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parent.parent

DATA_DIR = PROJECT_ROOT / "data" / "processed"

DATASETS = {
    "all_biddable_auctions": (
        DATA_DIR / "all_biddable_auctions.json"
    ),
    "closeout_listings": (
        DATA_DIR / "closeout_listings.json"
    ),
}

REPORT_PATH = DATA_DIR / "profiling_report.json"

SAMPLE_LIMIT = 3
BATCH_SIZE = 5000


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

def classify_type(value):
    """Identify the JSON/Python data type."""

    if value is None:
        return "null"

    # bool must be checked before int because bool is
    # a subclass of int in Python.
    if isinstance(value, bool):
        return "boolean"

    if isinstance(value, int):
        return "integer"

    if isinstance(value, (float, Decimal)):
        return "number"

    if isinstance(value, str):
        return "string"

    if isinstance(value, list):
        return "array"

    if isinstance(value, dict):
        return "object"

    return type(value).__name__


def make_json_safe(value):
    """Convert values such as Decimal into JSON-safe values."""

    if isinstance(value, Decimal):
        return str(value)

    if isinstance(value, dict):
        return {
            key: make_json_safe(item)
            for key, item in value.items()
        }

    if isinstance(value, list):
        return [make_json_safe(item) for item in value]

    return value


def create_domain_database():
    """Create temporary SQLite storage for domain comparisons."""

    handle = tempfile.NamedTemporaryFile(
        prefix="godaddy_domains_",
        suffix=".sqlite",
        dir=DATA_DIR,
        delete=False,
    )

    db_path = Path(handle.name)
    handle.close()

    connection = sqlite3.connect(db_path)

    connection.execute("""
        CREATE TABLE domain_counts (
            dataset TEXT NOT NULL,
            domain_name TEXT NOT NULL,
            occurrences INTEGER NOT NULL,
            PRIMARY KEY (dataset, domain_name)
        )
    """)

    return connection, db_path


def flush_domain_batch(connection, dataset_name, domains):
    """Insert a batch of domains into SQLite."""

    if not domains:
        return

    counts = Counter(domains)

    connection.executemany(
        """
        INSERT INTO domain_counts (
            dataset,
            domain_name,
            occurrences
        )
        VALUES (?, ?, ?)

        ON CONFLICT(dataset, domain_name)
        DO UPDATE SET
            occurrences = occurrences + excluded.occurrences
        """,
        [
            (dataset_name, domain, count)
            for domain, count in counts.items()
        ],
    )

    connection.commit()
    domains.clear()


# --------------------------------------------------
# Dataset profiling
# --------------------------------------------------

def profile_dataset(dataset_name, file_path, connection):
    """Stream and profile one GoDaddy dataset."""

    if not file_path.exists():
        raise FileNotFoundError(
            f"Dataset not found: {file_path}"
        )

    logger.info("Profiling dataset: %s", dataset_name)

    field_stats = {}
    auction_types = Counter()

    record_count = 0
    domain_batch = []

    with file_path.open("rb") as file:

        # Each item represents one object in the data array.
        records = ijson.items(file, "data.item")

        for record in records:

            if not isinstance(record, dict):
                raise ValueError(
                    f"Expected a JSON object in {dataset_name}, "
                    f"record {record_count + 1}."
                )

            record_count += 1

            for field_name, value in record.items():

                if field_name not in field_stats:
                    field_stats[field_name] = {
                        "present_count": 0,
                        "null_count": 0,
                        "type_counts": Counter(),
                        "samples": [],
                        "numeric_count": 0,
                        "numeric_min": None,
                        "numeric_max": None,
                        "numeric_sum": Decimal("0"),
                    }

                stats = field_stats[field_name]

                stats["present_count"] += 1

                value_type = classify_type(value)
                stats["type_counts"][value_type] += 1

                if value is None:
                    stats["null_count"] += 1

                # Retain a small number of representative values.
                if len(stats["samples"]) < SAMPLE_LIMIT:
                    stats["samples"].append(
                        make_json_safe(value)
                    )

                # Calculate statistics only for native JSON numbers.
                if (
                    isinstance(value, (int, float, Decimal))
                    and not isinstance(value, bool)
                ):
                    numeric_value = Decimal(str(value))

                    stats["numeric_count"] += 1
                    stats["numeric_sum"] += numeric_value

                    if stats["numeric_min"] is None:
                        stats["numeric_min"] = numeric_value
                        stats["numeric_max"] = numeric_value
                    else:
                        stats["numeric_min"] = min(
                            stats["numeric_min"],
                            numeric_value,
                        )
                        stats["numeric_max"] = max(
                            stats["numeric_max"],
                            numeric_value,
                        )

            # Auction type distribution.
            auction_type = record.get("auctionType")

            if auction_type is not None:
                auction_types[str(auction_type)] += 1

            # Store valid domain names for duplicate/overlap analysis.
            domain_name = record.get("domainName")

            if isinstance(domain_name, str) and domain_name:
                domain_batch.append(domain_name)

            if len(domain_batch) >= BATCH_SIZE:
                flush_domain_batch(
                    connection,
                    dataset_name,
                    domain_batch,
                )

            if record_count % 100000 == 0:
                logger.info(
                    "%s: processed %s records",
                    dataset_name,
                    f"{record_count:,}",
                )

    # Insert any remaining domains.
    flush_domain_batch(
        connection,
        dataset_name,
        domain_batch,
    )

    # Finalize field statistics.
    fields_report = {}

    for field_name, stats in field_stats.items():

        numeric_report = None

        if stats["numeric_count"] > 0:
            numeric_report = {
                "count": stats["numeric_count"],
                "min": str(stats["numeric_min"]),
                "max": str(stats["numeric_max"]),
                "average": str(
                    stats["numeric_sum"] / stats["numeric_count"]
                ),
            }

        fields_report[field_name] = {
            "present_count": stats["present_count"],
            "missing_count": (
                record_count - stats["present_count"]
            ),
            "null_count": stats["null_count"],
            "type_counts": dict(stats["type_counts"]),
            "samples": stats["samples"],
            "numeric_statistics": numeric_report,
        }

    logger.info(
        "%s profiling completed: %s records",
        dataset_name,
        f"{record_count:,}",
    )

    return {
        "file": file_path.name,
        "file_size_bytes": file_path.stat().st_size,
        "record_count": record_count,
        "field_count": len(fields_report),
        "fields": fields_report,
        "auction_type_distribution": dict(auction_types),
    }


# --------------------------------------------------
# Domain analysis
# --------------------------------------------------

def analyze_domains(connection):
    """Calculate duplicate domains and dataset overlap."""

    result = {}

    datasets = list(DATASETS.keys())

    for dataset_name in datasets:

        row = connection.execute(
            """
            SELECT
                COUNT(*),
                COALESCE(SUM(occurrences), 0),
                COALESCE(SUM(
                    CASE WHEN occurrences > 1
                    THEN 1 ELSE 0 END
                ), 0),
                COALESCE(SUM(
                    CASE WHEN occurrences > 1
                    THEN occurrences - 1 ELSE 0 END
                ), 0)
            FROM domain_counts
            WHERE dataset = ?
            """,
            (dataset_name,),
        ).fetchone()

        result[dataset_name] = {
            "unique_domain_count": row[0],
            "total_domain_occurrences": row[1],
            "duplicate_domain_count": row[2],
            "extra_duplicate_occurrences": row[3],
        }

    overlap = connection.execute(
        """
        SELECT COUNT(*)
        FROM domain_counts AS a
        INNER JOIN domain_counts AS b
            ON a.domain_name = b.domain_name
        WHERE
            a.dataset = ?
            AND b.dataset = ?
        """,
        (datasets[0], datasets[1]),
    ).fetchone()[0]

    common_samples = connection.execute(
        """
        SELECT a.domain_name
        FROM domain_counts AS a
        INNER JOIN domain_counts AS b
            ON a.domain_name = b.domain_name
        WHERE
            a.dataset = ?
            AND b.dataset = ?
        ORDER BY a.domain_name
        LIMIT 20
        """,
        (datasets[0], datasets[1]),
    ).fetchall()

    return {
        "datasets": result,
        "cross_dataset_overlap": {
            "common_unique_domain_count": overlap,
            "sample_common_domains": [
                row[0] for row in common_samples
            ],
        },
    }


# --------------------------------------------------
# Main
# --------------------------------------------------

def main():

    DATA_DIR.mkdir(parents=True, exist_ok=True)

    connection = None
    database_path = None

    dataset_reports = {}
    failures = []

    try:
        connection, database_path = create_domain_database()

        for dataset_name, file_path in DATASETS.items():

            try:
                dataset_reports[dataset_name] = profile_dataset(
                    dataset_name,
                    file_path,
                    connection,
                )

            except Exception as error:
                logger.exception(
                    "Profiling failed for %s",
                    dataset_name,
                )

                failures.append({
                    "dataset": dataset_name,
                    "error": str(error),
                })

        domain_report = None

        # Compare domains only when both datasets were profiled.
        if len(dataset_reports) == len(DATASETS):
            domain_report = analyze_domains(connection)

    finally:
        if connection is not None:
            connection.close()

        if database_path is not None and database_path.exists():
            os.remove(database_path)

    report = {
        "source": "godaddy",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "datasets": dataset_reports,
        "domain_analysis": domain_report,
        "failures": failures,
        "status": "success" if not failures else "partial_failure",
    }

    temporary_report = REPORT_PATH.with_suffix(".json.tmp")

    with temporary_report.open("w", encoding="utf-8") as file:
        json.dump(report, file, indent=4, ensure_ascii=False)

    os.replace(temporary_report, REPORT_PATH)

    logger.info("----------------------------------")
    logger.info("PROFILING SUMMARY")

    for name, result in dataset_reports.items():
        logger.info(
            "%s: %s records, %s fields",
            name,
            f"{result['record_count']:,}",
            result["field_count"],
        )

    logger.info("Failed datasets: %d", len(failures))
    logger.info("Report saved: %s", REPORT_PATH)
    logger.info("----------------------------------")

    if failures:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
