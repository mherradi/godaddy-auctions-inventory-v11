import argparse
import logging
import subprocess
import sys

from datetime import datetime, timezone


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    stream=sys.stdout,
)

logger = logging.getLogger(__name__)


PIPELINE_STAGES = [
    ("Discovery", "collect.discovery"),
    ("Download", "collect.downloader"),
    ("Extraction", "collect.extractor"),
    ("Transformation", "collect.transform"),
]


def main():
    parser = argparse.ArgumentParser(
        description="Run the complete GoDaddy Auctions ingestion pipeline."
    )

    parser.add_argument(
        "--snapshot-date",
        default=datetime.now(timezone.utc).date().isoformat(),
        help="Snapshot date in YYYY-MM-DD format. Defaults to current UTC date.",
    )

    args = parser.parse_args()

    logger.info("========================================")
    logger.info("GODADDY AUCTIONS PIPELINE")
    logger.info("Snapshot date: %s", args.snapshot_date)
    logger.info("========================================")

    for stage_name, module_name in PIPELINE_STAGES:
        logger.info("Starting stage: %s", stage_name)

        command = [
            sys.executable,
            "-m",
            module_name,
            "--snapshot-date",
            args.snapshot_date,
        ]

        try:
            subprocess.run(command, check=True)

        except subprocess.CalledProcessError as error:
            logger.exception(
                "Pipeline stopped: %s failed with exit code %s",
                stage_name,
                error.returncode,
            )
            return error.returncode or 1

        logger.info("Completed stage: %s", stage_name)

    logger.info("========================================")
    logger.info("PIPELINE COMPLETED SUCCESSFULLY")
    logger.info("Snapshot date: %s", args.snapshot_date)
    logger.info("========================================")

    return 0


if __name__ == "__main__":
    sys.exit(main())