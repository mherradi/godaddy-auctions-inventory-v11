
"""
Download selected GoDaddy inventory ZIP files from discovery_manifest.json.

Run from the project root:
    python -m collect.downloader
"""

import json
import logging
import os
import zipfile
from pathlib import Path
from urllib.parse import quote

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


# Project paths
PROJECT_ROOT = Path(__file__).resolve().parent.parent
MANIFEST_PATH = Path(__file__).resolve().parent / "discovery" / "discovery_manifest.json"
OUTPUT_DIR = PROJECT_ROOT / "data" / "raw" 

# GoDaddy inventory host
BASE_URL = "https://inventory.auctions.godaddy.com"

CHUNK_SIZE = 1024 * 1024  # 1 MB
TIMEOUT = (15, 120)       # connect timeout, read timeout


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger(__name__)


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


def load_manifest(manifest_path):
    """Load and validate the discovery manifest."""
    if not manifest_path.exists():
        raise FileNotFoundError(
            f"Manifest not found: {manifest_path}"
        )

    with manifest_path.open("r", encoding="utf-8") as file:
        manifest = json.load(file)

    selected_files = manifest.get("selected_files")

    if not isinstance(selected_files, list) or not selected_files:
        raise ValueError(
            "Manifest must contain a non-empty 'selected_files' list."
        )

    return selected_files


def is_valid_zip(file_path):
    """Check ZIP integrity, including CRC checks."""
    try:
        with zipfile.ZipFile(file_path, "r") as archive:
            bad_file = archive.testzip()
            return bad_file is None
    except (zipfile.BadZipFile, OSError):
        return False


def download_file(session, file_info, output_dir):
    """Download one inventory file and validate it."""
    filename = file_info.get("file_name")
    expected_size = file_info.get("file_size")

    if not filename or Path(filename).name != filename:
        raise ValueError(f"Invalid filename in manifest: {filename!r}")

    if not isinstance(expected_size, int) or expected_size <= 0:
        raise ValueError(
            f"Invalid file_size for {filename}: {expected_size!r}"
        )

    output_dir.mkdir(parents=True, exist_ok=True)

    destination = output_dir / filename
    partial_file = output_dir / f"{filename}.part"

    # Reuse an existing download only if size and ZIP integrity match.
    if destination.exists():
        if (
            destination.stat().st_size == expected_size
            and is_valid_zip(destination)
        ):
            logger.info("Already downloaded and valid: %s", filename)
            return destination

        logger.warning(
            "Existing file is incomplete or invalid; downloading again: %s",
            filename,
        )
        destination.unlink()

    url = f"{BASE_URL.rstrip('/')}/{quote(filename)}"

    logger.info("Downloading %s", filename)
    logger.info("Source: %s", url)

    try:
        with session.get(
            url,
            stream=True,
            timeout=TIMEOUT,
        ) as response:
            response.raise_for_status()

            downloaded_size = 0

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
                f"received {downloaded_size:,} bytes."
            )

        if not is_valid_zip(partial_file):
            raise zipfile.BadZipFile(
                f"Downloaded archive failed ZIP integrity check: {filename}"
            )

        # Rename only after all checks pass.
        os.replace(partial_file, destination)

        logger.info(
            "Download successful: %s (%s bytes)",
            filename,
            downloaded_size,
        )

        return destination

    except Exception:
        if partial_file.exists():
            partial_file.unlink()
        raise


def main():
    """Download every file selected in the discovery manifest."""
    selected_files = load_manifest(MANIFEST_PATH)
    session = create_session()

    successes = []
    failures = []

    for file_info in selected_files:
        filename = file_info.get("file_name", "<unknown>")

        try:
            path = download_file(
                session=session,
                file_info=file_info,
                output_dir=OUTPUT_DIR,
            )
            successes.append(path)

        except Exception as error:
            failures.append((filename, str(error)))
            logger.exception("Failed to download %s", filename)

    logger.info("Download summary: %d successful, %d failed",
                len(successes), len(failures))

    if failures:
        for filename, error in failures:
            logger.error("%s: %s", filename, error)

        raise SystemExit(1)


if __name__ == "__main__":
    main()
