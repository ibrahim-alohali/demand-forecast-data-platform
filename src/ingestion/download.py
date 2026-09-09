"""Download the UCI Online Retail II dataset.

Uses only stdlib (urllib, zipfile) — no extra dependencies.
Source: https://archive.ics.uci.edu/dataset/502/online+retail+ii
"""

from __future__ import annotations

import zipfile
from pathlib import Path
from shutil import copyfileobj
from urllib.request import urlretrieve

DATASET_URL = (
    "https://archive.ics.uci.edu/static/public/502/online+retail+ii.zip"
)
DEFAULT_DATA_DIR = Path(__file__).resolve().parent.parent.parent / "data"


def download(data_dir: Path | None = None) -> Path:
    """Download and extract the Online Retail II xlsx file.

    Args:
        data_dir: Directory to save the extracted file. Defaults to project data/.

    Returns:
        Path to the extracted xlsx file.
    """
    data_dir = data_dir or DEFAULT_DATA_DIR
    data_dir.mkdir(parents=True, exist_ok=True)

    zip_path = data_dir / "online_retail_ii.zip"
    xlsx_path = data_dir / "online_retail_ii.xlsx"

    if xlsx_path.exists():
        print(f"File already exists: {xlsx_path}")
        return xlsx_path

    print(f"Downloading {DATASET_URL} ...")
    urlretrieve(DATASET_URL, zip_path)
    print(f"Saved zip to {zip_path}")

    print("Extracting xlsx ...")
    with zipfile.ZipFile(zip_path, "r") as zf:
        # Find the xlsx file inside the zip
        xlsx_names = [n for n in zf.namelist() if n.endswith(".xlsx")]
        if len(xlsx_names) != 1:
            raise ValueError("Expected exactly one .xlsx file in the archive")
        # Ignore archive paths; stream only the workbook to a fixed local name.
        # Publish after a successful copy, so interrupted downloads are retryable.
        partial_path = xlsx_path.with_suffix(".xlsx.part")
        try:
            with zf.open(xlsx_names[0]) as source, partial_path.open("wb") as target:
                copyfileobj(source, target)
            partial_path.replace(xlsx_path)
        finally:
            partial_path.unlink(missing_ok=True)

    # Clean up the zip file
    zip_path.unlink()
    print(f"Extracted to {xlsx_path}")
    return xlsx_path


if __name__ == "__main__":
    download()
