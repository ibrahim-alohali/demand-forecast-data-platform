"""Download the UCI Online Retail II dataset.

Uses only stdlib (urllib, zipfile) — no extra dependencies.
Source: https://archive.ics.uci.edu/dataset/502/online+retail+ii
"""

from __future__ import annotations

import zipfile
from pathlib import Path
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
        if not xlsx_names:
            raise FileNotFoundError("No .xlsx file found inside the zip archive")
        zf.extract(xlsx_names[0], data_dir)
        extracted = data_dir / xlsx_names[0]
        # Rename to a consistent name if different
        if extracted != xlsx_path:
            extracted.rename(xlsx_path)

    # Clean up the zip file
    zip_path.unlink()
    print(f"Extracted to {xlsx_path}")
    return xlsx_path


if __name__ == "__main__":
    download()
