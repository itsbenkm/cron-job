import os
from pathlib import Path

# This script is located at:
# fashionbroda/fashionbroda_cj/fashionbroda_cj/scripts/paths.py

# Resolve the repository root (4 levels up from this file)
REPO_ROOT = Path(__file__).resolve().parents[4]

# Standardize the data and logs directories
DATA_DIR = REPO_ROOT / "fashionbroda" / "fashionbroda_cj" / "fashionbroda_cj" / "data"
LOGS_DIR = REPO_ROOT / "fashionbroda" / "fashionbroda_cj" / "fashionbroda_cj" / "logs"

# Ensure directories exist
DATA_DIR.mkdir(parents=True, exist_ok=True)
LOGS_DIR.mkdir(parents=True, exist_ok=True)

def get_data_path(filename: str) -> str:
    """Returns the absolute path to a file in the standardized data directory."""
    return str(DATA_DIR / filename)

def get_log_path(filename: str) -> str:
    """Returns the absolute path to a file in the standardized logs directory."""
    return str(LOGS_DIR / filename)

def get_repo_relative_path(path: str) -> str:
    """Returns the absolute path to a repo-relative path string."""
    return str(REPO_ROOT / path)
