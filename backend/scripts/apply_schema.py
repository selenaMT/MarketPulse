"""Apply SQL schema file to the configured PostgreSQL database."""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_SCHEMA_PATH = PROJECT_ROOT / "database" / "schema.sql"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Apply database/schema.sql to DATABASE_URL.")
    parser.add_argument(
        "--schema-path",
        default=str(DEFAULT_SCHEMA_PATH),
        help="Path to SQL schema file.",
    )
    return parser.parse_args()


def normalize_database_url(raw_url: str) -> str:
    """Convert SQLAlchemy URL form into a psycopg-compatible DSN."""
    if raw_url.startswith("postgresql+psycopg://"):
        return raw_url.replace("postgresql+psycopg://", "postgresql://", 1)
    return raw_url


def main() -> int:
    try:
        import psycopg
    except ImportError:
        print(
            "ERROR: psycopg is not installed. Install backend dependencies first:\n"
            "  python -m pip install -r backend/requirements.txt"
        )
        return 1

    args = parse_args()
    try:
        from dotenv import load_dotenv

        load_dotenv(PROJECT_ROOT / ".env")
    except ImportError:
        # Optional: rely on existing process env when python-dotenv is unavailable.
        pass

    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        print("ERROR: DATABASE_URL not found in environment variables.")
        return 1

    schema_path = Path(args.schema_path).resolve()
    if not schema_path.exists():
        print(f"ERROR: Schema file not found: {schema_path}")
        return 1

    sql_text = schema_path.read_text(encoding="utf-8")
    dsn = normalize_database_url(database_url)

    try:
        with psycopg.connect(dsn, autocommit=True) as conn:
            with conn.cursor() as cur:
                cur.execute(sql_text)
    except Exception as exc:
        print(f"ERROR: Failed to apply schema: {exc}")
        return 1

    print(f"Schema applied successfully from: {schema_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
