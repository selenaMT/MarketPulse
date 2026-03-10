"""Clear theme-related tables while keeping articles intact."""

from __future__ import annotations

import sys
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import text

# Ensure `app` package imports work when running this file directly.
BACKEND_ROOT = Path(__file__).resolve().parents[1]
PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

from app.db.session import SessionLocal


def main() -> None:
    load_dotenv(PROJECT_ROOT / ".env")
    session = SessionLocal()
    try:
        session.execute(
            text(
                """
                truncate table
                  historical_themes,
                  candidate_theme_article_links,
                  theme_article_links,
                  theme_candidates,
                  themes
                restart identity cascade
                """
            )
        )
        session.commit()
        print("Theme tables cleared successfully.")
    finally:
        session.close()


if __name__ == "__main__":
    main()
