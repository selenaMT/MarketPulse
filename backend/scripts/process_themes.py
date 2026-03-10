"""Run theme assignment and maintenance as a dedicated operational job.

This script is intentionally theme-only. It does not perform article ingestion,
embedding generation, or text processing backfills.
"""

from __future__ import annotations

import argparse
import sys
import traceback
from pathlib import Path
from typing import Any

from sqlalchemy import text
from sqlalchemy.orm import Session

# Ensure `app` package imports work when running this file directly.
BACKEND_ROOT = Path(__file__).resolve().parents[1]
PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

from app.db.session import SessionLocal
from app.pipelines.theme_management_pipeline import ThemeManagementPipeline
from app.repositories.theme_repository import ThemeRepository
from app.services.theme_management_service import ThemeManagementService

DEFAULT_LOCK_KEY = 420_042_001
EXIT_OK = 0
EXIT_ERROR = 1
EXIT_SKIPPED = 2


class ThemeProcessingSkippedError(RuntimeError):
    """Raised when a run should be skipped without treating it as a failure."""


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Process themes only: assignment, snapshots, relations, and lifecycle statuses."
    )
    parser.add_argument(
        "--assignment-limit",
        type=int,
        default=500,
        help="Maximum number of unlinked articles to evaluate for theme assignment.",
    )
    parser.add_argument(
        "--snapshot-lookback-days",
        type=int,
        default=90,
        help="Rolling window for rebuilding daily theme snapshots.",
    )
    parser.add_argument(
        "--assignment-lookback-days",
        type=int,
        default=None,
        help="Optional lookback window for assignment candidates only.",
    )
    parser.add_argument(
        "--relation-lookback-days",
        type=int,
        default=45,
        help="Rolling window for rebuilding co-occurrence relations.",
    )
    parser.add_argument(
        "--run-maintenance",
        action="store_true",
        help="Run merge/split recommendation maintenance pass.",
    )
    parser.add_argument(
        "--rebuild-centroids",
        action="store_true",
        help="Recompute theme centroids from all existing links before finishing this run.",
    )
    parser.add_argument(
        "--no-record-run",
        action="store_true",
        help="Skip writing run metrics to theme_sync_runs.",
    )
    parser.add_argument(
        "--lock-key",
        type=int,
        default=DEFAULT_LOCK_KEY,
        help="PostgreSQL advisory lock key to prevent concurrent theme runs.",
    )
    parser.add_argument(
        "--no-lock",
        action="store_true",
        help="Disable advisory lock guard (not recommended for concurrent schedulers).",
    )
    return parser.parse_args()


def maybe_load_dotenv() -> None:
    """Best-effort .env loading without hard dependency on python-dotenv."""
    try:
        from dotenv import load_dotenv
    except ImportError:
        return
    load_dotenv(PROJECT_ROOT / ".env")


def validate_args(args: argparse.Namespace) -> None:
    if args.assignment_limit <= 0:
        raise ValueError("--assignment-limit must be > 0")
    if args.snapshot_lookback_days <= 0:
        raise ValueError("--snapshot-lookback-days must be > 0")
    if args.relation_lookback_days <= 0:
        raise ValueError("--relation-lookback-days must be > 0")
    if args.assignment_lookback_days is not None and args.assignment_lookback_days <= 0:
        raise ValueError("--assignment-lookback-days must be > 0 when provided")


def try_acquire_lock(session: Session, lock_key: int) -> bool:
    row = session.execute(
        text("select pg_try_advisory_lock(:lock_key) as acquired"),
        {"lock_key": lock_key},
    ).mappings().first()
    return bool(row and row.get("acquired"))


def release_lock(session: Session, lock_key: int) -> None:
    session.execute(text("select pg_advisory_unlock(:lock_key)"), {"lock_key": lock_key})
    session.commit()


def run_theme_pipeline(
    *,
    session: Session,
    assignment_limit: int,
    assignment_lookback_days: int | None,
    snapshot_lookback_days: int,
    relation_lookback_days: int,
    run_maintenance: bool,
    rebuild_centroids: bool,
    record_run: bool,
) -> dict[str, Any]:
    theme_repository = ThemeRepository(session)
    theme_service = ThemeManagementService(theme_repository=theme_repository)
    pipeline = ThemeManagementPipeline(
        theme_repository=theme_repository,
        theme_management_service=theme_service,
    )
    result = pipeline.run(
        assignment_limit=assignment_limit,
        assignment_lookback_days=assignment_lookback_days,
        snapshot_lookback_days=snapshot_lookback_days,
        relation_lookback_days=relation_lookback_days,
        run_maintenance=run_maintenance,
        rebuild_centroids=rebuild_centroids,
        record_run=record_run,
    )
    return {
        "assignment_input_count": int(result["assignment_input_count"]),
        "assigned_articles": int(result["assigned_articles"]),
        "theme_links_upserted": int(result["theme_links_upserted"]),
        "created_themes": int(result["created_themes"]),
        "promoted_candidates": int(result["promoted_candidates"]),
        "abstained_articles": int(result.get("abstained_articles", 0)),
        "abstained_signals": int(result.get("abstained_signals", 0)),
        "candidate_observations": int(result.get("candidate_observations", 0)),
        "assignment_rate": float(result.get("assignment_rate", 0.0)),
        "abstain_rate": float(result.get("abstain_rate", 0.0)),
        "snapshots_upserted": int(result["snapshots_upserted"]),
        "relations_upserted": int(result["relations_upserted"]),
        "status_updates": int(result["status_updates"]),
        "centroids_rebuilt": int(result.get("centroids_rebuilt", 0)),
        "recommendation_count": int(result.get("recommendation_count", 0)),
        "merge_recommendations": int(result.get("merge_recommendations", 0)),
        "split_recommendations": int(result.get("split_recommendations", 0)),
    }


def run_theme_processing(args: argparse.Namespace) -> dict[str, Any]:
    session: Session = SessionLocal()
    lock_acquired = False
    use_lock = not args.no_lock
    try:
        if use_lock:
            lock_acquired = try_acquire_lock(session, lock_key=int(args.lock_key))
            if not lock_acquired:
                raise ThemeProcessingSkippedError(
                    "could not acquire advisory lock. Another theme run may still be active."
                )

        return run_theme_pipeline(
            session=session,
            assignment_limit=int(args.assignment_limit),
            assignment_lookback_days=(
                int(args.assignment_lookback_days)
                if args.assignment_lookback_days is not None
                else None
            ),
            snapshot_lookback_days=int(args.snapshot_lookback_days),
            relation_lookback_days=int(args.relation_lookback_days),
            run_maintenance=bool(args.run_maintenance),
            rebuild_centroids=bool(args.rebuild_centroids),
            record_run=not bool(args.no_record_run),
        )
    finally:
        if use_lock and lock_acquired:
            try:
                release_lock(session, lock_key=int(args.lock_key))
            except Exception:
                session.rollback()
                print("WARN: failed to release advisory lock cleanly", file=sys.stderr)
        session.close()


def print_summary(result: dict[str, Any]) -> None:
    print("Theme processing summary:")
    print(f"assignment_input_count={result['assignment_input_count']}")
    print(f"assigned_articles={result['assigned_articles']}")
    print(f"theme_links_upserted={result['theme_links_upserted']}")
    print(f"created_themes={result['created_themes']}")
    print(f"promoted_candidates={result['promoted_candidates']}")
    print(f"abstained_articles={result['abstained_articles']}")
    print(f"abstained_signals={result['abstained_signals']}")
    print(f"candidate_observations={result['candidate_observations']}")
    print(f"assignment_rate={result['assignment_rate']:.4f}")
    print(f"abstain_rate={result['abstain_rate']:.4f}")
    print(f"snapshots_upserted={result['snapshots_upserted']}")
    print(f"relations_upserted={result['relations_upserted']}")
    print(f"status_updates={result['status_updates']}")
    print(f"centroids_rebuilt={result['centroids_rebuilt']}")
    print(f"recommendation_count={result['recommendation_count']}")
    print(f"merge_recommendations={result['merge_recommendations']}")
    print(f"split_recommendations={result['split_recommendations']}")


def main() -> int:
    maybe_load_dotenv()
    args = parse_args()

    try:
        validate_args(args)
        result = run_theme_processing(args)
    except ThemeProcessingSkippedError as exc:
        print(f"SKIPPED: Theme processing {exc}")
        return EXIT_SKIPPED
    except Exception as exc:
        print(f"ERROR: Theme processing failed: {exc}")
        print(traceback.format_exc())
        return EXIT_ERROR

    print_summary(result)
    return EXIT_OK


if __name__ == "__main__":
    raise SystemExit(main())
