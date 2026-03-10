"""Backfill theme assignment and maintenance over a rolling lookback window."""

from __future__ import annotations

import argparse
import sys
import traceback
from pathlib import Path

# Ensure `app` package imports work when running this file directly.
BACKEND_ROOT = Path(__file__).resolve().parents[1]
PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

from app.db.session import SessionLocal
from app.pipelines.theme_management_pipeline import ThemeManagementPipeline
from app.repositories.theme_repository import ThemeRepository
from app.services.theme_management_service import ThemeManagementService


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Backfill and re-assign themes for recent articles.")
    parser.add_argument(
        "--days",
        type=int,
        default=120,
        help="Only process unlinked articles observed in the last N days.",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=500,
        help="Assignment batch size per run iteration.",
    )
    parser.add_argument(
        "--max-runs",
        type=int,
        default=200,
        help="Maximum loop iterations before stopping.",
    )
    parser.add_argument(
        "--snapshot-lookback-days",
        type=int,
        default=120,
        help="Snapshot refresh lookback.",
    )
    parser.add_argument(
        "--relation-lookback-days",
        type=int,
        default=90,
        help="Relation refresh lookback.",
    )
    parser.add_argument(
        "--run-maintenance",
        action="store_true",
        help="Run merge/split recommendation pass after backfill loop.",
    )
    parser.add_argument(
        "--rebuild-centroids",
        action="store_true",
        help="Rebuild theme centroids after loop completion.",
    )
    return parser.parse_args()


def maybe_load_dotenv() -> None:
    try:
        from dotenv import load_dotenv
    except ImportError:
        return
    load_dotenv(PROJECT_ROOT / ".env")


def main() -> int:
    maybe_load_dotenv()
    args = parse_args()
    if args.days <= 0:
        print("ERROR: --days must be > 0")
        return 1
    if args.batch_size <= 0:
        print("ERROR: --batch-size must be > 0")
        return 1
    if args.max_runs <= 0:
        print("ERROR: --max-runs must be > 0")
        return 1

    session = SessionLocal()
    totals = {
        "runs": 0,
        "assignment_input_count": 0,
        "assigned_articles": 0,
        "theme_links_upserted": 0,
        "created_themes": 0,
        "promoted_candidates": 0,
        "abstained_articles": 0,
        "abstained_signals": 0,
        "candidate_observations": 0,
        "snapshots_upserted": 0,
        "relations_upserted": 0,
        "status_updates": 0,
        "centroids_rebuilt": 0,
        "recommendation_count": 0,
        "merge_recommendations": 0,
        "split_recommendations": 0,
    }

    try:
        repository = ThemeRepository(session)
        service = ThemeManagementService(theme_repository=repository)
        pipeline = ThemeManagementPipeline(
            theme_repository=repository,
            theme_management_service=service,
        )

        for _ in range(args.max_runs):
            run_result = pipeline.run(
                assignment_limit=args.batch_size,
                assignment_lookback_days=args.days,
                snapshot_lookback_days=args.snapshot_lookback_days,
                relation_lookback_days=args.relation_lookback_days,
                run_maintenance=False,
                rebuild_centroids=False,
                record_run=True,
            )
            totals["runs"] += 1
            for key in totals:
                if key == "runs":
                    continue
                totals[key] += int(run_result.get(key, 0))

            assignment_input_count = int(run_result.get("assignment_input_count", 0))
            if assignment_input_count == 0:
                break

        final_result = pipeline.run(
            assignment_limit=1,
            assignment_lookback_days=args.days,
            snapshot_lookback_days=args.snapshot_lookback_days,
            relation_lookback_days=args.relation_lookback_days,
            run_maintenance=args.run_maintenance,
            rebuild_centroids=args.rebuild_centroids,
            record_run=True,
        )
        for key in totals:
            if key == "runs":
                continue
            totals[key] += int(final_result.get(key, 0))
    except Exception as exc:
        print(f"ERROR: Theme backfill failed: {exc}")
        print(traceback.format_exc())
        return 1
    finally:
        session.close()

    print("Theme backfill summary:")
    for key, value in totals.items():
        print(f"{key}={value}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
