"""Backward-compatible alias for the theme processing entrypoint."""

from __future__ import annotations

from process_themes import main


if __name__ == "__main__":
    raise SystemExit(main())
