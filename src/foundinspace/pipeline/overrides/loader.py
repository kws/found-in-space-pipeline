"""Load manual override source files from the overrides data package."""

from __future__ import annotations

from importlib.resources import files
from pathlib import Path
from typing import Dict, Iterable, Iterator

_YAML_SUFFIXES = (".yaml", ".yml")


def _sorted_yaml_paths(paths: Iterable[Path]) -> Iterator[Path]:
    """Yield YAML paths in deterministic name order."""
    yield from sorted(
        (p for p in paths if p.is_file() and p.suffix.lower() in _YAML_SUFFIXES),
        key=lambda p: p.name,
    )


def iter_override_source_files(data_dir: Path | None = None) -> list[Path]:
    """Return all override YAML files in deterministic order.

    If `data_dir` is provided, files are scanned from that filesystem path.
    Otherwise, the package data directory is scanned.
    """
    if data_dir is not None:
        return list(_sorted_yaml_paths(data_dir.iterdir()))

    data_root = files("foundinspace.pipeline.overrides.data")
    paths = (Path(item) for item in data_root.iterdir())
    return list(_sorted_yaml_paths(paths))


def load_override_source_texts(data_dir: Path | None = None) -> Dict[str, str]:
    """Load all override YAML files as text keyed by filename."""
    out: Dict[str, str] = {}
    for path in iter_override_source_files(data_dir=data_dir):
        out[path.name] = path.read_text(encoding="utf-8")
    return out
