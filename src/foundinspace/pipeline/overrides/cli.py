from pathlib import Path

import click

from foundinspace.pipeline.overrides.pipeline import prepare_overrides_parquet
from foundinspace.pipeline.paths import OVERRIDES_OUTPUT


@click.group(name="overrides")
def cli():
    """Manual override YAML → processed Parquet for merger."""


@cli.command(name="prepare")
@click.option(
    "--data-dir",
    type=click.Path(file_okay=False, path_type=Path),
    default=None,
    help="Directory of override YAML files (default: packaged overrides/data).",
)
@click.option(
    "--output",
    "-o",
    type=click.Path(dir_okay=False, path_type=Path),
    default=OVERRIDES_OUTPUT,
    show_default=True,
    help="Output Parquet path.",
)
@click.option("--force", "-f", is_flag=True, default=False)
def prepare(data_dir: Path | None, output: Path, force: bool) -> None:
    """Write OUTPUT_COLS + override metadata to Parquet (zstd)."""
    out = prepare_overrides_parquet(
        output,
        data_dir=data_dir if data_dir is not None else None,
        overwrite=force,
    )
    click.echo(f"Wrote overrides table to {out.resolve()}")
