from pathlib import Path

import click

from foundinspace.pipeline.merge.pipeline import run_merge
from foundinspace.pipeline.paths import (
    GAIA_HIP_MAP_OUTPUT,
    HIP_STARS_OUTPUT,
    MERGED_OUTPUT_DIR,
    OVERRIDES_OUTPUT,
    PROCESSED_GAIA_DIR,
)


@click.group(name="merge")
def cli():
    """Merge Gaia/HIP/overrides into HEALPix-partitioned Parquet output."""


@cli.command(name="prepare")
@click.option(
    "--gaia-dir",
    type=click.Path(file_okay=False, path_type=Path),
    default=PROCESSED_GAIA_DIR,
    show_default=True,
    help="Directory with prepared Gaia parquet batches.",
)
@click.option(
    "--hip-input",
    type=click.Path(dir_okay=False, path_type=Path),
    default=HIP_STARS_OUTPUT,
    show_default=True,
    help="Prepared Hipparcos parquet path.",
)
@click.option(
    "--crossmatch-input",
    type=click.Path(dir_okay=False, path_type=Path),
    default=GAIA_HIP_MAP_OUTPUT,
    show_default=True,
    help="Gaia↔HIP crossmatch parquet path.",
)
@click.option(
    "--overrides-input",
    type=click.Path(dir_okay=False, path_type=Path),
    default=OVERRIDES_OUTPUT,
    show_default=True,
    help="Prepared overrides parquet path.",
)
@click.option(
    "--output-dir",
    "-o",
    type=click.Path(file_okay=False, path_type=Path),
    default=MERGED_OUTPUT_DIR,
    show_default=True,
    help="Merge output directory root.",
)
@click.option(
    "--healpix-order",
    type=int,
    default=3,
    show_default=True,
    help="HEALPix order (nside = 2**order).",
)
@click.option("--force", "-f", is_flag=True, default=False)
def prepare(
    gaia_dir: Path,
    hip_input: Path,
    crossmatch_input: Path,
    overrides_input: Path,
    output_dir: Path,
    healpix_order: int,
    force: bool,
) -> None:
    """Run the streaming merge and emit HEALPix-partitioned outputs."""
    report = run_merge(
        gaia_dir=gaia_dir,
        hip_path=hip_input,
        crossmatch_path=crossmatch_input,
        overrides_path=overrides_input,
        output_dir=output_dir,
        healpix_order=healpix_order,
        force=force,
    )
    click.echo(f"Wrote merged shards under {(output_dir / 'healpix').resolve()}")
    click.echo(f"Merge report: {(output_dir / 'merge_report.json').resolve()}")
    click.echo(
        "Summary: "
        f"emitted={report.rows_emitted_total:,}, "
        f"matched={report.matched_pairs_scored:,}, "
        f"unmatched_gaia={report.unmatched_gaia:,}, "
        f"unmatched_hip={report.unmatched_hip:,}"
    )

