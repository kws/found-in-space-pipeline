from pathlib import Path

import click

from foundinspace.pipeline.gaia_to_hip import download
from foundinspace.pipeline.gaia_to_hip.pipeline import prepare_gaia_hip_mapping
from foundinspace.pipeline.paths import (
    GAIA_HIP_BEST_NEIGHBOUR_ECSV,
    GAIA_HIP_MAP_OUTPUT,
)


@click.group(name="gaia-to-hip")
def cli():
    """Gaia↔Hipparcos cross-match: download `hipparcos2_best_neighbour`, build sidecar."""


cli.add_command(download.main, name="download")


@cli.command(name="prepare")
@click.option(
    "--input",
    "-i",
    "input_ecsv",
    type=click.Path(exists=True, dir_okay=False, path_type=Path),
    default=GAIA_HIP_BEST_NEIGHBOUR_ECSV,
    show_default=True,
    help="Cross-match ECSV from `gaia-to-hip download`.",
)
@click.option(
    "--output",
    "-o",
    type=click.Path(dir_okay=False, path_type=Path),
    default=GAIA_HIP_MAP_OUTPUT,
    show_default=True,
    help="Gaia↔HIP mapping Parquet output path.",
)
@click.option("--force", "-f", is_flag=True, default=False)
def prepare_cmd(
    input_ecsv: Path,
    output: Path,
    force: bool,
) -> None:
    out = prepare_gaia_hip_mapping(input_ecsv, output, overwrite=force)
    click.echo(f"Wrote Gaia↔HIP mapping sidecar to {out.resolve()}")


@cli.command(name="build")
@click.option(
    "--download-output",
    type=click.Path(path_type=Path),
    default=GAIA_HIP_BEST_NEIGHBOUR_ECSV,
    show_default=True,
    help="ECSV path for downloaded cross-match table.",
)
@click.option(
    "--output",
    "-o",
    type=click.Path(dir_okay=False, path_type=Path),
    default=GAIA_HIP_MAP_OUTPUT,
    show_default=True,
    help="Gaia↔HIP mapping Parquet output path.",
)
@click.option("--force", "-f", is_flag=True, default=False)
def build_cmd(
    download_output: Path,
    output: Path,
    force: bool,
) -> None:
    download.ensure_hipparcos2_best_neighbour_ecsv(download_output, force=force)
    out = prepare_gaia_hip_mapping(download_output, output, overwrite=force)
    click.echo(f"Wrote Gaia↔HIP mapping sidecar to {out.resolve()}")
