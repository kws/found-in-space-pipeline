from pathlib import Path

import click

from foundinspace.pipeline.identifiers import download
from foundinspace.pipeline.identifiers.download import (
    DEFAULT_HIP_HD_OUTPUT,
    DEFAULT_IV27A_CATALOG_OUTPUT,
    DEFAULT_IV27A_PROPER_NAMES_OUTPUT,
)
from foundinspace.pipeline.identifiers.pipeline import prepare_identifiers_sidecar
from foundinspace.pipeline.paths import IDENTIFIERS_MAP_OUTPUT

DEFAULT_SIDECAR_OUTPUT = IDENTIFIERS_MAP_OUTPUT


@click.group(name="identifiers")
def cli():
    pass


cli.add_command(download.main, name="download")


@cli.command(name="prepare")
@click.option(
    "--hip-hd",
    type=click.Path(exists=True, dir_okay=False, path_type=Path),
    default=DEFAULT_HIP_HD_OUTPUT,
    show_default=True,
    help="HIP→HD ECSV input path.",
)
@click.option(
    "--catalog",
    type=click.Path(exists=True, dir_okay=False, path_type=Path),
    default=DEFAULT_IV27A_CATALOG_OUTPUT,
    show_default=True,
    help="IV/27A catalog ECSV input path.",
)
@click.option(
    "--proper-names",
    type=click.Path(exists=True, dir_okay=False, path_type=Path),
    default=DEFAULT_IV27A_PROPER_NAMES_OUTPUT,
    show_default=True,
    help="IV/27A proper names ECSV input path.",
)
@click.option(
    "--output",
    "-o",
    type=click.Path(dir_okay=False, path_type=Path),
    default=DEFAULT_SIDECAR_OUTPUT,
    show_default=True,
    help="Prepared identifiers sidecar output path (Parquet).",
)
@click.option("--force", "-f", is_flag=True, default=False)
def prepare(
    hip_hd: Path,
    catalog: Path,
    proper_names: Path,
    output: Path,
    force: bool,
) -> None:
    out = prepare_identifiers_sidecar(
        hip_hd,
        catalog,
        proper_names,
        output,
        overwrite=force,
    )
    click.echo(f"Wrote wide identifier sidecar to {out.resolve()}")


@cli.command(name="build")
@click.option(
    "--hip-hd-output",
    type=click.Path(path_type=Path),
    default=DEFAULT_HIP_HD_OUTPUT,
    show_default=True,
    help="ECSV output path for HIP→HD mapping (I/239/hip_main).",
)
@click.option(
    "--catalog-output",
    type=click.Path(path_type=Path),
    default=DEFAULT_IV27A_CATALOG_OUTPUT,
    show_default=True,
    help="ECSV output path for IV/27A/catalog.",
)
@click.option(
    "--proper-names-output",
    type=click.Path(path_type=Path),
    default=DEFAULT_IV27A_PROPER_NAMES_OUTPUT,
    show_default=True,
    help="ECSV output path for IV/27A/table3 proper names.",
)
@click.option(
    "--output",
    "-o",
    type=click.Path(dir_okay=False, path_type=Path),
    default=DEFAULT_SIDECAR_OUTPUT,
    show_default=True,
    help="Prepared identifiers sidecar output path (Parquet).",
)
@click.option("--force", "-f", is_flag=True, default=False)
def build(
    hip_hd_output: Path,
    catalog_output: Path,
    proper_names_output: Path,
    output: Path,
    force: bool,
) -> None:
    outputs = download.ensure_identifier_catalogs(
        hip_hd_output=hip_hd_output,
        iv27a_catalog_output=catalog_output,
        iv27a_proper_names_output=proper_names_output,
        force=force,
    )
    out = prepare_identifiers_sidecar(
        outputs["hip_hd"],
        outputs["iv27a_catalog"],
        outputs["iv27a_proper_names"],
        output,
        overwrite=force,
    )
    click.echo(f"Wrote wide identifier sidecar to {out.resolve()}")
