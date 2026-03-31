from pathlib import Path

import click

from foundinspace.pipeline.identifiers import download
from foundinspace.pipeline.identifiers.pipeline import prepare_identifiers_sidecar
from foundinspace.pipeline.project import PipelineProject, load_project


@click.group(name="identifiers")
def cli():
    pass


cli.add_command(download.main, name="download")


def _load_project_or_die(project_path: Path, *required: str) -> PipelineProject:
    try:
        project = load_project(project_path)
        if required:
            project.require(*required)
        return project
    except ValueError as exc:
        raise click.ClickException(str(exc)) from exc


@cli.command(name="build")
@click.option(
    "--project",
    "project_path",
    required=True,
    type=click.Path(exists=True, dir_okay=False, path_type=Path),
    help="Path to pipeline project TOML.",
)
@click.option("--force", "-f", is_flag=True, default=False)
def build(
    project_path: Path,
    force: bool,
) -> None:
    project = _load_project_or_die(project_path, "identifiers")
    outputs = download.ensure_identifier_catalogs(
        hip_hd_output=project.identifiers.hip_hd_ecsv,
        iv27a_catalog_output=project.identifiers.iv27a_catalog_ecsv,
        iv27a_proper_names_output=project.identifiers.iv27a_proper_names_ecsv,
        force=force,
    )
    crossmatch_parquet = (
        project.gaia_to_hip.output_parquet if project.gaia_to_hip.is_configured else None
    )
    overrides_data_dir = (
        project.overrides.data_dir if project.overrides.is_configured else None
    )
    out = prepare_identifiers_sidecar(
        outputs["hip_hd"],
        outputs["iv27a_catalog"],
        outputs["iv27a_proper_names"],
        project.identifiers.output_parquet,
        crossmatch_parquet=crossmatch_parquet,
        overrides_data_dir=overrides_data_dir,
        overwrite=force,
    )
    click.echo(f"Wrote wide identifier sidecar to {out.resolve()}")
