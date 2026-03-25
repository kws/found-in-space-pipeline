from pathlib import Path

import click

from foundinspace.pipeline.gaia.pipeline import (
    combine_gaia_hip_mappings,
    main,
    write_gaia_hip_mapping,
)


@click.group(name="gaia")
def cli():
    pass


@cli.command(name="import")
@click.argument(
    "input_files",
    nargs=-1,
    type=click.Path(exists=True, dir_okay=False, path_type=Path),
)
@click.option(
    "--output-dir", "-o", type=click.Path(file_okay=False, path_type=Path), default=None
)
@click.option(
    "--mapping-output",
    type=click.Path(dir_okay=False, path_type=Path),
    default=None,
    help="Run-level Gaia↔HIP mapping output path (Parquet).",
)
@click.option("--force", "-f", is_flag=True, default=False)
@click.option("--limit", "-l", type=int, default=None)
def import_gaia(
    input_files: list[Path],
    output_dir: Path | None = None,
    mapping_output: Path | None = None,
    force: bool = False,
    limit: int | None = None,
):
    if len(input_files) == 0:
        click.echo("No input files provided")
        return

    mapping_output_file = _mapping_output_path_for(
        input_files=input_files,
        output_dir=output_dir,
        mapping_output=mapping_output,
    )
    if not mapping_output_file.parent.exists():
        click.echo(f"Mapping output directory {mapping_output_file.parent} does not exist")
        return
    if not force and mapping_output_file.exists():
        click.echo(
            f"Mapping output exists and --force not set: {mapping_output_file}"
        )
        return

    mapping_chunks = []
    for input_file in input_files:
        output_name = _output_path_for(input_file)
        if output_dir is not None:
            output_file = output_dir / output_name
        else:
            output_file = input_file.parent / output_name

        if not output_file.parent.exists():
            click.echo(f"Output directory {output_file.parent} does not exist")
            return

        mapping = main(input_file, output_file, skip_if_exists=not force, limit=limit)
        if not mapping.empty:
            mapping_chunks.append(mapping)

    combined_mapping = combine_gaia_hip_mappings(mapping_chunks)
    write_gaia_hip_mapping(combined_mapping, mapping_output_file)
    click.echo(
        f"Wrote Gaia↔HIP mapping sidecar with {len(combined_mapping):,} rows to {mapping_output_file}"
    )


def _output_path_for(input_path: Path) -> str:
    """Parquet output path for a given VOTable input (same dir, stem from name)."""
    name_lower = input_path.name.lower()
    if name_lower.endswith(".vot.gz"):
        output_base = input_path.name[: -len(".vot.gz")]
    elif name_lower.endswith(".vot.xz"):
        output_base = input_path.name[: -len(".vot.xz")]
    else:
        output_base = input_path.stem
    return f"{output_base}.parquet"


def _mapping_output_path_for(
    *,
    input_files: list[Path],
    output_dir: Path | None,
    mapping_output: Path | None,
) -> Path:
    if mapping_output is not None:
        return mapping_output
    if output_dir is not None:
        return output_dir / "gaia_hip_map.parquet"

    parents = {path.parent for path in input_files}
    if len(parents) != 1:
        raise click.ClickException(
            "Input files are in multiple directories; provide --mapping-output."
        )
    parent = next(iter(parents))
    return parent / "gaia_hip_map.parquet"
