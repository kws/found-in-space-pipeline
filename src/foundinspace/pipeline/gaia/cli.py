from pathlib import Path

import click

from foundinspace.pipeline.gaia.pipeline import (
    combine_gaia_hip_mappings,
    main,
    write_gaia_hip_mapping,
)
from foundinspace.pipeline.paths import GAIA_HIP_MAP_OUTPUT, PROCESSED_GAIA_DIR


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
    "--output-dir",
    "-o",
    type=click.Path(file_okay=False, path_type=Path),
    default=PROCESSED_GAIA_DIR,
    show_default=True,
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
    output_dir: Path = PROCESSED_GAIA_DIR,
    mapping_output: Path | None = None,
    force: bool = False,
    limit: int | None = None,
):
    if len(input_files) == 0:
        click.echo("No input files provided")
        return

    output_root = Path(output_dir).expanduser()
    output_root.mkdir(parents=True, exist_ok=True)

    mapping_output_file = _mapping_output_path_for(
        mapping_output=mapping_output,
    )
    mapping_output_file.parent.mkdir(parents=True, exist_ok=True)
    if not force and mapping_output_file.exists():
        click.echo(
            f"Mapping output exists and --force not set: {mapping_output_file}"
        )
        return

    mapping_chunks = []
    for input_file in input_files:
        output_name = _output_path_for(input_file)
        output_file = output_root / output_name

        mapping = main(input_file, output_file, skip_if_exists=not force, limit=limit)
        if not mapping.empty:
            mapping_chunks.append(mapping)

    combined_mapping = combine_gaia_hip_mappings(mapping_chunks)
    write_gaia_hip_mapping(combined_mapping, mapping_output_file)
    click.echo(
        f"Wrote Gaia↔HIP mapping sidecar with {len(combined_mapping):,} rows to {mapping_output_file}"
    )


def _output_path_for(input_path: Path) -> str:
    """Parquet output filename for a given VOTable input (stem from name)."""
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
    mapping_output: Path | None,
) -> Path:
    if mapping_output is not None:
        return mapping_output.expanduser()
    return GAIA_HIP_MAP_OUTPUT
