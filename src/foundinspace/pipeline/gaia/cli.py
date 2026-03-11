from pathlib import Path

import click

from foundinspace.pipeline.gaia.pipeline import main


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
@click.option("--force", "-f", is_flag=True, default=False)
@click.option("--limit", "-l", type=int, default=None)
def import_gaia(
    input_files: list[Path],
    output_dir: Path | None = None,
    force: bool = False,
    limit: int | None = None,
):
    if len(input_files) == 0:
        click.echo("No input files provided")
        return

    for input_file in input_files:
        output_name = _output_path_for(input_file)
        if output_dir is not None:
            output_file = output_dir / output_name
        else:
            output_file = input_file.parent / output_name

        if not output_file.parent.exists():
            click.echo(f"Output directory {output_file.parent} does not exist")
            return

        main(input_file, output_file, skip_if_exists=not force, limit=limit)


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
