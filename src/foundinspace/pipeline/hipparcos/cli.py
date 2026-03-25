from pathlib import Path

import click

from foundinspace.pipeline.hipparcos import download
from foundinspace.pipeline.hipparcos.pipeline import main


@click.group(name="hip")
def cli():
    pass


cli.add_command(download.main, name="download")


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
def import_hip(
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
    name_lower = input_path.name.lower()
    if name_lower.endswith(".ecsv"):
        output_base = input_path.name[: -len(".ecsv")]
    else:
        output_base = input_path.stem
    return f"{output_base}.parquet"
