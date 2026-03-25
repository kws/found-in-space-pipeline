from pathlib import Path

import click

from foundinspace.pipeline.hipparcos import download
from foundinspace.pipeline.hipparcos.pipeline import main
from foundinspace.pipeline.paths import HIP_STARS_OUTPUT


@click.group(name="hip")
def cli():
    pass


cli.add_command(download.main, name="download")


@cli.command(name="import")
@click.argument(
    "input_file",
    type=click.Path(exists=True, dir_okay=False, path_type=Path),
)
@click.option(
    "--output",
    "-o",
    type=click.Path(dir_okay=False, path_type=Path),
    default=HIP_STARS_OUTPUT,
    show_default=True,
)
@click.option("--force", "-f", is_flag=True, default=False)
@click.option("--limit", "-l", type=int, default=None)
def import_hip(
    input_file: Path,
    output: Path = HIP_STARS_OUTPUT,
    force: bool = False,
    limit: int | None = None,
):
    output_file = Path(output).expanduser()
    output_file.parent.mkdir(parents=True, exist_ok=True)
    main(input_file, output_file, skip_if_exists=not force, limit=limit)
