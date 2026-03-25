from pathlib import Path

import click

from foundinspace.pipeline.gaia.pipeline import main
from foundinspace.pipeline.paths import PROCESSED_GAIA_DIR


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
@click.option("--force", "-f", is_flag=True, default=False)
@click.option(
    "--mag-limit",
    type=float,
    default=None,
    help="Keep only rows with Gaia G magnitude <= this value.",
)
def import_gaia(
    input_files: list[Path],
    output_dir: Path = PROCESSED_GAIA_DIR,
    force: bool = False,
    mag_limit: float | None = None,
):
    if len(input_files) == 0:
        click.echo("No input files provided")
        return

    output_root = Path(output_dir).expanduser()
    output_root.mkdir(parents=True, exist_ok=True)

    for input_file in input_files:
        output_name = _output_path_for(input_file)
        output_file = output_root / output_name
        main(
            input_file,
            output_file,
            skip_if_exists=not force,
            mag_limit=mag_limit,
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
