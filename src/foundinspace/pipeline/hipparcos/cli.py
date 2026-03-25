from pathlib import Path

import click

from foundinspace.pipeline.hipparcos import download
from foundinspace.pipeline.hipparcos.pipeline import main
from foundinspace.pipeline.paths import HIPPARCOS2_ECSV, HIP_STARS_OUTPUT


@click.group(name="hip")
def cli():
    pass


cli.add_command(download.main, name="download")


@cli.command(name="prepare")
@click.option(
    "--input",
    "-i",
    "input_file",
    type=click.Path(dir_okay=False, path_type=Path),
    default=HIPPARCOS2_ECSV,
    show_default=True,
    help="Hipparcos ECSV input path.",
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
def prepare_hip(
    input_file: Path,
    output: Path = HIP_STARS_OUTPUT,
    force: bool = False,
    limit: int | None = None,
):
    resolved_input = Path(input_file).expanduser()
    if not resolved_input.exists():
        # Backward-compatible fallback for users with the old default filename.
        if resolved_input == HIPPARCOS2_ECSV and download.LEGACY_DEFAULT_OUTPUT.exists():
            resolved_input = download.LEGACY_DEFAULT_OUTPUT
        else:
            raise click.BadParameter(
                (
                    f"Input file does not exist: {resolved_input}. "
                    "Run `fis-pipeline hip download` first or pass --input."
                ),
                param_hint="--input",
            )

    output_file = Path(output).expanduser()
    output_file.parent.mkdir(parents=True, exist_ok=True)
    main(resolved_input, output_file, skip_if_exists=not force, limit=limit)


@cli.command(name="build")
@click.option(
    "--download-output",
    type=click.Path(path_type=Path),
    default=HIPPARCOS2_ECSV,
    show_default=True,
    help="ECSV path for downloaded Hipparcos catalog.",
)
@click.option(
    "--output",
    "-o",
    type=click.Path(dir_okay=False, path_type=Path),
    default=HIP_STARS_OUTPUT,
    show_default=True,
    help="Hipparcos output path.",
)
@click.option("--force", "-f", is_flag=True, default=False)
@click.option("--limit", "-l", type=int, default=None)
def build_cmd(
    download_output: Path,
    output: Path,
    force: bool,
    limit: int | None,
) -> None:
    input_file = download.ensure_hipparcos_ecsv(download_output, force=force)
    output_file = Path(output).expanduser()
    output_file.parent.mkdir(parents=True, exist_ok=True)
    main(input_file, output_file, skip_if_exists=not force, limit=limit)
