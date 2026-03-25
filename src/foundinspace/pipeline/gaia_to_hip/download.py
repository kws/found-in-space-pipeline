"""Fetch Gaia DR3 `hipparcos2_best_neighbour` cross-match from the Gaia archive (TAP)."""

from __future__ import annotations

from pathlib import Path

import click
from astroquery.gaia import Gaia

from foundinspace.pipeline.paths import GAIA_HIP_BEST_NEIGHBOUR_ECSV

BEST_NEIGHBOUR_QUERY = """
SELECT
  source_id,
  original_ext_source_id,
  angular_distance,
  number_of_neighbours
FROM gaiadr3.hipparcos2_best_neighbour
"""


def fetch_hipparcos2_best_neighbour_to_ecsv(
    output_path: Path,
    *,
    overwrite: bool = False,
) -> Path:
    """Run TAP query and write ``output_path`` (ASCII ECSV).

    Creates parent directories as needed. Pass ``overwrite=True`` to replace an
    existing file.
    """
    output_path = Path(output_path).expanduser()
    if output_path.exists() and not overwrite:
        raise FileExistsError(str(output_path))

    job = Gaia.launch_job(BEST_NEIGHBOUR_QUERY)
    result = job.get_results()

    output_path.parent.mkdir(parents=True, exist_ok=True)
    result.write(output_path, format="ascii.ecsv", overwrite=True)
    return output_path


def ensure_hipparcos2_best_neighbour_ecsv(
    output_path: Path | None = None,
    *,
    force: bool = False,
) -> Path:
    """Return path to the cross-match ECSV, downloading only if missing.

    Unless ``force`` is true, an existing file is left unchanged.
    """
    path = Path(output_path) if output_path is not None else GAIA_HIP_BEST_NEIGHBOUR_ECSV
    path = path.expanduser()

    if path.exists() and not force:
        if not path.is_file():
            raise ValueError(f"Output path exists but is not a regular file: {path}")
        return path

    return fetch_hipparcos2_best_neighbour_to_ecsv(path, overwrite=True)


@click.command()
@click.option(
    "--output",
    "-o",
    type=click.Path(path_type=Path),
    default=GAIA_HIP_BEST_NEIGHBOUR_ECSV,
    show_default=True,
    help="ECSV output path for hipparcos2_best_neighbour.",
)
@click.option(
    "--force",
    "-f",
    is_flag=True,
    default=False,
    help="Re-download even if the output file already exists.",
)
def main(output: Path, force: bool) -> None:
    path = ensure_hipparcos2_best_neighbour_ecsv(output, force=force)
    click.echo(f"hipparcos2_best_neighbour catalog ready at {path.resolve()}")


if __name__ == "__main__":
    main()
