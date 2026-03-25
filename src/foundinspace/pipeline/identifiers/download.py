"""Fetch identifier source catalogs from Vizier as ECSV files."""

from __future__ import annotations

from pathlib import Path

import click
from astroquery.vizier import Vizier

HIP_HD_CATALOG = "I/239/hip_main"
IV27A_CATALOG = "IV/27A/catalog"
IV27A_PROPER_NAMES = "IV/27A/table3"

DEFAULT_OUTPUT_DIR = Path("downloads")
DEFAULT_HIP_HD_OUTPUT = DEFAULT_OUTPUT_DIR / "hip_hd.ecsv"
DEFAULT_IV27A_CATALOG_OUTPUT = DEFAULT_OUTPUT_DIR / "iv27a_catalog.ecsv"
DEFAULT_IV27A_PROPER_NAMES_OUTPUT = DEFAULT_OUTPUT_DIR / "iv27a_proper_names.ecsv"


def _fetch_catalog_to_ecsv(
    *,
    catalog: str,
    columns: list[str],
    output_path: Path,
    overwrite: bool = False,
) -> Path:
    output_path = Path(output_path).expanduser()
    if output_path.exists() and not overwrite:
        raise FileExistsError(str(output_path))

    v = Vizier(columns=columns, row_limit=-1)
    tables = v.get_catalogs(catalog)
    table = tables[0]

    output_path.parent.mkdir(parents=True, exist_ok=True)
    table.write(output_path, format="ascii.ecsv", overwrite=True)
    return output_path


def ensure_identifier_catalogs(
    *,
    hip_hd_output: Path = DEFAULT_HIP_HD_OUTPUT,
    iv27a_catalog_output: Path = DEFAULT_IV27A_CATALOG_OUTPUT,
    iv27a_proper_names_output: Path = DEFAULT_IV27A_PROPER_NAMES_OUTPUT,
    force: bool = False,
) -> dict[str, Path]:
    outputs = {
        "hip_hd": Path(hip_hd_output).expanduser(),
        "iv27a_catalog": Path(iv27a_catalog_output).expanduser(),
        "iv27a_proper_names": Path(iv27a_proper_names_output).expanduser(),
    }
    if not force and all(path.exists() and path.is_file() for path in outputs.values()):
        return outputs

    outputs["hip_hd"] = _fetch_catalog_to_ecsv(
        catalog=HIP_HD_CATALOG,
        columns=["HIP", "HD"],
        output_path=outputs["hip_hd"],
        overwrite=True,
    )
    outputs["iv27a_catalog"] = _fetch_catalog_to_ecsv(
        catalog=IV27A_CATALOG,
        columns=["HIP", "HD", "Bayer", "Fl", "Cst"],
        output_path=outputs["iv27a_catalog"],
        overwrite=True,
    )
    outputs["iv27a_proper_names"] = _fetch_catalog_to_ecsv(
        catalog=IV27A_PROPER_NAMES,
        columns=["HD", "Name"],
        output_path=outputs["iv27a_proper_names"],
        overwrite=True,
    )
    return outputs


@click.command()
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
    "--force",
    "-f",
    is_flag=True,
    default=False,
    help="Re-download files even when all outputs already exist.",
)
def main(
    hip_hd_output: Path,
    catalog_output: Path,
    proper_names_output: Path,
    force: bool,
) -> None:
    outputs = ensure_identifier_catalogs(
        hip_hd_output=hip_hd_output,
        iv27a_catalog_output=catalog_output,
        iv27a_proper_names_output=proper_names_output,
        force=force,
    )
    click.echo(f"HIP→HD catalog ready at {outputs['hip_hd'].resolve()}")
    click.echo(f"IV/27A catalog ready at {outputs['iv27a_catalog'].resolve()}")
    click.echo(
        "IV/27A proper names catalog ready at "
        f"{outputs['iv27a_proper_names'].resolve()}"
    )


if __name__ == "__main__":
    main()
