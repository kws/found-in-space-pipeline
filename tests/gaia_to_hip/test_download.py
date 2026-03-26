"""Tests for Gaia↔HIP cross-match downloading."""

from pathlib import Path

from astropy.table import Table
from click.testing import CliRunner

from foundinspace.pipeline.gaia_to_hip import download


def test_fetch_uses_async_job_and_writes_ecsv(tmp_path: Path, monkeypatch):
    output = tmp_path / "gaia_hipparcos2_best_neighbour.ecsv"
    captured: dict[str, str] = {}

    class _FakeJob:
        def get_results(self):
            return Table(
                rows=[
                    [1, 11, 0.01, 1],
                    [2, 22, 0.02, 1],
                ],
                names=(
                    "source_id",
                    "original_ext_source_id",
                    "angular_distance",
                    "number_of_neighbours",
                ),
            )

    def _fake_launch_job_async(query: str):
        captured["query"] = query
        return _FakeJob()

    def _sync_should_not_be_used(*_args, **_kwargs):
        raise AssertionError("launch_job() should not be used for this download")

    monkeypatch.setattr(download.Gaia, "launch_job_async", _fake_launch_job_async)
    monkeypatch.setattr(download.Gaia, "launch_job", _sync_should_not_be_used)

    out = download.fetch_hipparcos2_best_neighbour_to_ecsv(output, overwrite=True)

    assert out == output
    assert output.exists()
    table = Table.read(output, format="ascii.ecsv")
    assert len(table) == 2
    assert "FROM gaiadr3.hipparcos2_best_neighbour" in captured["query"]


def test_download_command_prints_row_count(tmp_path: Path, monkeypatch):
    output = tmp_path / "gaia_hipparcos2_best_neighbour.ecsv"
    Table(
        rows=[[1, 11, 0.01, 1], [2, 22, 0.02, 1]],
        names=(
            "source_id",
            "original_ext_source_id",
            "angular_distance",
            "number_of_neighbours",
        ),
    ).write(output, format="ascii.ecsv", overwrite=True)

    def _fake_ensure(path: Path, *, force: bool = False) -> Path:
        _ = force
        return Path(path)

    monkeypatch.setattr(download, "ensure_hipparcos2_best_neighbour_ecsv", _fake_ensure)

    runner = CliRunner()
    result = runner.invoke(download.main, ["--output", str(output)])

    assert result.exit_code == 0
    assert "2 rows" in result.output
