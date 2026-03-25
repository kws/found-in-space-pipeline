import numpy as np

from foundinspace.pipeline.overrides import (
    icrs_spherical_to_cartesian_pc,
    iter_override_source_files,
    load_normalized_override_stars,
    load_override_source_texts,
)


def test_iter_override_source_files_includes_sun_yaml():
    files = iter_override_source_files()
    names = [p.name for p in files]
    assert "sun.yaml" in names
    assert "alpha_cen.yaml" in names


def test_load_override_source_texts_reads_packaged_yaml():
    sources = load_override_source_texts()
    assert "sun.yaml" in sources
    assert "manual.sun.add.v1" in sources["sun.yaml"]


def test_load_normalized_override_stars_fills_cartesian_for_alpha_cen():
    stars = load_normalized_override_stars()
    by_id = {s["override_id"]: s for s in stars}
    a = by_id["manual.alpha_cen_a.replace.v1"]
    assert "x_icrs_pc" in a and "y_icrs_pc" in a and "z_icrs_pc" in a
    ex, ey, ez = icrs_spherical_to_cartesian_pc(
        float(a["ra_deg"]),
        float(a["dec_deg"]),
        float(a["r_pc"]),
    )
    assert np.allclose([a["x_icrs_pc"], a["y_icrs_pc"], a["z_icrs_pc"]], [ex, ey, ez])


def test_sun_override_keeps_explicit_cartesian():
    stars = load_normalized_override_stars()
    sun = next(s for s in stars if s["override_id"] == "manual.sun.add.v1")
    assert sun["x_icrs_pc"] == 0.0
    assert sun["y_icrs_pc"] == 0.0
    assert sun["z_icrs_pc"] == 0.0
