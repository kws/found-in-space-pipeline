from foundinspace.pipeline.overrides import (
    iter_override_source_files,
    load_override_source_texts,
)


def test_iter_override_source_files_includes_sun_yaml():
    files = iter_override_source_files()
    names = [p.name for p in files]
    assert "sun.yaml" in names


def test_load_override_source_texts_reads_packaged_yaml():
    sources = load_override_source_texts()
    assert "sun.yaml" in sources
    assert "manual.sun.add.v1" in sources["sun.yaml"]
