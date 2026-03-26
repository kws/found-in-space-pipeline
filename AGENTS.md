# Agent Instructions

## Python Tooling: Use `uv`

- Use `uv` for all Python dependency and environment operations in this repository.
- Do not use `pip`, `poetry`, or `conda` commands directly.

### Standard commands

- Sync/install dependencies: `uv sync`
- Run Python entrypoints/tools: `uv run <command>`
- Run tests: `uv run pytest`
- Add a dependency: `uv add <package>`
- Add a dev dependency: `uv add --dev <package>`

### Examples

- `uv run fis-pipeline --help`
- `uv run pytest tests/merge/test_pipeline.py`

