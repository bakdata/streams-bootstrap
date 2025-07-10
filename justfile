set shell := ["bash", "-uc"]

base-directory := justfile_directory()

_default:
    @just --list --unsorted

###############################################################################
## Documentation
###############################################################################

docs-config := base-directory / "docs/mkdocs.yml"

# Serve current docs located in ./docs/docs
serve-docs port="8000":
    poetry run mkdocs serve --config-file {{ docs-config }} --dev-addr localhost:{{ port }}
