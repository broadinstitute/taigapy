import pytest


def pytest_addoption(parser):
    parser.addoption(
        "--runlocal",
        action="store_true",
        default=False,
        help="run tests for local server",
    )
    parser.addoption(
        "--create_dataset",
        action="store_true",
        default=False,
        help="create a new dataset",
    )


def pytest_configure(config):
    config.addinivalue_line(
        "markers", "local: mark test as only available for local server"
    )
    config.addinivalue_line(
        "markers",
        "create_dataset: mark test as only run if a new dataset in the home folder should be created",
    )


def pytest_collection_modifyitems(config, items):
    if config.getoption("--runlocal") and config.getoption("--create_dataset"):
        # --runlocal given in cli: do not skip local tests
        return
    skip_local = pytest.mark.skip(reason="need --runlocal option to run")
    skip_create_dataset = pytest.mark.skip(reason="need --create_dataset option to run")
    for item in items:
        if not config.getoption("--runlocal") and "local" in item.keywords:
            item.add_marker(skip_local)
        if (
            not config.getoption("--create_dataset")
            and "create_dataset" in item.keywords
        ):
            item.add_marker(skip_create_dataset)
