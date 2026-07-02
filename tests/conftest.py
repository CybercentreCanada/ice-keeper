import os
from collections.abc import Generator

import pytest

from ice_keeper import TimeProvider
from ice_keeper.config import ICEKEEPER_CONFIG, Config


@pytest.fixture(autouse=True)
def reset_config() -> None:
    config_file = os.environ.get(ICEKEEPER_CONFIG)
    assert config_file is not None
    Config.load_config(config_file)


@pytest.fixture(autouse=True)
def reset_time_provider() -> Generator[None, None, None]:
    """Ensure tests do not leak a fixed TimeProvider value across cases."""
    TimeProvider.fixed_time = None
    yield
    TimeProvider.fixed_time = None
