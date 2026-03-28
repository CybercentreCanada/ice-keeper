import os

import pytest

from ice_keeper import configure_logger
from ice_keeper.config import ICEKEEPER_CONFIG, Config


@pytest.fixture(autouse=True)
def reset_config() -> None:
    config_file = os.environ.get(ICEKEEPER_CONFIG)
    assert config_file is not None
    Config.load_config(config_file)
    configure_logger()
