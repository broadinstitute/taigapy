import pytest
import os	
from taigapy import Taiga2Client as TaigaClient

token_path = os.path.expanduser("~/.taiga/token")

@pytest.fixture(scope="session")
def taigaClient(tmpdir_factory):
    cache_dir = str(tmpdir_factory.getbasetemp().join("cache"))
    c = TaigaClient(cache_dir=cache_dir, token_path=token_path)
    return c