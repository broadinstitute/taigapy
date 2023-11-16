import pytest
from taigapy.client_v3 import Client as ClientV3
from taigapy import TaigaClient

@pytest.fixture
def taiga_client(tmpdir):
    cache_dir = str(tmpdir.join("cache"))
    tc_v2 = TaigaClient(cache_dir=cache_dir, token_path="~/.taiga/token")

    tc_v2._set_token_and_initialized_api()
    return ClientV3(tc_v2.cache_dir, tc_v2.api)

def test_canonicalize_taiga_id(taiga_client):
    assert taiga_client.get_canonical_id("small-gecko-aff0.1") == "small-gecko-aff0.1/gecko_score"
    assert taiga_client.get_canonical_id("small-gecko-aff0.1/gecko_score") == "small-gecko-aff0.1/gecko_score"
    assert taiga_client.get_canonical_id("small-gecko-aff0.2") == "small-gecko-aff0.1/gecko_score"
    assert taiga_client.get_canonical_id("small-gecko-aff0.2/gecko_score") == "small-gecko-aff0.1/gecko_score"

def test_get(taiga_client):
    df = taiga_client.get("small-gecko-aff0.1/gecko_score")
    assert df.shape ==  (9, 6)