from taigapy import Taiga1Client as TaigaClient
import os

def test_get(tmpdir):
    cache_dir = str(tmpdir.join("cache"))
    
    def get_cached_count():
        return len(os.listdir(cache_dir))
    
    c = TaigaClient(cache_dir=cache_dir)
    df = c.get(id='6d9a6104-e2f8-45cf-9002-df3bcedcb80b')
    assert df is not None
    assert get_cached_count() == 1

    df1 = c.get(name='achilles-v2-4-6', version=4)
    assert df1 is not None
    assert get_cached_count() == 1

    df2 = c.get(name='achilles-v2-4-6', version=3)
    assert df2 is not None
    assert get_cached_count() == 2

def test_get_table(tmpdir):
    # test fetch table
    cache_dir = str(tmpdir.join("cache"))
    c = TaigaClient(cache_dir=cache_dir)
    df3 = c.get(name="kegg-ribosome", version=1)
    assert df3 is not None
    