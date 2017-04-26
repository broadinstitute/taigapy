from taigapy import Taiga2Client as TaigaClient
import os

token_path = os.path.expanduser("~/.taiga/token")

def test_get(tmpdir):
    cache_dir = str(tmpdir.join("cache"))
    
    def get_cached_count():
        return len(os.listdir(cache_dir))
    
    c = TaigaClient(cache_dir=cache_dir, token_path=token_path)
    df = c.get(id='b9a6c877-37cb-4ebb-8c05-3385ff9a5ec7')
    assert df is not None
    assert get_cached_count() == 1

    df1 = c.get(name='depcon-binary-context-matrix', version=1)
    assert df1 is not None
    assert get_cached_count() == 1
    #
    # df2 = c.get(name='depcon-binary-context-matrix', version=3)
    # assert df2 is not None
    # assert get_cached_count() == 2

def test_get_table(tmpdir):
    # test fetch table
    cache_dir = str(tmpdir.join("cache"))
    c = TaigaClient(cache_dir=cache_dir, token_path=token_path)
    df3 = c.get(name="lineage-colors", version=1)
    assert df3 is not None

