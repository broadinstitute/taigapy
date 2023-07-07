__version__ = "3.3.4"

from .consts import DEFAULT_TAIGA_URL, DEFAULT_CACHE_DIR, CACHE_FILE
from .client import TaigaClient

try:
    default_tc = TaigaClient()
except Exception as e:
    print("default_tc could not be set for this reason: {}".format(e))
    print(
        "You can import TaigaClient and add your custom options if you would want to customize it to your settings"
    )

from .client_v3 import Client as ClientV3

def create_taiga_client_v3(*args, **kwargs):
    tc = TaigaClient(*args, **kwargs)
    tc._set_token_and_initialized_api()
    return ClientV3(tc.cache_dir, tc.api)
