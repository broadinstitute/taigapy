from taigapy.client_v3 import UploadedFile, LocalFormat
from taigapy import create_taiga_client_v3
import time

tc = create_taiga_client_v3()
filename = "sample-100x100.hdf5"
start = time.time()
version = tc.create_dataset("test-client-v3", "test client", files=[
    UploadedFile(filename, local_path=filename, format=LocalFormat.HDF5_MATRIX)
    ])
print(f"elapsed: {time.time()-start} seconds")

print("fetching file back down")
start = time.time()
df = tc.get(f"{version.permaname}.{version.version_number}/{filename}")
print(f"elapsed: {time.time()-start} seconds")

