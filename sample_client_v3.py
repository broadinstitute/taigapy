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

version = tc.update_dataset(version.permaname, "test update", additions=[
    UploadedFile('sample2', local_path=filename, format=LocalFormat.HDF5_MATRIX)
    ])
print(version)

version = tc.replace_dataset(version.permaname, "test update", files=[
    UploadedFile('sample2', local_path=filename, format=LocalFormat.HDF5_MATRIX)
    ])
print(version)

print("fetching file back down")
start = time.time()
file_id = f"{version.permaname}.{version.version_number}/sample2"
print(f"fetching {file_id}")
df = tc.get(file_id)
print(f"elapsed: {time.time()-start} seconds")

