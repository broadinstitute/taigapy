from taigapy import TaigaClient
from taigapy.UploadFile import UploadFile

if __name__ == "__main__":
    c = TaigaClient(url="http://localhost:8080/taiga", token_path="./dev_token")

    # fileOne = UploadFile(file_path='./.gitignore', format=UploadFile.InitialFileType.TableCSV)
    # dataset_id = c.create_dataset(dataset_name='Test name', dataset_description='Test description',
    #                               upload_file_path_dict={'./test_upload/db_cpd_meta.csv': 'TableCSV'},
    #                               folder_id='387b86f711ad4600972bfeea23d011bb')
    #
    # print("\nTesting update_dataset interactive")
    # c.update_dataset(dataset_id=dataset_id, upload_file_path_dict={'./test_upload/DESCRIPTION.txt': 'Raw'},
    #                  dataset_description="Interactive test")
    #
    # print("\nTesting update_dataset with force_keep")
    # c.update_dataset(dataset_id=dataset_id, upload_file_path_dict={'./test_upload/db_plate_meta.csv': 'TableCSV'},
    #                  force_keep=True, dataset_description="Force Keep test")
    #
    # print("\nTesting update_dataset with force_remove")
    # c.update_dataset(dataset_id=dataset_id, upload_file_path_dict={'./test_upload/db_cl_meta.csv': 'TableCSV'},
    #                  force_remove=True, dataset_description="Force remove test")

    print("\nTesting update_dataset by dataset permaname, with force_keep")
    c.update_dataset(
        dataset_permaname="csv-test-dd60",
        upload_file_path_dict={"./test_upload/db_cl_meta.csv": "TableCSV"},
        force_remove=True,
        dataset_description="Permaname update (latest version)",
    )

    # df = c.get(name="csv-test-dd60", version=1)
