from taigapy import TaigaClient
from taigapy.UploadFile import UploadFile

if __name__ == '__main__':
    c = TaigaClient(url='http://localhost:8080/taiga', token_path='./dev_token')

    # fileOne = UploadFile(file_path='./.gitignore', format=UploadFile.InitialFileType.TableCSV)
    c.upload(dataset_name='Test name', dataset_description='Test description',
             upload_file_path_dict={'./test_upload/db_cpd_meta.csv': 'TableCSV'}, folder_id='387b86f711ad4600972bfeea23d011bb')

    df = c.get(name="csv-test-dd60", version=1)
    # print(df)