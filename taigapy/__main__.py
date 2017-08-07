import argparse
import json

from taigapy import TaigaClient


def create(args):
    taiga_client = init_taiga(args.token)
    taiga_client.create_dataset(dataset_name=args.name,
                                dataset_description=args.description,
                                upload_file_path_dict=args.files, folder_id=args.container)


def update(args):
    taiga_client = init_taiga(args.token)
    taiga_client.update_dataset(dataset_id=args.dataset_id,
                                dataset_permaname=args.dataset_permaname,
                                dataset_version=args.dataset_version,
                                dataset_description=args.dataset_description,
                                upload_file_path_dict=args.files,
                                force_keep=args.force_keep,
                                force_remove=args.force_remove)


def get(args):
    taiga_client = init_taiga(args.token)
    local_file = taiga_client.download_to_cache(id=args.dataset_id,
                                                name=args.dataset_permaname,
                                                version=args.dataset_version,
                                                file=args.file,
                                                force=args.force,
                                                format=args.format)
    print("\nDownloaded the file into: {}".format(local_file))


def init_taiga(token_path):
    return TaigaClient(token_path=token_path)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description='Download or upload Taiga datafiles/datasets. Use -f to get the available formats.')

    subparser = parser.add_subparsers()

    # Create
    create_parser = subparser.add_parser("create", help="Create a new dataset")

    create_parser.add_argument('-n', '--name',
                               required=True, help="Name of the dataset to create")
    create_parser.add_argument('-d', '--description',
                               help="Description of the dataset to create")
    create_parser.add_argument('-f', '--files',
                               required=True, type=json.loads,
                               help="Json dictionary of files and formats. Example: '{\"path_file_one\": \"format_file_one\"}'")
    # TODO: Default should be the home of the user
    create_parser.add_argument('-c', '--container',
                               help='Folder which is going to contain the new dataset. Default is public')
    create_parser.set_defaults(func=create)

    # Update
    # dataset_id = None, dataset_permaname = None, dataset_version = None, dataset_description = None,
    # upload_file_path_dict = None, force_keep = False, force_remove = False)
    update_parser = subparser.add_parser("update", help="Update an existing dataset")
    # First group
    update_parser.add_argument('-i', '--dataset_id', help="Dataset ID to update")
    update_parser.add_argument('-p', '--dataset_permaname', help="Dataset permaname to update")
    update_parser.add_argument('-v', '--dataset_version', help="Dataset version to update")
    update_parser.add_argument('-d', '--dataset_description',
                               help="Change the description of the dataset (default is keeping it)")
    update_parser.add_argument('-f', '--files',
                               required=True, type=json.loads,
                               help="Json dictionary of files and formats. Example: '{\"path_file_one\": \"format_file_one\"}'")
    update_parser.add_argument('-k', '--force_keep', action='store_true', help="Keep all the files")
    update_parser.add_argument('-r', '--force_remove', action='store_true', help="Remove all the files")
    update_parser.set_defaults(func=update)

    # Get
    get_parser = subparser.add_parser("get", help="Get a dataset or datafiles")
    # id = None, name = None, version = None, file = None, force = False, encoding = None
    get_parser.add_argument('-i', '--dataset_id', help="Dataset ID to retrieve")
    get_parser.add_argument('-p', '--dataset_permaname', help="Dataset permaname to retrieve")
    get_parser.add_argument('-v', '--dataset_version', help="Dataset version to retrieve")
    # TODO: Give multiple files here?
    get_parser.add_argument('-f', '--file', help="Datafile name to retrieve")
    get_parser.add_argument('--force', action='store_true',
                            help="Force the conversion to happen again. Useful when a job seems stuck.")
    get_parser.add_argument('-e', '--encoding', help="Encoding to use to decypher the file")
    get_parser.add_argument('-t', '--format',
                            help="Format of the data you want. Enter one available in Taiga in 'Download' column",
                            required=True)
    get_parser.set_defaults(func=get)

    parser.add_argument('-f', '--format', action='store_true',
                        help='Print the available formats')
    parser.add_argument('-t', '--token',
                        help='Path to the user token')

    args = parser.parse_args()

    if hasattr(args, 'func'):
        args.func(args)

    format = args.format

    if format:
        print("Available formats are: \n\n- {}".format("\n- ".join(TaigaClient().formats)))
