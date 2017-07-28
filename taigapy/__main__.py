import argparse
import json

from taigapy import TaigaClient


def create(args):
    taiga_client = init_taiga(args.token)
    taiga_client.create_dataset(dataset_name=args.name,
                   dataset_description=args.description,
                   upload_file_path_dict=args.files, folder_id=args.container)

def update(args):
    print('update')


def get(args):
    print('get')


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
    update_parser = subparser.add_parser("update", help="Update an existing dataset")
    update_parser.set_defaults(func=update)

    # Get
    get_parser = subparser.add_parser("get", help="Get a dataset or datafiles")
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


