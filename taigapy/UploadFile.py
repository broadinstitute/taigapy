import enum
import os


class UploadFile:
    class InitialFileType(enum.Enum):
        # This class mirrors the one in taiga/taiga2/models.py
        NumericMatrixCSV = "NumericMatrixCSV"
        NumericMatrixTSV = "NumericMatrixTSV"
        TableCSV = "TableCSV"
        TableTSV = "TableTSV"
        GCT = "GCT"
        Raw = "Raw"

    def __init__(self, prefix, file_path, format):
        """Constructor of UploadFile
        :param file_path: str
        :param format: str => Matches a InitialFileType enum
        """
        def drop_extension(file_path_with_extension):
            filename, file_extension = os.path.splitext(file_path_with_extension)
            return filename

        def get_file_name(file_path_without_extension):
            file_name = os.path.basename(os.path.normpath(file_path_without_extension))
            return file_name

        self.file_name = get_file_name(drop_extension(file_path))
        self.prefix_and_file_name = prefix + self.file_name
        self.format = self.InitialFileType(format)

