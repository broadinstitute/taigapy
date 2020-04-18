from typing import Iterable


class TaigaHttpException(Exception):
    """Exception to retrieve a Bad Status code from Taiga"""


class Taiga404Exception(TaigaHttpException):
    """Exception to retrieve a NotFound returned by Taiga"""

    pass


class TaigaDeletedVersionException(TaigaHttpException):
    """Exception to retrieve a deleted dataset version"""

    pass


class TaigaServerError(TaigaHttpException):
    """500 errors"""

    def __init__(self):
        super().__init__(
            "Something went wrong behind the scenes. Please try again later, or contact the maintainers of Taiga if the problem persists."
        )


class TaigaRawTypeException(Exception):
    """Exception when we are trying to get a file from a Table or Matrix format, whereas it is Raw data"""

    pass


class TaigaClientConnectionException(Exception):
    """Exception when we are unable to connect to Taiga"""

    pass


class TaigaTokenFileNotFound(Exception):
    def __init__(self, file_paths_checked: Iterable[str]):
        super().__init__(
            "No token file found. Checked the following locations: {}".format(
                file_paths_checked
            )
        )


class TaigaCacheFileCorrupted(Exception):
    def __init__(self):
        super().__init__("Local file is corrupted. Deleting file from cache.")
