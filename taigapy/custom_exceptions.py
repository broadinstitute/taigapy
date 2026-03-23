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
    """Raised when the Taiga server returns an error (HTTP 5xx) or a background task fails."""

    def __init__(
        self,
        *,
        status_code: int = None,
        endpoint: str = None,
        params: dict = None,
        task_message: str = None,
        response_body: str = None,
    ):
        parts = ["Taiga server error."]
        if endpoint:
            parts.append(f"Endpoint: {endpoint}")
        if status_code:
            parts.append(f"HTTP status: {status_code}")
        if params:
            parts.append(f"Params: {params}")
        if task_message:
            parts.append(f"Task failure message: {task_message}")
        if response_body:
            parts.append(f"Response body: {response_body[:500]}")
        parts.append(
            "If this persists, please contact the Taiga maintainers."
        )
        super().__init__(" | ".join(parts))


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
