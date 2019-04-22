class TaigaHttpException(Exception):
    """Exception to retrieve a Bad Status code from Taiga"""


class Taiga404Exception(TaigaHttpException):
    """Exception to retrieve a NotFound returned by Taiga"""
    pass


class TaigaDeletedVersionException(TaigaHttpException):
    """Exception to retrieve a deleted dataset version"""
    pass


class TaigaRawTypeException(Exception):
    """Exception when we are trying to get a file from a Table or Matrix format, whereas it is Raw data"""
    pass
