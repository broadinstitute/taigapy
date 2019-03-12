class TaigaHttpException(Exception):
    """Exception to retrieve a Bad Status code from Taiga"""


class Taiga404Exception(TaigaHttpException):
    """Exception to retrieve a NotFound returned by Taiga"""
    pass


class TaigaDeletedVersionException(TaigaHttpException):
    pass
