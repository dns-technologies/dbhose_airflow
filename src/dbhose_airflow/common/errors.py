from base_dumper import (
    BaseDumperError,
    BaseDumperTypeError,
    BaseDumperValueError,
)


class DBHoseError(BaseDumperError):
    """Base DBHose error."""


class DBHoseTypeError(DBHoseError, BaseDumperTypeError):
    """Type error."""


class DBHoseValueError(DBHoseError, BaseDumperValueError):
    """Value error."""


class DBHoseErrorPermissionError(DBHoseError, PermissionError):
    """Permission denied error."""


class DBHoseErrorNotFoundError(DBHoseError, FileNotFoundError):
    """Object not found error."""
