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


class DBHosePermissionError(DBHoseError, PermissionError):
    """Permission denied error."""


class DBHoseNotFoundError(DBHoseError, FileNotFoundError):
    """Object not found error."""
