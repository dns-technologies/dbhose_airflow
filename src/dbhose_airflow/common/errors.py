from base_dumper import (
    BaseDumperError,
    BaseDumperTypeError,
    BaseDumperValueError,
)


class DBHoseAirflowError(BaseDumperError):
    """Base DBHose error."""


class DBHoseAirflowTypeError(DBHoseAirflowError, BaseDumperTypeError):
    """Type error."""


class DBHoseAirflowValueError(DBHoseAirflowError, BaseDumperValueError):
    """Value error."""


class DBHoseAirflowErrorPermissionError(DBHoseAirflowError, PermissionError):
    """Permission denied error."""


class DBHoseAirflowErrorNotFoundError(DBHoseAirflowError, FileNotFoundError):
    """Object not found error."""
