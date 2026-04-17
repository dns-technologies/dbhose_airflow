from base_dumper import (
    BaseDumperError,
    BaseDumperTypeError,
    BaseDumperValueError,
)


class DBHoseAirflowError(BaseDumperError): ...
class DBHoseAirflowTypeError(DBHoseAirflowError, BaseDumperTypeError): ...
class DBHoseAirflowValueError(DBHoseAirflowError, BaseDumperValueError): ...
