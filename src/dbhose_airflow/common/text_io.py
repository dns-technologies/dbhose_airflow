from pathlib import Path

from ..core import (
    DQCheck,
    Error,
    MoveMethod,
)


__root_path = Path(__file__).parent.absolute()
__logo_path = __root_path / "renders" / "LOGO"
__query_path = __root_path / "queries" / "{}" / "{}" / "{}.sql"
__query_type = {DQCheck: "dq", MoveMethod: "move"}


def __read_text(path: str | Path) -> str:
    """Read from text file."""

    with open(path, encoding="utf-8") as file:
        return file.read()


def define_query(dbname: str, kind: DQCheck | MoveMethod) -> str:
    """Define specify query."""

    try:
        query_type = __query_type[type(kind)]
        path = str(__query_path).format(query_type, dbname, kind.name)
        return __read_text(path)
    except (KeyError, ValueError, TypeError) as error:
        raise Error.DBHoseValueError(error)
    except FileNotFoundError as error:
        raise Error.DBHoseNotFoundError(error)
    except PermissionError as error:
        raise Error.DBHosePermissionError(error)


def logo() -> str:
    """Render Logo."""

    return __read_text(__logo_path)
