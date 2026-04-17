from pathlib import Path


__root_path = Path(__file__).parent.absolute()
__logo_path = __root_path / "renders" / "LOGO"
__query_path = __root_path / "queries" / "{}" / "{}" / "{}.sql"


def __read_text(path: str | Path) -> str:
    """Read from text file."""

    with open(path, encoding="utf-8") as file:
        return file.read()


def define_query(query_type: str, dbname: str, kind: str) -> str:
    """Define specify query."""

    return __read_text(str(__query_path).format(query_type, dbname, kind))


def logo() -> str:
    """Render Logo."""

    return __read_text(__logo_path)
