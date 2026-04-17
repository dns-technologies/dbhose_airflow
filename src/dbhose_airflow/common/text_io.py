from pathlib import Path


root_path = Path(__file__).parent.absolute()
logo_path = root_path / "renders" / "LOGO"
query_path = root_path / "queries" / "{}" / "{}" / "{}.sql"


def __read_text(path: str | Path) -> str:
    """Read from text file."""

    with open(path, encoding="utf-8") as file:
        return file.read()


def define_query(query_type: str, dbname: str, kind: str) -> str:
    """Define specify query."""

    return __read_text(str(query_path).format(query_type, dbname, kind))


def logo() -> str:
    """Render Logo."""

    return __read_text(logo_path)
