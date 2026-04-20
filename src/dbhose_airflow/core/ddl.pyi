from native_dumper import HTTPCursor
from psycopg import Cursor

from .structs import (
    ETLInfo,
    TableMetadata,
)

def generate_ddl(
    table_name: str,
    cursor: Cursor | HTTPCursor,
    staging_random_suffix: bool = True,
    skip_staging: bool = False,
) -> ETLInfo | TableMetadata:
    """Generate ETLInfo or TableMetadata object."""

    ...
