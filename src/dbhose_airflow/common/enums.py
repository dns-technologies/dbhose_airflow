from enum import Enum
from typing import NamedTuple


class DQTest(NamedTuple):
    """Data quality test."""

    description: str
    generate_queryes: int
    need_source_table: int


class DQCheck(DQTest, Enum):
    """Enum for avaliable tests."""

    empty = DQTest("Table not empty", 0, 0)
    uniq = DQTest("Table don't have any duplicate rows", 0, 0)
    future = DQTest("Table don't have dates from future", 1, 0)
    infinity = DQTest("Table don't have infinity values", 1, 0)
    nan = DQTest("Table don't have NaN values", 1, 0)
    total = DQTest("Equal data total rows count between objects", 0, 1)
    sum = DQTest("Equal data sums in digits columns between objects", 1, 1)


class MoveType(NamedTuple):
    """Move method object."""

    name: str
    have_sql: bool
    need_filter: bool
    is_custom: bool


class MoveMethod(MoveType, Enum):
    """Insert from temp table methods."""

    append = MoveType("append", False, False, False)
    custom = MoveType("custom", False, False, True)
    delete = MoveType("delete", True, True, False)
    replace = MoveType("replace", True, False, False)
    rewrite = MoveType("rewrite", False, False, False)
