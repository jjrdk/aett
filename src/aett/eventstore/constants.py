import typing
from typing_extensions import Final

MAX_INT: Final[int] = 2 ** 31 - 1

T = typing.TypeVar('T')
SNAPSHOTS = 'snapshots'
COMMITS: str = 'commits'
