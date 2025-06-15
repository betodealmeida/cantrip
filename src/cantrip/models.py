import enum
from dataclasses import dataclass


@dataclass(frozen=True)
class SemanticView:

    name: str


@dataclass(frozen=True)
class Relation:

    name: str
    schema: str | None = None
    catalog: str | None = None


@dataclass(frozen=True)
class Metric:

    name: str
    expression: str
    parents: set[Relation]
    tables: set[Relation]


@dataclass(frozen=True)
class Dimension:

    name: str


class FilterTypeEnum(enum.Enum):

    WHERE = enum.auto()
    HAVING = enum.auto()


@dataclass(frozen=True)
class Filter:

    type: FilterTypeEnum
    expression: str


class SortDirectionEnum(enum.Enum):

    ASC = enum.auto()
    DESC = enum.auto()


@dataclass(frozen=True)
class Sort:

    fields: list[str]
    direction: SortDirectionEnum


@dataclass(frozen=True)
class Query:

    sql: str
