import enum
from dataclasses import dataclass


@dataclass(frozen=True)
class SemanticView:

    name: str
    description: str | None = None


@dataclass(frozen=True)
class Relation:

    name: str
    schema: str | None = None
    catalog: str | None = None


@dataclass(frozen=True)
class Metric:

    name: str
    sql: str
    tables: set[Relation]


class Grain:
    pass


@dataclass(frozen=True)
class Dimension:

    table: Relation
    column: str
    name: str
    grains: set[Grain]
    grain: Grain | None = None


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

    fields: list[Metric | Dimension]
    direction: SortDirectionEnum


@dataclass(frozen=True)
class Query:

    sql: str
