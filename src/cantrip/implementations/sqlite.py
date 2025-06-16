from collections import defaultdict

import sqlglot
from sqlglot import exp
from sqlglot.dialects.sqlite import SQLite

from cantrip.implementations.base import BaseSemanticLayer
from cantrip.models import (
    Dimension,
    Relation,
    SemanticView,
)


class SQLiteSemanticLayer(BaseSemanticLayer):

    dialect = SQLite()

    def get_semantic_views(self) -> set[SemanticView]:
        return {SemanticView("semantic_view")}

    def get_dimensions(self, semantic_view: SemanticView) -> set[Dimension]:
        sql = """
WITH fk_relations AS (
  SELECT
    fk."table" AS referenced_table,
    fk."to" AS referenced_column
  FROM sqlite_master m
  JOIN pragma_foreign_key_list(m.name) fk
  WHERE m.type = 'table'
),

referenced_tables AS (
  SELECT DISTINCT referenced_table FROM fk_relations
),

table_columns AS (
  SELECT
    m.name AS table_name,
    p.name AS column_name
  FROM sqlite_master m
  JOIN pragma_table_info(m.name) p
  WHERE m.type = 'table'
),

dimensions AS (
  SELECT
    tc.table_name,
    tc.column_name
  FROM table_columns tc
  JOIN referenced_tables rt ON tc.table_name = rt.referenced_table
  LEFT JOIN fk_relations fk
    ON tc.table_name = fk.referenced_table
    AND tc.column_name = fk.referenced_column
  WHERE fk.referenced_column IS NULL
)

SELECT table_name, column_name
FROM dimensions;
        """

        dimensions: set[Dimension] = set()

        for row in self.execute(sql):
            table = self.quote(row["table_name"])
            column = self.quote(row["column_name"])
            name = f"{table}.{column}"
            dimensions.add(Dimension(name))

        return dimensions

    def get_dimensions_per_relation(
        self,
        semantic_view: SemanticView,
    ) -> dict[Relation, set[Dimension]]:
        sql = """
WITH fk_relations AS (
  SELECT
    m.name AS fact_table,
    fk."table" AS dimension_table,
    fk."from" AS fk_column,
    fk."to" AS dimension_column
  FROM sqlite_master m
  JOIN pragma_foreign_key_list(m.name) fk
  WHERE m.type = 'table'
),

dimension_columns AS (
  SELECT
    m.name AS dimension_table,
    p.name AS column_name
  FROM sqlite_master m
  JOIN pragma_table_info(m.name) p
  WHERE m.type = 'table'
),

fk_and_columns AS (
  SELECT
    fk.fact_table,
    fk.dimension_table,
    dc.column_name
  FROM fk_relations fk
  JOIN dimension_columns dc
    ON fk.dimension_table = dc.dimension_table
),

referenced_columns AS (
  SELECT DISTINCT
    fact_table,
    dimension_table,
    dimension_column AS column_name
  FROM fk_relations
),

filtered_columns AS (
  SELECT
    fkc.fact_table,
    fkc.dimension_table,
    fkc.column_name
  FROM fk_and_columns fkc
  LEFT JOIN referenced_columns rc
    ON fkc.fact_table = rc.fact_table
    AND fkc.dimension_table = rc.dimension_table
    AND fkc.column_name = rc.column_name
  WHERE rc.column_name IS NULL
)

SELECT
  fact_table,
  dimension_table,
  column_name
FROM filtered_columns;
        """

        dimensions: dict[Relation, set[Dimension]] = defaultdict(set)

        for row in self.execute(sql):
            relation = Relation(
                row["fact_table"],
                self.default_schema,
                self.default_catalog,
            )
            table = self.quote(row["dimension_table"])
            column = self.quote(row["column_name"])
            name = f"{table}.{column}"
            dimensions[relation].add(Dimension(name))

        return dimensions

    def get_default_schema(self) -> str:
        return "main"

    def get_default_catalog(self) -> None:
        return None

    def get_views(self) -> dict[Relation, exp.Select]:
        views: dict[str, exp.Select] = {}

        for name, sql in self.execute(
            "SELECT name, sql FROM sqlite_master WHERE type='view'"
        ):
            relation = Relation(name, self.default_schema, self.default_catalog)
            ast = sqlglot.parse_one(sql, self.dialect)
            if isinstance(ast, exp.Create) and isinstance(ast.expression, exp.Select):
                views[relation] = ast.expression

        return views
