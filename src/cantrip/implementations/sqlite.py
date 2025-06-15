import sqlglot
from sqlglot import exp
from sqlglot.dialects.sqlite import SQLite

from cantrip.implementations.base import BaseSemanticLayer
from cantrip.models import Metric, Dimension, Filter, Relation, Sort, Query


class SQLiteSemanticLayer(BaseSemanticLayer):

    dialect = SQLite()

    def get_views(self) -> dict[Relation, exp.Select]:
        views: dict[str, exp.Select] = {}

        for name, sql in self.execute(
            "SELECT name, sql FROM sqlite_master WHERE type='view'"
        ):
            ast = sqlglot.parse_one(sql, self.dialect)
            if isinstance(ast, exp.Create) and isinstance(ast.expression, exp.Select):
                views[Relation(name, "main")] = ast.expression

        return views

    def get_metrics(self) -> set[Metric]:
        metrics: set[Metric] = set()

        for ast in self.get_views().values():
            if metric := self.get_metric_from_view(ast):
                metrics.add(metric)

        return metrics

    def get_dimensions(self) -> set[Dimension]:
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

    def get_metrics_for_dimensions(self, dimensions: set[Dimension]) -> set[Metric]: ...

    def get_dimensions_for_metrics(self, metrics: set[Metric]) -> set[Dimension]: ...

    def get_query(
        self,
        metrics: set[Metric],
        dimensions: set[Dimension],
        filters: set[Filter],
        sort: Sort,
        limit: int | None = None,
        offset: int | None = None,
    ) -> Query: ...

    def get_query_from_standard_sql(self, sql: str) -> Query: ...
