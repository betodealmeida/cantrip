from typing import Any, Iterator

import sqlglot
from sqlalchemy import text
from sqlalchemy.engine import Engine
from sqlglot import exp
from sqlglot.dialects.dialect import Dialect
from sqlglot.optimizer.scope import traverse_scope

from cantrip.models import (
    Dimension,
    Filter,
    Metric,
    Query,
    Relation,
    SemanticView,
    Sort,
)


class BaseSemanticLayer:
    """
    Base semantic layer.

    This class provides a reference implementation of a semantic layer. In this
    implementation, metrics are defined as `VIEW`s with a single aggregated expression,
    while dimensions are inferred from the database schema and foreign keys.

    The implementation is based on `sqlot`, and should be extended for different
    databases by overriding the methods that interact with the database (eg, fetching the
    list of `VIEW`s).
    """

    dialect: Dialect | None = None

    def __init__(self, engine: Engine) -> None:
        """
        Initialize the semantic layer with DB engine.
        """
        self.engine = engine
        self.default_schema = self.get_default_schema()
        self.default_catalog = self.get_default_catalog()

    def get_default_schema(self) -> str | None:
        return None

    def get_default_catalog(self) -> str | None:
        return None

    def execute(
        self,
        sql: str,
        kwargs: Any,
    ) -> Iterator[dict[str, Any]]:
        """
        Execute a SQL query and return the results.
        """
        with self.engine.connect() as connection:
            for row in connection.execute(text(sql), **kwargs):
                yield dict(row)

    def get_semantic_views(self) -> set[SemanticView]:
        raise NotImplementedError()

    def get_metrics(self, semantic_view: SemanticView) -> set[Metric]:
        metrics: set[Metric] = set()

        for ast in self.get_views().values():
            if metric := self.get_metric_from_view(ast):
                metrics.add(metric)

        return metrics

    def get_dimensions(self, semantic_view: SemanticView) -> set[Dimension]:
        raise NotImplementedError()

    def get_valid_metrics(
        self,
        semantic_view: SemanticView,
        metrics: set[Metric],
        dimensions: set[Dimension],
    ) -> set[Metric]:
        parents_sets = {frozenset(metric.parents) for metric in metrics}
        if len(parents_sets) > 1:
            raise ValueError("All metrics must have the same parents to be valid.")

        valid_parents = parents_sets.pop()
        candidates = {
            metric
            for metric in self.get_metrics(semantic_view)
            if metric.parents == valid_parents
        }

        dimensions_per_relation = self.get_dimensions_per_relation(semantic_view)
        valid_metrics = {
            metric
            for metric in candidates
            if all(
                dimensions <= dimensions_per_relation.get(table, set())
                for table in metric.tables
            )
        }

        return valid_metrics

    def get_valid_dimensions(
        self,
        semantic_view: SemanticView,
        metrics: set[Metric],
        dimensions: set[Dimension],
    ) -> set[Dimension]:
        dimensions_per_relation = self.get_dimensions_per_relation(semantic_view)

        return {
            dimension
            for metric in metrics
            for table in metric.tables
            for dimension in dimensions_per_relation.get(table, set())
        }

    def get_query(
        self,
        semantic_view: SemanticView,
        metrics: set[Metric],
        dimensions: set[Dimension],
        filters: set[Filter],
        sort: Sort,
        limit: int | None = None,
        offset: int | None = None,
    ) -> Query:
        raise NotImplementedError()

    def get_query_from_standard_sql(
        self,
        semantic_view: SemanticView,
        sql: str,
    ) -> Query:
        raise NotImplementedError()

    def get_views(self) -> dict[Relation, exp.Select]:
        """
        Return a map of view names to their parsed SQL expressions.
        """
        raise NotImplementedError()

    def get_dimensions_per_relation(
        self,
        semantic_view: SemanticView,
    ) -> dict[Relation, set[Dimension]]:
        """
        Return a map of tables and their joinable dimensions.
        """
        raise NotImplementedError()

    def get_relations(self, sql: exp.Select) -> set[Relation]:
        return {
            Relation(
                source.name,
                source.db if source.db != "" else self.default_schema,
                source.catalog if source.catalog != "" else self.default_catalog,
            )
            for scope in traverse_scope(sql)
            for source in scope.sources.values()
            if isinstance(source, exp.Table)
        }

    def get_dependencies(self, sql: exp.Select, recurse: bool = False) -> set[Relation]:
        """
        Get the dependencies of a SQL expression.

        This is used to find the relations that a metric depends on, as well as the actual
        tables when views are traversed.
        """
        views = self.get_views() if recurse else {}

        parents: set[Relation] = set()
        for relation in self.get_relations(sql):
            if recurse and relation in views:
                parents.update(self.get_relations(views[relation]))
            else:
                parents.add(relation)

        return parents

    def get_metric_from_view(self, ast: exp.Select) -> Metric | None:
        """
        Get a metric from a view, if it exists.
        """
        return (
            Metric(
                name=ast.expressions[0].alias_or_name,
                expression=ast.expressions[0].this.sql(),
                parents=self.get_dependencies(ast, recurse=False),
                tables=self.get_dependencies(ast, recurse=True),
            )
            if (
                len(ast.expressions) == 1
                and ast.expressions[0].find(exp.AggFunc)
                and "joins" not in ast.args
            )
            else None
        )

    def quote(self, identifier: str) -> str:
        """
        Quote an identifier for the database dialect.
        """
        expression = sqlglot.exp.Identifier(this=identifier)
        quoted = self.dialect.quote_identifier(expression, identify=False)
        return quoted.sql()
