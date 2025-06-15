from typing import Any, Iterator

import sqlglot
from sqlalchemy import text
from sqlalchemy.engine import Engine
from sqlglot import exp
from sqlglot.optimizer.scope import traverse_scope

from cantrip.models import Metric, Relation


class BaseSemanticLayer:
    """
    Base semantic layer.

    This has generic SQL manipulation that should work in any database.
    """

    dialect: sqlglot.dialects.dialect.Dialect | None = None

    def __init__(self, engine: Engine) -> None:
        """
        Initialize the semantic layer with DB engine.
        """
        self.engine = engine

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

    def get_views(self) -> dict[Relation, exp.Select]:
        """
        Return a map of view names to their SQL expressions.
        """
        raise NotImplementedError()

    def get_relations(self, sql: exp.Select) -> set[Relation]:
        return {
            Relation(
                source.name,
                source.db if source.db != "" else None,
                source.catalog if source.catalog != "" else None,
            )
            for scope in traverse_scope(sql)
            for source in scope.sources.values()
            if isinstance(source, exp.Table)
        }

    def get_dependencies(self, sql: exp.Select, recurse: bool = False) -> set[Relation]:
        """
        Get the dependencies of a SQL expression.

        This is used to find the relations and tables that a metric depends on.
        """
        views = self.get_views() if recurse else {}

        parents: set[Relation] = set()
        for relation in self.get_relations(sql):
            if relation in views:
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
            if (len(ast.expressions) == 1 and ast.expressions[0].find(exp.AggFunc))
            else None
        )

    def quote(self, identifier: str) -> str:
        """
        Quote an identifier for the database dialect.
        """
        expression = sqlglot.exp.Identifier(this=identifier)
        quoted = self.dialect.quote_identifier(expression, identify=False)
        return quoted.sql()
