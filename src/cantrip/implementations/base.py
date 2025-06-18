from collections import defaultdict
from typing import Any, cast, Iterator

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
    SortDirectionEnum,
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

    supports_filter_clause: bool = False
    supports_cte: bool = True

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
        dimensions_per_table = self.get_dimensions_per_table(semantic_view)

        valid = {
            metric
            for metric in self.get_metrics(semantic_view)
            if all(
                dimension in dimensions_per_table.get(table, set())
                for table in metric.tables
                for dimension in dimensions
            )
        }

        if invalid := metrics - valid:
            raise ValueError(
                "Some given metrics are not valid for the given dimensions: "
                ", ".join(metric.name for metric in invalid)
            )

        return valid

    def get_valid_dimensions(
        self,
        semantic_view: SemanticView,
        metrics: set[Metric],
        dimensions: set[Dimension],
    ) -> set[Dimension]:
        dimensions_per_table = self.get_dimensions_per_table(semantic_view)

        valid = {
            dimension
            for dimension in self.get_dimensions(semantic_view)
            if all(
                dimension in dimensions_per_table.get(table, set())
                for metric in metrics
                for table in metric.tables
            )
        }

        if invalid := dimensions - valid:
            raise ValueError(
                "Some given dimensions are not valid for the given metrics: "
                ", ".join(dimension.name for dimension in invalid)
            )

        return valid

    def get_query(
        self,
        semantic_view: SemanticView,
        metrics: set[Metric],
        dimensions: set[Dimension],
        filters: set[Filter],
        sort: Sort | None = None,
        limit: int | None = None,
        offset: int | None = None,
    ) -> Query:
        # TODO: validate metrics and dimensions

        contexts: dict[
            tuple[exp.From, list[exp.Join]],
            set[exp.Select],
        ] = defaultdict(set)

        # group metrics by context -- FROM/JOINs
        for metric in metrics:
            ast = sqlglot.parse_one(metric.sql)
            if not self.is_valid_metric(ast):
                raise ValueError(f"Invalid metric SQL: {metric.sql}")

            context = (ast.args["from"], ast.args.get("joins", []))
            contexts[context].add(cast(exp.Select, ast))

        # build queries for each context
        queries: list[exp.Select] = []
        for context, metrics in contexts.items():
            predicates = {ast.args["where"] for ast in metrics if "where" in ast.args}
            if len(predicates) <= 1:
                expressions = [ast.expressions[0] for ast in metrics if ast.expressions]
                where = predicates.pop() if predicates else None
            else:
                expressions = [
                    self.get_metric_as_expression(metric) for metric in metrics
                ]
                where = None

            query = exp.Select(
                **{
                    "expressions": expressions,
                    "from": context[0],
                    "joins": context[1],
                    "where": where,
                }
            )

            # select and group by dimensions
            group = query.args.setdefault("group", exp.Group())
            for dimension in dimensions:
                column = exp.Column(
                    this=exp.Identifier(
                        this=dimension.column,
                        table=dimension.table,
                    )
                )
                query.expressions.append(column)
                group.expressions.append(column)

            # and perform necessary joins
            fact_tables = {table for metric in metrics for table in metric.tables}

            queries.append(query)

        # combine context queries
        if len(queries) == 1:
            query = queries[0]
        elif self.supports_cte:
            expressions = [
                exp.Alias(
                    this=exp.Column(
                        this=exp.Identifier(this=metric.name),
                        table=exp.Identifier(this=f"context_{i}"),
                    ),
                    alias=exp.Identifier(this=metric.name),
                )
                for i, metric in enumerate(metrics)
            ]
            with_ = [
                exp.CTE(this=query, alias=f"context_{i}")
                for i, query in enumerate(queries)
            ]
            from_ = exp.From(this=exp.Table(this=exp.Identifier(this="context_0")))
            joins = [
                exp.Join(
                    this=exp.Table(
                        this=exp.Identifier(this=f"context_{i}", kind="CROSS")
                    )
                )
                for i in range(1, len(queries))
            ]
            query = exp.Select(
                **{
                    "expressions": expressions,
                    "from": from_,
                    "joins": joins,
                    "with": with_,
                }
            )
        else:
            # TODO: handle dimensions
            query = exp.Select(
                expressions=[
                    exp.Alias(
                        this=exp.Subquery(
                            this=query,
                            alias=exp.Identifier(this=metric.name),
                        )
                    )
                    for metric in metrics
                ]
            )

        # filters: set[Filter],

        if sort:
            order = [
                exp.Ordered(
                    this=exp.Column(this=exp.Identifier(this=field.name)),
                )
                for field in sort.fields
            ]
            order[-1].desc(sort.direction == SortDirectionEnum.DESC)
            query.args["order"] = exp.Order(expressions=order)

        if offset:
            query = query.offset(offset)
        if limit:
            query = query.limit(limit)

        return Query(sql=query.sql())

    def get_query_from_standard_sql(
        self,
        semantic_view: SemanticView,
        sql: str,
    ) -> Query:
        raise NotImplementedError()

    def get_metric_as_expression(self, metric: exp.Select) -> list[exp.Expression]:
        """
        Convert a metric query into an expression for a projection.
        """
        expression = metric.expressions[0]

        where = metric.args.get("where")
        if not where:
            return expression

        if self.supports_filter_clause:
            return exp.Filter(this=expression, expression=where)

        if expression == exp.Count:
            return exp.Sum(
                this=exp.Case(
                    expressions=[exp.When(this=where, expression=exp.One())],
                    else_=exp.Zero(),
                )
            )

        if expression == exp.Sum:
            return exp.Sum(
                this=exp.Case(
                    expressions=[exp.When(this=where, expression=expression.this)],
                    else_=exp.Zero(),
                )
            )

        if isinstance(expression, (exp.Max, exp.Min, exp.Avg)):
            return expression.__class__(
                this=exp.Case(
                    expressions=[exp.When(this=where, expression=expression.this)],
                    else_=exp.Null(),
                )
            )

        raise ValueError(f"Unsupported metric expression: {expression.sql()}")

    def get_views(self) -> dict[Relation, exp.Select]:
        """
        Return a map of view names to their parsed SQL expressions.
        """
        raise NotImplementedError()

    def get_dimensions_per_table(
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

    def get_tables(self, sql: exp.Select) -> set[Relation]:
        """
        Get the tables of a SQL expression.
        """
        views = self.get_views()

        tables: set[Relation] = set()
        for relation in self.get_relations(sql):
            if relation in views:
                tables.update(self.get_relations(views[relation]))
            else:
                tables.add(relation)

        return tables

    def is_valid_metric(self, sql: exp.Expression) -> bool:
        return (
            isinstance(sql, exp.Select)
            and len(sql.expressions) == 1
            and sql.expressions[0].find(exp.AggFunc)
        )

    def get_metric_from_view(self, ast: exp.Select) -> Metric | None:
        """
        Get a metric from a view, if it exists.
        """
        if not self.is_valid_metric(ast):
            return None

        return Metric(
            name=ast.expressions[0].alias_or_name,
            sql=ast.sql(),
            tables=self.get_tables(ast),
        )

    def quote(self, identifier: str) -> str:
        """
        Quote an identifier for the database dialect.
        """
        expression = sqlglot.exp.Identifier(this=identifier)
        quoted = self.dialect.quote_identifier(expression, identify=False)
        return quoted.sql()
