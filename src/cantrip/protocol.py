from typing import Protocol

from sqlalchemy.engine import Engine

from cantrip.models import Metric, Dimension, Filter, SemanticView, Sort, Query


class SemanticLayer(Protocol):

    def __init__(self, view: SemanticView, engine: Engine): ...

    def get_semantic_views(self) -> set[SemanticView]:
        """
        Returns a set of all semantic views available in the semantic layer.
        """
        ...

    def get_metrics(self) -> set[Metric]:
        """
        Returns a set of all available metrics.
        """
        ...

    def get_dimensions(self) -> set[Dimension]:
        """
        Returns a set of all available dimensions.
        """
        ...

    def get_metrics_for_dimensions(self, dimensions: set[Dimension]) -> set[Metric]:
        """
        Return compatible metrics for the given dimensions.
        """
        ...

    def get_dimensions_for_metrics(self, metrics: set[Metric]) -> set[Dimension]:
        """
        Return compatible dimensions for the given metrics.
        """
        ...

    def get_query(
        self,
        metrics: set[Metric],
        dimensions: set[Dimension],
        filters: set[Filter],
        # populations: set[Population],
        sort: Sort,
        limit: int | None = None,
        offset: int | None = None,
    ) -> Query:
        """
        Build a SQL query from the given metrics, dimensions, filters, and sort order.
        """
        ...

    def get_query_from_standard_sql(self, sql: str) -> Query:
        """
        Build a SQL query from a pseudo-query referencing metrics and dimensions.

        For example, given `metric1` having the expression `COUNT(*)`, this query:

            SELECT metric1, dim1
            FROM semantic_layer
            GROUP BY dim1

        Becomes:

            SELECT COUNT(*) AS metric1, dim1
            FROM fact_table
            JOIN dim_table
            ON fact_table.dim_id = dim_table.id
            GROUP BY dim1

        """
        ...
