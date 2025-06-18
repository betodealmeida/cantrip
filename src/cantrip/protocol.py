from typing import Protocol

from cantrip.models import Metric, Dimension, Filter, SemanticView, Sort, Query


class SemanticLayer(Protocol):
    """
    A generic protocol for semantic layers.
    """

    def get_semantic_views(self) -> set[SemanticView]:
        """
        Returns a set of all semantic views available in the semantic layer.
        """
        ...

    def get_metrics(self, semantic_view: SemanticView) -> set[Metric]:
        """
        Returns a set of all available metrics.
        """
        ...

    def get_dimensions(self, semantic_view: SemanticView) -> set[Dimension]:
        """
        Returns a set of all available dimensions.
        """
        ...

    def get_valid_metrics(
        self,
        semantic_view: SemanticView,
        metrics: set[Metric],
        dimensions: set[Dimension],
    ) -> set[Metric]:
        """
        Return compatible metrics for the given metrics and dimensions.

        For metrics to be valid they must be compatible with all the provided dimensions.
        """
        ...

    def get_valid_dimensions(
        self,
        semantic_view: SemanticView,
        metrics: set[Metric],
        dimensions: set[Dimension],
    ) -> set[Dimension]:
        """
        Return compatible dimensions for the given metrics.

        For dimensions to be valid they must be compatible with all the provided metrics.
        """
        ...

    def get_query(
        self,
        semantic_view: SemanticView,
        metrics: set[Metric],
        dimensions: set[Dimension],
        # populations: set[Population],
        filters: set[Filter],
        sort: Sort,
        limit: int | None = None,
        offset: int | None = None,
    ) -> Query:
        """
        Build a SQL query from the given metrics, dimensions, filters, and sort order.
        """
        ...

    def get_query_from_standard_sql(
        self,
        semantic_view: SemanticView,
        sql: str,
    ) -> Query:
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
