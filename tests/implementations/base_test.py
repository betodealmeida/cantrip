import pytest
import sqlglot
from pytest_mock import MockerFixture

from cantrip.implementations.base import BaseSemanticLayer
from cantrip.models import Metric, Relation


@pytest.mark.parametrize(
    "sql, expected",
    [
        (
            """
SELECT SUM(quantity * unit_price * (1 - discount) + tax_amount) AS total_revenue
FROM fact_orders;
            """,
            Metric(
                "total_revenue",
                "SUM(quantity * unit_price * (1 - discount) + tax_amount)",
                Relation("fact_orders"),
                {Relation("fact_orders")},
            ),
        ),
        (
            """
SELECT COUNT(*) AS total_orders
FROM my_table t
JOIN (
  WITH cte AS (SELECT id FROM other_table)
  SELECT * FROM cte
) sub
ON t.id = sub.id;
            """,
            None,
        ),
    ],
)
def test_get_metric_from_view(
    mocker: MockerFixture,
    sql: str,
    expected: Metric,
) -> None:
    engine = mocker.MagicMock()
    semantic_layer = BaseSemanticLayer(engine)
    mocker.patch.object(semantic_layer, "get_views", return_value={})

    ast = sqlglot.parse_one(sql)
    assert semantic_layer.get_metric_from_view(ast) == expected
