from sqlglot import parse_one, exp
from sqlglot.optimizer.qualify_columns import qualify_columns
from sqlglot.optimizer.scope import traverse_scope

unqualified = """
WITH total_sales AS (
    SELECT SUM(amount) AS total_amount
    FROM sales
)
SELECT total_amount
FROM total_sales;
"""

unqualified = """
    SELECT SUM(amount)+1,a FROM sales
    """


schema = {
    "sales": {
        "amount": "float",
        "a": "int",
    },
}
ast = parse_one(unqualified)
ast = qualify_columns(ast, schema=schema)

for scope in traverse_scope(ast):
    if select := scope.find(exp.Select):
        if len(select.expressions) == 1 and select.expressions[0].find(exp.AggFunc):
            print("FOUND!")
