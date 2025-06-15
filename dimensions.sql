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

SELECT table_name || '.' || column_name AS dimension
FROM dimensions
ORDER BY dimension;

