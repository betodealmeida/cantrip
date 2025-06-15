-- 1️⃣ Extract all foreign key relationships
WITH fk_relations AS (
  SELECT
    fk."table" AS referenced_table,
    fk."to" AS referenced_column
  FROM sqlite_master m
  JOIN pragma_foreign_key_list(m.name) fk
  WHERE m.type = 'table'
),

-- 2️⃣ Find all referenced tables
referenced_tables AS (
  SELECT DISTINCT referenced_table FROM fk_relations
),

-- 3️⃣ Extract all columns from all tables
table_columns AS (
  SELECT
    m.name AS table_name,
    p.name AS column_name
  FROM sqlite_master m
  JOIN pragma_table_info(m.name) p
  WHERE m.type = 'table'
),

-- 4️⃣ Define dimensions: columns in referenced tables that are not FK targets
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
),

-- 5️⃣ Build view-to-table dependencies (direct only)
direct_dependencies AS (
  SELECT
    v.name AS view_name,
    t.name AS referenced_object
  FROM sqlite_master v
  JOIN sqlite_master t
  ON v.sql LIKE '%' || t.name || '%'
  WHERE v.type = 'view' AND (t.type = 'table' OR t.type = 'view')
),

-- 6️⃣ Recursively resolve full dependencies (view-on-view)
full_dependencies AS (
  SELECT view_name, referenced_object FROM direct_dependencies

  UNION

  SELECT
    fd.view_name,
    dd.referenced_object
  FROM full_dependencies fd
  JOIN direct_dependencies dd
    ON fd.referenced_object = dd.view_name
),

-- 7️⃣ Finally, map metrics (views) to dimensions
related_metrics AS (
  SELECT
    d.table_name || '.' || d.column_name AS dimension,
    fd.view_name AS metric
  FROM dimensions d
  JOIN full_dependencies fd
    ON fd.referenced_object = d.table_name
)

SELECT * FROM related_metrics
ORDER BY dimension, metric;

