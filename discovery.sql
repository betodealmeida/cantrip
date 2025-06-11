WITH tables AS (
  SELECT name FROM sqlite_master WHERE type='table'
)
SELECT 
  t.name AS table_name,
  fk.id,
  fk.seq,
  fk."table" AS referenced_table,
  fk."from" AS fk_column,
  fk."to" AS referenced_column
FROM tables t
JOIN pragma_foreign_key_list(t.name) fk;
