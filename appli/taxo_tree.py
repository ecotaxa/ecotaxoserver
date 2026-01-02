# -*- coding: utf-8 -*-
from appli import database

SQL_LIST_LINEAGE = """
WITH RECURSIVE taxonomy_lineage AS (
    -- Roots
    SELECT 
        id, 
        parent_id, 
        name, 
        CAST(name AS TEXT) || '(' || COALESCE(aphia_id,0) || ')' AS lineage
    FROM taxonomy_worms
    WHERE parent_id IS NULL OR parent_id NOT IN (SELECT id FROM taxonomy_worms)
    UNION ALL
    -- Recursive step: join children with their parents
    SELECT 
        t.id, 
        t.parent_id, 
        t.name, 
        tl.lineage || ' > ' || t.name || '(' || COALESCE(t.aphia_id,0) || ')'
    FROM taxonomy_worms t
    INNER JOIN taxonomy_lineage tl ON t.parent_id = tl.id
)
SELECT id, lineage FROM taxonomy_lineage ORDER BY lineage;
"""


def list_taxonomy_lineage(db, filename):
    cnx = db.engine.raw_connection()
    with cnx.cursor() as cur:
        cur.execute(SQL_LIST_LINEAGE)
        res = cur.fetchall()
    rows = 0
    with open(filename, "w") as f:
        for row in res:
            print(row[0], row[1], file=f)
            rows += 1
        print("Total rows: ", rows, file=f)


if __name__ == "__main__":
    from appli import db

    list_taxonomy_lineage(db, "new_taxo.txt")
