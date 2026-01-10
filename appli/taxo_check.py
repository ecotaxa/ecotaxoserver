# -*- coding: utf-8 -*-
import os
import pickle
import urllib3
import json
import time
import re


CACHE_FILE = "worms_cache.pkl"
worms_cache = {}

if os.path.exists(CACHE_FILE):
    with open(CACHE_FILE, "rb") as f:
        worms_cache = pickle.load(f)

http = urllib3.PoolManager()
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

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
SELECT id, lineage FROM taxonomy_lineage ORDER BY id;
"""

SQL_APHIA_AND_ECO_ID = """
select id, aphia_id from taxonomy_worms txp
where aphia_id is not null
  -- and not exists(select 1 from taxonomy_worms txc
  --                where txc.parent_id = txp.id and txc.aphia_id is not null)
order by id
"""


def to_lineage(dct: dict):
    """{'AphiaID': 1, 'rank': 'Superdomain', 'scientificname': 'Biota', 'child': {'AphiaID': 2, 'rank': 'Kingdom', 'scientificname': 'Animalia', 'child': {'AphiaID': 799, 'rank': 'Phylum', 'scientificname': 'Nematoda', 'child': {'AphiaID': 834422, 'rank': 'Class', 'scientificname': 'Enoplea', 'child': {'AphiaID': 2135, 'rank': 'Subclass', 'scientificname': 'Enoplia', 'child': {'AphiaID': 2141, 'rank': 'Order', 'scientificname': 'Enoplida', 'child': {'AphiaID': 834425, 'rank': 'Suborder', 'scientificname': 'Oncholaimina', 'child': {'AphiaID': 2159, 'rank': 'Superfamily', 'scientificname': 'Oncholaimoidea', 'child': {'AphiaID': 2204, 'rank': 'Family', 'scientificname': 'Oncholaimidae', 'child': {'AphiaID': 2269, 'rank': 'Subfamily', 'scientificname': 'Oncholaimellinae', 'child': {'AphiaID': 2570, 'rank': 'Genus', 'scientificname': 'Viscosia', 'child': None}}}}}}}}}}}"""
    name, aphia_id = dct["scientificname"], dct["AphiaID"]
    ret = f"{name}({aphia_id})"
    if dct["child"] is not None:
        ret += " > " + to_lineage(dct["child"])
    return ret


def list_taxonomy_lineage(db, filename):
    cnx = db.engine.raw_connection()
    with cnx.cursor() as cur:
        cur.execute(SQL_LIST_LINEAGE)
        res = cur.fetchall()
    rows = 0
    with open(filename, "w") as f:
        ko = 0
        checked = 0
        failed_aphia_ids = set()
        for row in res:
            cat_id, db_worms_lineage = row
            # print(f"{cat_id:6} {db_worms_lineage}", file=f)
            # set aphia_id to last parenthised number in db_worms_lineage
            # e.g. Cyanophyceae(146542) > Nodosilineales(1653516) > Cymatolegaceae(1653542)
            # returns 1653542
            aphia_id = None
            matches = re.findall(r"\((\d+)\)", db_worms_lineage)
            if matches:
                aphia_id = int(matches[-1])
            if aphia_id in worms_cache:
                checked += 1
                worms = to_lineage(worms_cache[aphia_id])
                if worms != db_worms_lineage:
                    seen = False
                    for a_failed in failed_aphia_ids:
                        if f"({a_failed})" in worms:
                            seen = True
                    if not seen:
                        print(f"DB: {cat_id:6} {db_worms_lineage}", file=f)
                        print(f"GET:       {worms}", file=f)
                        print(f"!== ", file=f)
                        failed_aphia_ids.add(aphia_id)
                    ko += 1
            rows += 1
        print("Total rows: ", rows, file=f)
        print(f"Total rows {rows} checked {checked} KO {ko}, see {filename}")


URL = "https://www.marinespecies.org/rest/AphiaClassificationByAphiaID/{aphia_id}"


def fetch_worms(db):
    cnx = db.engine.raw_connection()
    with cnx.cursor() as cur:
        cur.execute(SQL_APHIA_AND_ECO_ID)
        res = cur.fetchall()

    get_count = 0
    for row in res:
        eco_id, aphia_id = row
        if aphia_id in worms_cache:
            continue
        print("Fetching worms for", aphia_id)
        url = URL.format(aphia_id=aphia_id)
        try:
            r = http.request("GET", url)
            if r.status == 200:
                worms_cache[aphia_id] = json.loads(r.data.decode("utf-8"))
                time.sleep(0.2)
                get_count += 1
                if get_count % 10 == 0:
                    with open(CACHE_FILE, "wb") as f:
                        pickle.dump(worms_cache, f)
                    print(f"Saved cache at {get_count} GETs")
            else:
                print(f"Failed to fetch {url}: {r.status}")
        except Exception as e:
            print(f"Error fetching {url}: {e}")

    with open(CACHE_FILE, "wb") as f:
        pickle.dump(worms_cache, f)
    print("Final cache save completed.")


RULES = [
    (
        """select * from taxonomy_worms where aphia_id is not null and rank is null""",
        "consistent aphia",
    ),
    ("""select * from taxonomy_worms where rank = 'None'""", "bad rank"),
    ("""select txc.id, txc.name, txp.id as parent_id, txp.name as parent_name, txp.aphia_id as parent_aphia from taxonomy_worms txc
  join taxonomy_worms txp on txp.id = txc.parent_id
 where txc.aphia_id is not null
   and txp.aphia_id is null""", "WoRMS child non-WoRMS parent"),
    ("""select txc.id, txc.name, txp.id as parent_id, txp.name as parent_name, txp.aphia_id as parent_aphia from taxonomy_worms txc
  join taxonomy_worms txp on txp.id = txc.parent_id
 where txc.aphia_id is null and txc.taxotype != 'M' and txc.taxostatus != 'D'
   and txp.aphia_id is not null
order by txc.id""", "bad dead branch start"),
   ("""select * from taxonomy_worms
 where taxostatus = 'D'
   and exists(select 1 from ecotaxainststat where id_instance in (1,8) and id_taxon=id )
  and rename_to is null""", "deprecated with objects but no replacement"),
]


def check_rules(db):
    cnx = db.engine.raw_connection()
    for a_rule, text in RULES:
        with cnx.cursor() as cur:
            cur.execute(a_rule)
            res = cur.fetchall()
        if res:
            print(f"Rule: {text} KO", res)


if __name__ == "__main__":
    from appli import db

    fetch_worms(db)
    list_taxonomy_lineage(db, "new_taxo.txt")
    check_rules(db)
