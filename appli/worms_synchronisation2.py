# -*- coding: utf-8 -*-
import csv
import datetime
import os
from typing import NamedTuple

import psycopg2.extras

from appli import app, db, ntcv

WORMS_TAXO_DDL = [
    """DROP TABLE if exists taxonomy_worms;""",
    """CREATE TABLE taxonomy_worms
       (
           id                  INTEGER PRIMARY KEY,
           aphia_id            INTEGER,
           parent_id           INTEGER,
           name                VARCHAR(100)     NOT NULL,
           taxotype            CHAR DEFAULT 'P' NOT NULL,
           display_name        VARCHAR(200),
           source_url          VARCHAR(200),
           source_desc         VARCHAR(1000),
           creator_email       VARCHAR(255),
           creation_datetime   TIMESTAMP,
           lastupdate_datetime TIMESTAMP,
           id_instance         INTEGER,
           taxostatus          CHAR DEFAULT 'A' NOT NULL,
           rename_to           INTEGER,
           rank                VARCHAR(24),
           nbrobj              INTEGER,
           nbrobjcum           INTEGER
       );""",
    """INSERT INTO taxonomy_worms(id, parent_id, name, taxotype, display_name, source_url, source_desc,
                                  creator_email, creation_datetime, lastupdate_datetime, id_instance, taxostatus,
                                  rename_to, nbrobj, nbrobjcum)
       select id,
              parent_id,
              name,
              taxotype,
              display_name,
              source_url,
              source_desc,
              creator_email,
              creation_datetime,
              lastupdate_datetime,
              id_instance,
              taxostatus,
              rename_to,
              nbrobj,
              nbrobjcum
       from taxonomy;""",
    """
              DROP SEQUENCE IF EXISTS public.seq_taxonomy_worms;"""
    """
        CREATE SEQUENCE public.seq_taxonomy_worms
        START WITH 1
        INCREMENT BY 1
        NO MINVALUE
        NO MAXVALUE
        CACHE 1
        OWNED BY taxonomy_worms.id;""",
    """ALTER TABLE public.seq_taxonomy_worms OWNER TO postgres;""",
    """CREATE UNIQUE INDEX is_taxo_worms_parent_name
        on public.taxonomy_worms (parent_id, name);""",
    """CREATE INDEX "is_taxo_worms_name_lower"
        on public.taxonomy_worms (lower(name::text));""",
    """CREATE INDEX "is_taxo_worms_parent"
        on public.taxonomy_worms (parent_id);""",
]

INPUT_CSV = "static/tableau_ecotaxa_worms_17122025.csv"
INPUT_CSV = "static/tableau_ecotaxa_worms_01122025.csv"

START_OF_WORMS = 100000

NA = "NA"


class CsvRow(NamedTuple):
    ecotaxa_id: int
    new_id_ecotaxa: str
    new_parent_id_ecotaxa: str
    aphia_id: str
    taxotype: str
    action: str
    details: str
    name_ecotaxa: str
    name_wrm: str
    rank: str


class WormsSynchronisation2(object):
    def __init__(self, filename):
        self.serverdb = db.engine.raw_connection()
        self.serverdb.autocommit = True
        self.filename = filename

    def exec_sql(self, sql, params=None, debug=False):
        with self.serverdb.cursor() as cur:
            try:
                cur.execute(sql, params)
            except psycopg2.Error as e:
                print("SQL problem:", e, sql)
                self.serverdb = db.engine.raw_connection()
                self.serverdb.autocommit = True
        return None

    def get_one(self, sql, params=None):
        with self.serverdb.cursor() as cur:
            cur.execute(sql, params)
            res = cur.fetchone()
        return res[0] if res else None

    def get_all(
        self,
        sql,
        params=None,
        debug=False,
        cursor_factory=psycopg2.extras.RealDictCursor,
    ):
        with self.serverdb.cursor(cursor_factory=cursor_factory) as cur:
            cur.execute(sql, params)
            res = cur.fetchall()
        cur.close()
        return res

    def do_worms_synchronisation(self):

        self.clone_taxo_table()
        errorfile = open("static/db_update/error.log", "w")
        actions = self.read_and_check_csv()
        verifs = {}
        i = 0
        index = 1
        for row in actions:
            i += 1
            computename = True
            dt = datetime.datetime.now(datetime.timezone.utc)

            if i > 0 and i == index * 50000:
                index += 1

            filesql = "static/db_update/" + row.action.replace(" ", "_") + ".sql"
            if os.path.exists(filesql):
                wr = "a"
            else:
                wr = "w"
            if row.aphia_id != NA and row.name_wrm != NA:
                obj = {}
                obj[str(row.aphia_id)] = (
                    row.aphia_id,
                    row.new_parent_id_ecotaxa,
                )
                verifs.update(obj)
            if row.rank == NA:
                rank = None
            else:
                rank = row.rank
            if row.name_ecotaxa != row.name_wrm and row.name_wrm != NA:
                newname = row.name_wrm.replace("'", "''")
            elif row.name_ecotaxa != NA:
                # No new name
                newname = row.name_ecotaxa.replace("'", "''")
            else:
                newname = NA + str(row.ecotaxa_id)
                if row.action == "Creer nouvelle categorie":
                    qry = "INSERT INTO taxonomy_worms(id, name,parent_id,rank,creation_datetime,lastupdate_datetime) VALUES(%s, %s, %s, %s, %s, %s);"
                    params = (
                        row.ecotaxa_id,
                        newname,
                        row.new_parent_id_ecotaxa,
                        rank,
                        dt,
                        dt,
                    )
                else:
                    errorfile.write(" no name - " + ", ".join(map(str, row)) + "\n")
                continue
            if (
                row.action == "Creer nouvelle categorie"
                or (row.action == "Rien" and int(row.ecotaxa_id) >= START_OF_WORMS)
                or row.action == "Creer nouvelle categorie && deprecier"
            ):
                if row.action != "Creer nouvelle categorie":
                    row = row._replace(action="Creer nouvelle categorie")

                params = {
                    "ecotaxa_id": row.ecotaxa_id,
                    "name": newname,
                    "new_parent_id_ecotaxa": row.new_parent_id_ecotaxa,
                    "rank": rank,
                    "dt": dt,
                }

                if row.action == "Creer nouvelle categorie && deprecier":
                    if row.new_id_ecotaxa != NA:
                        pluskeys = ", rename_to, taxostatus"
                        plusvalues = ", %(new_id_ecotaxa)s, 'D'"
                        params["new_id_ecotaxa"] = row.new_id_ecotaxa
                    else:
                        pluskeys = ", taxostatus"
                        plusvalues = ", 'D'"
                else:
                    pluskeys = ""
                    plusvalues = ""

                if row.aphia_id != NA:
                    params["aphia_id"] = row.aphia_id
                    qry = (
                        "INSERT INTO taxonomy_worms(id,aphia_id,name,parent_id,rank,creation_datetime,lastupdate_datetime {pluskeys}) "
                        "VALUES(%(ecotaxa_id)s,%(aphia_id)s, %(name)s,%(new_parent_id_ecotaxa)s,%(rank)s,%(dt)s,%(dt)s  {plusvalues}); "
                    ).format(pluskeys=pluskeys, plusvalues=plusvalues)
                elif newname != NA:
                    qry = (
                        "INSERT INTO taxonomy_worms(id, name,parent_id,rank,creation_datetime,lastupdate_datetime {pluskeys}) "
                        "VALUES(%(ecotaxa_id)s, %(name)s,%(new_parent_id_ecotaxa)s,%(rank)s,%(dt)s,%(dt)s {plusvalues}); "
                    ).format(pluskeys=pluskeys, plusvalues=plusvalues)
                else:
                    qry = ""
                    with open("skipped.txt", "a") as f:
                        print(row, file=f)  # File
                    # raise
            elif row.action == "deprecier":
                if row.new_id_ecotaxa != NA:
                    qry = (
                        "UPDATE taxonomy_worms "
                        "SET rename_to=%(new_id_ecotaxa)s,taxostatus='D',rank=%(rank)s,lastupdate_datetime=%(dt)s "
                        "WHERE id=%(ecotaxa_id)s;"
                    )
                    params = {
                        "ecotaxa_id": row.ecotaxa_id,
                        "new_id_ecotaxa": row.new_id_ecotaxa,
                        "rank": rank,
                        "dt": dt,
                    }
                else:
                    qry = ""
                    errorfile.write(
                        " no rename_to defined - " + ",".join(map(str, row)) + "\n"
                    )
            elif row.action == "A supprimer":
                qry = "DELETE FROM taxonomy_worms " "WHERE id=%s;"
                params = (row.ecotaxa_id,)
                computename = False
            elif row.action == "changer type en Morpho":
                qry = (
                    "UPDATE taxonomy_worms SET taxotype='M',name=%s,lastupdate_datetime=%s "
                    "WHERE id=%s;"
                )
                params = (newname, dt, row.ecotaxa_id)
            elif row.action == "Ajouter aphia_id":
                qry = (
                    "UPDATE taxonomy_worms "
                    "SET name=%(name)s,aphia_id=%(aphia_id)s,rank=%(rank)s,lastupdate_datetime=%(dt)s "
                    "WHERE id=%(ecotaxa_id)s;"
                )
                params = {
                    "ecotaxa_id": row.ecotaxa_id,
                    "name": newname,
                    "aphia_id": row.aphia_id,
                    "rank": rank,
                    "dt": dt,
                }
            elif row.action == "Changer le parent":
                params = {
                    "ecotaxa_id": row.ecotaxa_id,
                    "name": newname,
                    "rank": rank,
                    "dt": dt,
                    "new_parent_id_ecotaxa": row.new_parent_id_ecotaxa,
                }
                if row.details.strip() == "Changer le parent":
                    qry = (
                        "UPDATE taxonomy_worms "
                        "SET name=%(name)s,aphia_id=%(aphia_id)s, parent_id=%(new_parent_id_ecotaxa)s,rank=%(rank)s,lastupdate_datetime=%(dt)s "
                        "WHERE id=%(ecotaxa_id)s;"
                    )
                    params["aphia_id"] = row.aphia_id
                elif row.details.strip() == "morpho parent deprécié":
                    qry = (
                        "UPDATE taxonomy_worms "
                        "SET name=%(name)s, parent_id=%(new_parent_id_ecotaxa)s,rank=%(rank)s,lastupdate_datetime=%(dt)s "
                        "WHERE id=%(ecotaxa_id)s;"
                    )
                elif row.details.strip() == "Brancher à nouvel ecotaxa_id":
                    qry = (
                        "UPDATE taxonomy_worms "
                        "SET name=%(name)s, aphia_id=%(aphia_id)s,parent_id=%(new_parent_id_ecotaxa)s,rank=%(rank)s,lastupdate_datetime=%(dt)s "
                        "WHERE id=%(ecotaxa_id)s;"
                    )
                    params["aphia_id"] = row.aphia_id
                elif (
                    row.details.strip()
                    == "Pas de match avec Worms mais rattache plus haut"
                ):
                    qry = (
                        "UPDATE taxonomy_worms "
                        "SET name=%(name)s, parent_id=%(new_parent_id_ecotaxa)s,rank=%(rank)s,lastupdate_datetime=%(dt)s "
                        "WHERE id=%(ecotaxa_id)s;"
                    )
                else:
                    qry = ""
                    with open("skipped.txt", "a") as f:
                        print(row, file=f)  # File
                    # raise row

            elif (
                row.action
                == "Changer le parent + Pas de match avec Worms mais rattache plus haut"
                or row.action
                == "Rien + Pas de match avec Worms mais rattache plus haut"
            ):
                qry = (
                    "UPDATE taxonomy_worms "
                    "SET parent_id=%(new_parent_id_ecotaxa)s,rank=%(rank)s,lastupdate_datetime=%(dt)s "
                    "WHERE id=%(ecotaxa_id)s;"
                )
                params = {
                    "ecotaxa_id": row.ecotaxa_id,
                    "new_parent_id_ecotaxa": row.new_parent_id_ecotaxa,
                    "rank": rank,
                    "dt": dt,
                }
            elif row.action == "Rien":
                params = {
                    "ecotaxa_id": row.ecotaxa_id,
                    "name": newname,
                    "rank": rank,
                    "dt": dt,
                }
                if row.aphia_id != NA:
                    qryplus = ",aphia_id=%(aphia_id)s"
                    params["aphia_id"] = row.aphia_id
                else:
                    qryplus = ""
                if row.new_parent_id_ecotaxa != NA:
                    qryplus += " ,parent_id=%(new_parent_id_ecotaxa)s"
                    params["new_parent_id_ecotaxa"] = row.new_parent_id_ecotaxa
                qry = (
                    "UPDATE taxonomy_worms "
                    "SET name=%(name)s,rank=%(rank)s,lastupdate_datetime=%(dt)s  {qryplus} "
                    "WHERE id=%(ecotaxa_id)s;"
                ).format(qryplus=qryplus)
            else:
                qry = ""
                errorfile.write(" no sql defined - " + ",".join(map(str, row)) + "\n")
            if qry != "":
                self.exec_sql(qry, params)
                with open(filesql, wr) as actionfile:
                    # For the log file, we'll use the cursor's mogrify to see the final SQL
                    with self.serverdb.cursor() as log_cur:
                        final_qry = log_cur.mogrify(qry, params).decode("utf-8")
                        actionfile.write(final_qry + "\n")
                qry = ""  # prevent double execution later
            if computename:
                self.compute_display_name([int(row.ecotaxa_id)])

        db.session.commit()

        qry = "SELECT setval('seq_taxonomy_worms', COALESCE((SELECT MAX(id) FROM taxonomy_worms), 1), false);"
        with db.engine.connect() as conn:
            res = conn.execute(qry)
        conn.close()

        # synchronise table ecotaxoserver et ecotaxa
        # deltable = 'psql -U user2  -h host2 -p port2 -d dbtwo -c "DROP TABLE taxonomy_worms;"'
        # os.system(deltable)
        # copytable = "pg_dump -t taxonomy_worms -p port -h host -U user dbone | psql -U user2 -h host2 -p port2 dbtwo"
        # os.system(copytable)

    def clone_taxo_table(self) -> None:
        for qry in WORMS_TAXO_DDL:
            self.exec_sql(qry)
        return None

    def compute_display_name(self, taxolist):
        return
        sql = """with duplicate as (select lower(name) as name
                                    from taxonomy_worms
                                    GROUP BY lower(name)
                                    HAVING count(*) > 1)
                 select t.id, t.name tname, p.name pname, p2.name p2name, p3.name p3name, t.display_name, t.taxostatus
                 from taxonomy_worms t
                          left JOIN duplicate d on lower(t.name) = d.name
                          left JOIN taxonomy_worms p on t.parent_id = p.id
                          left JOIN taxonomy_worms p2 on p.parent_id = p2.id
                          left JOIN taxonomy_worms p3 on p2.parent_id = p3.id
                 where d.name is not null
                    or t.display_name is null
                    or lower(t.name) in (select lower(st.name)
                                         from taxonomy_worms st
                                                  left JOIN taxonomy_worms sp on st.parent_id = sp.id
                                                  left JOIN taxonomy_worms sp2 on sp.parent_id = sp2.id
                                                  left JOIN taxonomy_worms sp3 on sp2.parent_id = sp3.id
                                         where (st.id = any (%(taxo)s) or sp.id = any (%(taxo)s) or
                                                sp2.id = any (%(taxo)s) or sp3.id = any (%(taxo)s)))
              """
        Duplicates = self.get_all(
            sql, {"taxo": taxolist}, cursor_factory=psycopg2.extras.RealDictCursor
        )

        starttime = datetime.datetime.now()
        DStats = {}

        def AddToDefStat(clestat):
            clestat = clestat.lower()
            if clestat in DStats:
                DStats[clestat] += 1
            else:
                DStats[clestat] = 1

        for D in Duplicates:
            cle = ntcv(D["tname"])
            AddToDefStat(cle)
            cle += "<" + ntcv(D["pname"])
            AddToDefStat(cle)
            cle += "<" + ntcv(D["p2name"])
            AddToDefStat(cle)
            cle += "<" + ntcv(D["p3name"])
            AddToDefStat(cle)

        for i, D in enumerate(Duplicates):
            cle = ntcv(D["tname"])
            if DStats[cle.lower()] == 1:
                Duplicates[i]["newname"] = cle
            else:
                cle += "<" + ntcv(D["pname"])
                if DStats[cle.lower()] == 1:
                    Duplicates[i]["newname"] = cle
                else:
                    cle += "<" + ntcv(D["p2name"])
                    if DStats[cle.lower()] == 1:
                        Duplicates[i]["newname"] = cle
                    else:
                        cle += "<" + ntcv(D["p3name"])
                        Duplicates[i]["newname"] = cle
            if D["taxostatus"] == "D":
                Duplicates[i]["newname"] += " (Deprecated)"
        app.logger.debug(
            "Compute time %s ", (datetime.datetime.now() - starttime).total_seconds()
        )
        starttime = datetime.datetime.now()
        UpdateParam = []
        for D in Duplicates:
            if D["display_name"] != D["newname"]:
                UpdateParam.append((int(D["id"]), D["newname"]))
        if len(UpdateParam) > 0:
            dt = datetime.datetime.now(datetime.timezone.utc)
            with self.serverdb.cursor() as cur:
                # The execute_values doesn't easily support extra params outside the VALUES list for all rows in the same way.
                # Actually, it can if we include it in each row of UpdateParam or if we use a different approach.
                # Let's just use the current time for all.

                UpdateParamWithTime = [(id, name, dt) for id, name in UpdateParam]
                psycopg2.extras.execute_values(
                    cur,
                    """UPDATE taxonomy_worms
                       SET display_name        = data.pdisplay_name
                         , lastupdate_datetime = data.pdt FROM (VALUES %s) AS data (pid
                         , pdisplay_name
                         , pdt)
                       WHERE id = data.pid""",
                    UpdateParamWithTime,
                )
        app.logger.debug(
            "Update time %s for %d rows",
            (datetime.datetime.now() - starttime).total_seconds(),
            len(UpdateParam),
        )

    POSSIBLE_ACTIONS = [
        ("Changer le parent", "NA"),
        ("Changer le parent", "Changer le parent"),
        ("Changer le parent", "Morpho : parent déprécié"),
        ("Changer le parent", "Pas de match Worms (branche + haut)"),
        ("Changer le parent", "Brancher à nouvel ecotaxa_id"),
        ("Rien", "NA"),
        ("Rien", "Rien"),
        ("Rien", "Rien : Root French"),
        ("Rien", "Rien : Morpho, parent P non déprécié"),
        ("Rien", "Pas de match Worms (branche + haut)"),
        ("Rien", "Enfant de French, garder hors arbre Worms"),
        ("Rien", "Rien : child of not-living"),
        ("Rien", "Rien : t0 or taxa not matchable"),
        ("deprecier", "NA"),
        ("deprecier", "deprecate to new id"),
        ("deprecier", "temporary associate to Biota"),
        ("deprecier", "deprecate to morpho"),
        ("changer type en Morpho", "Changer en Morpho"),
        ("Ajouter aphia_id", "NA"),
        ("Creer nouvelle categorie", "NA"),
        ("Creer nouvelle categorie", "Changer le parent"),
        ("Creer nouvelle categorie", "Rien"),
        ("Creer nouvelle categorie", "Brancher à nouvel ecotaxa_id"),
        ("Creer nouvelle categorie", "Nouvel ecotaxa_id"),
        (
            "Creer nouvelle categorie",
            "Nouvel ecotaxa_id + pas de match Worms (rattaché + haut)",
        ),
        ("A supprimer", "NA"),
    ]

    def read_and_check_csv(self):
        rows = []
        with open(self.filename, "r") as csvfile:
            reader = csv.DictReader(csvfile)
            line = 2
            for row in reader:
                # Clean and strip all values
                cleaned_row = {k: (v.strip() if v else "") for k, v in row.items()}

                action, details = cleaned_row["action"], cleaned_row["details"]
                assert (action, details) in self.POSSIBLE_ACTIONS, (action, details)
                ecotaxa_id, name_ecotaxa = (
                    int(cleaned_row["ecotaxa_id"]),
                    cleaned_row["name_ecotaxa"],
                )
                if ecotaxa_id < START_OF_WORMS:
                    res = self.get_one(
                        "select name from taxonomy_worms where id = %s", (ecotaxa_id,)
                    )
                    if res != name_ecotaxa:
                        print("XLS not present:", line, res, ecotaxa_id, name_ecotaxa)
                aphia_id, name_wrm = cleaned_row["aphia_id"], cleaned_row["name_wrm"]
                if (aphia_id, name_wrm) == (NA, NA) or (
                    aphia_id != NA and name_wrm != NA
                ):
                    pass
                else:
                    print("XLS worms inconsistent", line, aphia_id, name_wrm)

                rows.append(
                    CsvRow(
                        ecotaxa_id=ecotaxa_id,
                        new_id_ecotaxa=cleaned_row["new_id_ecotaxa"],
                        new_parent_id_ecotaxa=cleaned_row["new_parent_id_ecotaxa"],
                        aphia_id=aphia_id,
                        taxotype=cleaned_row["taxotype"],
                        action=action,
                        details=details,
                        name_ecotaxa=name_ecotaxa,
                        name_wrm=name_wrm,
                        rank=cleaned_row["rank"],
                    )
                )
                line += 1
        return rows


wormssynchro = WormsSynchronisation2(INPUT_CSV)
wormssynchro.do_worms_synchronisation()


def branch_taxon_parent(parent_id):
    print("")


def search_worms_parent(aphia_id):
    print("")
