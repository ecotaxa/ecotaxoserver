# -*- coding: utf-8 -*-
import csv
from datetime import datetime, timezone
from typing import NamedTuple, List, Union, Dict, Tuple, Any, IO, Set

import psycopg2.extras

from appli import app, db, ntcv

WORMS_URL = "https://www.marinespecies.org/aphia.php?p=taxdetails&id="
AJOUTER_APHIA_ID = "Ajouter aphia_id"  # Once in CSV, for living->Biota
CHANGER_TYPE_EN_MORPHO = (
    "changer type en Morpho"  # 3 times in CSV, Protista + Chloroplast + Protoplastes
)
CHANGER_TYPE_EN_PHYLO = (
    "changer type en Phylo"  # next CSV
)
BRANCHER_A_NOUVEL_ECOTAXA_ID = "Brancher a nouvel ecotaxa_id"
DEPRECIER = "deprecier"
RIEN_FAIRE = "Rien"
A_SUPPRIMER = "A supprimer"
CHANGER_LE_PARENT = "Changer le parent"
PAS_MATCH_WORMS_BRANCHE_HAUT = "Pas de match Worms (branche + haut)"
MORPHO_PARENT_DEPRECIE = "Morpho : parent deprecie"
CREER_NOUVELLE_CATEGORIE = "Creer nouvelle categorie"
RIEN_MORPHO_PARENT_P_NON_DEPRECIE = "Rien : Morpho, parent P non deprecie"

ParamDictT = Dict[str, Union[int, str, datetime]]

DELETE_MARK = "'⊖'"

CANCEL_MARK = "'🗙'"
MARK_CANCELLED_SQL = (
    f"update taxonomy_worms set name ={CANCEL_MARK}||name where id=%(id)s"
)
# DEPRECATED_MARK = '→'
DEPRECATED_MARK = "' '"
MARK_DEPRECATED_SQL = (
    f"update taxonomy_worms set name = {DEPRECATED_MARK}||name where id=%(id)s"
)

EMBEDDED = (
    93382,
    56693,
    85123,
    27642,
    45074,
    11514,
    13381,
    56317,
    11758,
    342,
    25942,
    85008,
    93973,
    84963,
    85076,
    85011,
    85024,
    93491,
    85039,
    85025,
) # Now in CSV


class MiniTree:
    def __init__(self, conn):
        # Get a full parent relationship from DB
        self.children = {}
        self.names = {}
        self.parents = {}
        qry = "SELECT id, name, parent_id  FROM taxonomy_worms"
        with conn.cursor() as cur:
            cur.execute(qry)
            res = cur.fetchall()
        for child, name, parent in res:
            assert parent != child
            assert child is not None
            self.names[child] = name
            self.parents[child] = parent
            try:
                self.children[parent].append(child)
            except KeyError:
                self.children[parent] = [child]

    def existing_child(
        self, parent_id: Union[int, None], name: str
    ) -> Union[int, None]:
        if parent_id not in self.children:
            return None
        names = self.names
        already_there = [i for i in self.children[parent_id] if names[i] == name]
        if len(already_there) == 0:
            return None
        assert len(already_there) == 1
        return already_there[0]

    def store_child(self, parent_id: Union[int, None], name: str, child_id: int):
        assert name != NA
        self.names[child_id] = name
        self.parents[child_id] = parent_id
        if parent_id not in self.children:
            self.children[parent_id] = [child_id]
        else:
            self.children[parent_id].append(child_id)

    def get_one(
        self, cat_id: int
    ) -> Tuple[Union[int, None], Union[str, None], Union[int, None]]:
        if cat_id not in self.names:
            return None, None, None
        return cat_id, self.names[cat_id], self.parents[cat_id]

    def get_parent(self, cat_id: int) -> Union[int, None]:
        return self.parents[cat_id]

    def is_leaf(self, cat_id: int):
        return cat_id not in self.children

    def stream_all_children(self, cat_id):
        for child in self.children.get(cat_id, []):
            yield child
            yield from self.stream_all_children(child)

    def get_roots(self) -> List[Tuple[str, int]]:
        return self.children[None]

    def set_name(self, cat_id: int, name: str) -> None:
        self.names[cat_id] = name

    def change_parent(self, cat_id: int, parent_id: int):
        present_parent = self.parents[cat_id]
        self.children[present_parent].remove(cat_id)
        if parent_id in self.children:
            self.children[parent_id].append(cat_id)
        else:
            self.children[parent_id] = [cat_id]
        self.parents[cat_id] = parent_id

    def deepest_parent_not_in(self, cat_id: int, ids_to_suppress) -> int:
        lineage = self.get_lineage(cat_id)
        lineage.reverse()
        for parent_id in lineage:
            if parent_id in ids_to_suppress:
                return prev_parent, parent_id
            prev_parent = parent_id
        return lineage[-1], None

    def get_lineage(self, cat_id: int) -> List[int]:
        ret = []
        parent_id = self.parents[cat_id]
        while parent_id is not None:
            ret.append(parent_id)
            parent_id = self.parents[parent_id]
        return ret


FK_NAMES = ("taxonomy_worms_rename_to_fkey", "taxonomy_worms_parent_id_fkey")
FOREIGN_KEY_2 = """ALTER TABLE public.taxonomy_worms
        ADD CONSTRAINT taxonomy_worms_rename_to_fkey FOREIGN KEY (rename_to) REFERENCES public.taxonomy_worms (id);"""
FOREIGN_KEY_1 = """ALTER TABLE public.taxonomy_worms
        ADD CONSTRAINT taxonomy_worms_parent_id_fkey FOREIGN KEY (parent_id) REFERENCES public.taxonomy_worms (id);"""

WORMS_TAXO_DDL = [
    """DROP TABLE if exists gone_taxa;""",
    """CREATE TABLE gone_taxa
       (
           id                  INTEGER PRIMARY KEY,
           aphia_id            INTEGER,
           parent_id           INTEGER,
           name                VARCHAR(100),
           taxotype            CHAR DEFAULT 'P',
           display_name        VARCHAR(200),
           source_url          VARCHAR(200),
           source_desc         VARCHAR(1000),
           creator_email       VARCHAR(255),
           creation_datetime   TIMESTAMP,
           lastupdate_datetime TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
           id_instance         INTEGER,
           taxostatus          CHAR DEFAULT 'X',
           rename_to           INTEGER,
           rank                VARCHAR(24),
           nbrobj              INTEGER,
           nbrobjcum           INTEGER
       );""",
    """DROP TABLE if exists taxonomy_worms;""",
    """CREATE TABLE taxonomy_worms
       (
           id                  INTEGER PRIMARY KEY,
           aphia_id            INTEGER, /* Added instead of source_id */
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
           rank                VARCHAR(24), /* Added */
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
       DROP SEQUENCE IF EXISTS public.seq_taxonomy_worms;
    """
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
    """CREATE UNIQUE INDEX is_taxo_worms_aphia_id
        on public.taxonomy_worms (aphia_id);""",
    """CREATE INDEX "is_taxo_worms_name_lower"
        on public.taxonomy_worms (lower(name::text));""",
    """CREATE INDEX "is_taxo_worms_parent"
        on public.taxonomy_worms (parent_id);""",
    FOREIGN_KEY_1,
    FOREIGN_KEY_2,
    """ANALYZE public.taxonomy_worms;""",
]

INPUT_CSV = "static/tableau_ecotaxa_worms_05012025complet_fiches_ecopart.csv"

START_OF_WORMS = 100000

NA = "NA"


class CsvRow(NamedTuple):
    i: int
    ecotaxa_id: int
    new_id_ecotaxa: Union[int, None]
    new_parent_id_ecotaxa: Union[int, None]
    aphia_id: Union[int, None]
    taxotype: str
    action: str
    details: str
    name_ecotaxa: str
    name_wrm: str  # NA or real name
    rank: Union[str, None]


class WormsSynchronisation2(object):
    def __init__(self, filename):
        self.serverdb = db.engine.raw_connection()
        self.filename = filename

    def exec_sql(self, sql, params=None, debug=False):
        with self.serverdb.cursor() as cur:
            try:
                cur.execute(sql, params)
                self.serverdb.commit()
            except psycopg2.Error as e:
                print("SQL problem:", e, sql, params)
                self.serverdb = db.engine.raw_connection()
            except KeyError as e:
                print("Code problem:", e, sql, params)
                self.serverdb = db.engine.raw_connection()
        return None

    def get_one(self, sql, params=None):
        with self.serverdb.cursor() as cur:
            cur.execute(sql, params)
            res = cur.fetchone()
        return res

    def get_all(
        self,
        sql,
        params=None,
    ):
        with self.serverdb.cursor() as cur:
            cur.execute(sql, params)
            res = cur.fetchall()
        return res

    def do_worms_synchronisation(self):

        self.clone_taxo_table()
        errorfile = open("static/db_update/error.log", "w")
        all_actions = set([action for action, detail in self.POSSIBLE_ACTIONS])
        log_filenames = {
            action: "static/db_update/" + action.replace(" ", "_") + ".sql"
            for action in all_actions
        }
        action_logs = {
            action: open(filename, "w") for action, filename in log_filenames.items()
        }

        tree = MiniTree(self.serverdb)
        actions = self.read_and_check_csv(tree)
        ids_to_deprecate = set(
            [a_row.ecotaxa_id for a_row in actions if a_row.action == DEPRECIER]
        )
        ids_to_suppress = set(
            [a_row.ecotaxa_id for a_row in actions if a_row.action == A_SUPPRIMER]
        )
        deprecate_means_nothing = set()

        mapped_ids: Dict[int, int] = (
            {}
        )  # key=asked, from CSV, value=served, already existing

        # Care for creations first
        creations = [
            a_row for a_row in actions if a_row.action == CREER_NOUVELLE_CATEGORIE
        ]
        creations = self.order_for_creation(creations)
        for row in creations:
            if row.ecotaxa_id in (101109,):
                continue  # TODO: Fix in CSV, there is a double creation of Biota>Animalia>Arthropoda>Crustacea>Allotriocarida>Hexapoda>Insecta>Pterygota>Blattodea>Nocticolidae
            turn_into_add = False
            if row.new_parent_id_ecotaxa in mapped_ids:
                # print(f"Amend created parent from", row.new_parent_id_ecotaxa, "to",mapped_parent_id)
                row = row._replace(
                    new_parent_id_ecotaxa=mapped_ids[row.new_parent_id_ecotaxa]
                )
            if row.aphia_id is not None:
                existing_id = tree.existing_child(
                    row.new_parent_id_ecotaxa, row.name_wrm
                )
                if existing_id is not None:
                    # Replace a create+delete old with an ID redirection and aphia update
                    # print(f"Redir line {row.i}", row.ecotaxa_id, " -> ", existing_id)
                    row2 = row._replace(ecotaxa_id=existing_id)
                    qry, params = self.add_aphia_id(row2, tree)
                    mapped_ids[row.ecotaxa_id] = existing_id
                    if existing_id in ids_to_suppress:
                        ids_to_suppress.remove(existing_id)
                    else:
                        assert row.ecotaxa_id >= START_OF_WORMS, (existing_id, row)
                    turn_into_add = True
            if not turn_into_add:
                qry, params = self.create_row(row, tree)
            self.exec_sql(qry, params)
            self.log_query(action_logs[row.action], qry, params)

        print("Redirections (asked:served): ", mapped_ids)

        parent_changes = [
            a_row for a_row in actions if a_row.action == CHANGER_LE_PARENT
        ]
        for row in parent_changes:
            if row.new_parent_id_ecotaxa in mapped_ids:
                row = row._replace(
                    new_parent_id_ecotaxa=mapped_ids[row.new_parent_id_ecotaxa],
                    details="redirected",
                )
            if row.new_parent_id_ecotaxa == tree.get_parent(row.ecotaxa_id):
                # Already the good parent, just WoRMS-ize
                if row.aphia_id is not None:
                    qry, params = self.add_aphia_id(row, tree)
                else:
                    continue
            else:
                existing_id = tree.existing_child(
                    row.new_parent_id_ecotaxa,
                    row.name_ecotaxa if row.aphia_id is None else row.name_wrm,
                )
                if existing_id is not None:
                    print("Cannot apply verbatim", row)
                    if not tree.is_leaf(existing_id):
                        # print("Not a leaf CPR:",tree.get_one(existing_id),row)
                        if existing_id in ids_to_deprecate:
                            self.exec_sql(MARK_DEPRECATED_SQL, {"id": existing_id})
                            self.log_query(action_logs[row.action], qry, params)
                        elif existing_id in ids_to_suppress:
                            self.exec_sql(MARK_CANCELLED_SQL, {"id": existing_id})
                            self.log_query(action_logs[row.action], qry, params)
                        else:
                            assert False, (existing_id, row)
                        qry, params = self.change_parent(row, tree)
                    else:
                        if existing_id in ids_to_deprecate:
                            deprecate_means_nothing.add(
                                existing_id
                            )  # Avoid later deprecation
                        elif existing_id in ids_to_suppress:
                            ids_to_suppress.remove(existing_id)
                            if row.aphia_id is not None:
                                self.add_make_aphia_action(row, existing_id, actions)
                        else:
                            # No special post-processing, just make the parent change a deprecation
                            pass
                        self.add_deprecate_action(row, existing_id, actions)
                        continue
                else:
                    qry, params = self.change_parent(row, tree)
            self.exec_sql(qry, params)
            self.log_query(action_logs[row.action], qry, params)

        nothing_changes = [a_row for a_row in actions if a_row.action == RIEN_FAIRE]
        for row in nothing_changes:
            # Do nothing _structural_ but still create WoRMS facet, if needed
            if row.new_id_ecotaxa is not None:
                assert row.taxotype == "M"
                # Implied, it's a deprecation
                self.add_deprecate_action(row, row.new_id_ecotaxa, actions)
                continue

            elif row.aphia_id is None:
                if row.name_wrm != NA:
                    # Small data hack, need a rename using WoRMS name even if not a WoRMS-ification
                    qry, params = self.change_name(row, tree)
                else:
                    continue  # Nothing, for real
            else:
                existing_id = tree.existing_child(
                    row.new_parent_id_ecotaxa, row.name_wrm
                )
                if existing_id is not None and existing_id != row.ecotaxa_id:
                    print("Cannot apply verbatim", row)
                    if not tree.is_leaf(existing_id):
                        # print("Not a leaf AAI:", tree.get_one(existing_id), row)
                        if existing_id in ids_to_deprecate:
                            self.exec_sql(MARK_DEPRECATED_SQL, {"id": existing_id})
                            self.log_query(action_logs[row.action], qry, params)
                        elif existing_id in ids_to_suppress:
                            self.exec_sql(MARK_CANCELLED_SQL, {"id": existing_id})
                            self.log_query(action_logs[row.action], qry, params)
                        else:
                            assert False, (existing_id, row)
                        qry, params = self.add_aphia_id(row, tree)
                    else:
                        if existing_id in ids_to_deprecate:
                            # Will deprecate the other, less used probably
                            deprecate_means_nothing.add(
                                existing_id
                            )  # Avoid later deprecation
                        elif existing_id in ids_to_suppress:
                            # Relies on suppression to "make space" before applying WoRMS
                            ids_to_suppress.remove(existing_id)
                            self.add_make_aphia_action(row, existing_id, actions)
                        else:
                            assert False, ("Unforeseen", row)
                        self.add_deprecate_action(row, existing_id, actions)
                        continue
                else:
                    qry, params = self.add_aphia_id(row, tree)
            self.exec_sql(qry, params)
            self.log_query(action_logs[row.action], qry, params)

        self.serverdb.commit()

        for row in actions:
            if row.action in (
                A_SUPPRIMER,
                CREER_NOUVELLE_CATEGORIE,
                CHANGER_LE_PARENT,
                RIEN_FAIRE,
            ):
                continue

                # if row.name_ecotaxa != row.name_wrm and row.name_wrm != NA:
                #     newname = row.name_wrm
                # elif row.name_ecotaxa != NA:
                ##    No new name
                # newname = row.name_ecotaxa
                # else:
                #     assert (
                #         False
                #     ), (
                #         row.i
                #     )  # Not reachable with current CSV, some double NAs are in "A supprimer"
                if row.action == CREER_NOUVELLE_CATEGORIE:
                    newname = NA + str(row.ecotaxa_id)
                    qry = "INSERT INTO taxonomy_worms(id, name,parent_id,rank,creation_datetime,lastupdate_datetime) VALUES(%s, %s, %s, %s, %s, %s);"
                    params = (
                        row.ecotaxa_id,
                        newname,
                        row.new_parent_id_ecotaxa,
                        rank,
                        dt,
                        dt,
                    )
                    self.exec_sql(qry)
                else:
                    errorfile.write(" no name - " + ", ".join(map(str, row)) + "\n")
                continue
                # assert newname != NA
                # if (
                #     row.action == RIEN_FAIRE and int(row.ecotaxa_id) >= START_OF_WORMS
                # ) or row.action == "Creer nouvelle categorie && deprecier":
                #     assert False, row.i  # No such condition in CSV
                continue
                if row.action != CREER_NOUVELLE_CATEGORIE:
                    row = row._replace(action=CREER_NOUVELLE_CATEGORIE)

                params = {
                    "ecotaxa_id": row.ecotaxa_id,
                    "name": newname,
                    "new_parent_id_ecotaxa": row.new_parent_id_ecotaxa,
                    "dt": datetime.now(timezone.utc),
                }

                if row.action == "Creer nouvelle categorie && deprecier":
                    if row.new_id_ecotaxa is not None:
                        pluskeys = ", rename_to, taxostatus"
                        plusvalues = ", %(new_id_ecotaxa)s, 'D'"
                        params["new_id_ecotaxa"] = row.new_id_ecotaxa
                    else:
                        pluskeys = ", taxostatus"
                        plusvalues = ", 'D'"
                else:
                    pluskeys = ""
                    plusvalues = ""

                if row.aphia_id is not None:
                    qry = (
                        f"INSERT /*WCR{row.i}*/ INTO taxonomy_worms(id,aphia_id,name,parent_id,rank,creation_datetime,lastupdate_datetime {pluskeys}) "
                        f"VALUES(%(ecotaxa_id)s,%(aphia_id)s, %(name)s,%(new_parent_id_ecotaxa)s,%(rank)s,%(dt)s,%(dt)s  {plusvalues}); "
                    )
                    params["aphia_id"] = row.aphia_id
                    params["rank"] = row.rank
                else:  # Non-WoRMS creation
                    qry = (
                        f"INSERT /*NWC{row.i}*/ INTO taxonomy_worms(id,name,parent_id,creation_datetime,lastupdate_datetime {pluskeys}) "
                        f"VALUES(%(ecotaxa_id)s, %(name)s,%(new_parent_id_ecotaxa)s,%(dt)s,%(dt)s {plusvalues}); "
                    )
            elif row.action == DEPRECIER:
                # assert tree.get_parent(row.new_id_ecotaxa) == row.new_parent_id_ecotaxa, row
                if row.new_id_ecotaxa in mapped_ids:
                    mapped_id = mapped_ids[row.new_id_ecotaxa]
                    row = row._replace(new_id_ecotaxa=mapped_id)
                    if mapped_id == row.ecotaxa_id:
                        # Don't deprecate to self
                        deprecate_means_nothing.add(row.ecotaxa_id)
                if row.ecotaxa_id in deprecate_means_nothing:
                    deprecate_means_nothing.remove(row.ecotaxa_id)
                    if row.aphia_id is not None:
                        qry, params = self.add_aphia_id(row, tree)
                else:
                    parent_id = tree.get_parent(row.ecotaxa_id)
                    valid_parent, invalid_parent = tree.deepest_parent_not_in(
                        row.ecotaxa_id, ids_to_suppress
                    )
                    if parent_id != valid_parent:
                        # self.exec_sql("update taxonomy_worms set parent_id = %(parent)s where id=%(cat)s",
                        # {"parent": valid_parent, "cat": row.ecotaxa_id})
                        # tree.change_parent(row.ecotaxa_id, valid_parent)
                        # print("W: deprecating in invalid tree, parent", parent_id, "valid parent", valid_parent, "invalid parent", invalid_parent, row)
                        pass
                    try:
                        target_valid_parent, target_invalid_parent = (
                            tree.deepest_parent_not_in(
                                row.new_id_ecotaxa, ids_to_suppress
                            )
                        )
                        if target_invalid_parent is not None:
                            print(
                                "E:Invalid deprecate target",
                                row,
                                "lineage KO at",
                                target_invalid_parent,
                            )
                    except IndexError:  # TODO: Issue with top taxon
                        pass
                    qry, params = self.deprecate(row)
            elif row.action == CHANGER_TYPE_EN_MORPHO:
                qry, params = self.change_to_morpho(row)
            elif row.action == CHANGER_TYPE_EN_PHYLO:
                # Is now a composite: change to phylo + make aphia + change parent
                assert tree.get_parent(row.ecotaxa_id) != row.new_id_ecotaxa
                qry, params = self.change_parent(row, tree)
                self.log_query(action_logs[row.action], qry, params)
                self.exec_sql(qry, params)
                qry, params = self.add_aphia_id(row, tree)
                self.exec_sql(qry, params)
                self.log_query(action_logs[row.action], qry, params)
                qry, params = self.change_to_phylo(row)
            elif row.action == AJOUTER_APHIA_ID:
                qry, params = self.add_aphia_id(row, tree)
            elif (
                row.action
                == "Changer le parent + Pas de match avec Worms mais rattache plus haut"
                or row.action
                == "Rien + Pas de match avec Worms mais rattache plus haut"
            ):
                assert False, row.i  # No such condition in CSV
                qry = (
                    "UPDATE /*M1*/ taxonomy_worms "
                    "SET parent_id=%(new_parent_id_ecotaxa)s,rank=%(rank)s,lastupdate_datetime=%(dt)s "
                    "WHERE id=%(ecotaxa_id)s;"
                )
                params = {
                    "ecotaxa_id": row.ecotaxa_id,
                    "new_parent_id_ecotaxa": row.new_parent_id_ecotaxa,
                    "rank": row.rank,
                    "dt": datetime.now(timezone.utc),
                }
            else:
                qry = ""
                errorfile.write(" no sql defined - " + ",".join(map(str, row)) + "\n")
            if qry != "":
                self.exec_sql(qry, params)
                self.log_query(action_logs[row.action], qry, params)

        qry = "SELECT setval('seq_taxonomy_worms', COALESCE((SELECT MAX(id)+1 FROM taxonomy_worms), 1), false);"
        self.exec_sql(qry)

        self.mark_cancelled(ids_to_suppress)

        for fk in FK_NAMES:
            self.exec_sql(f"ALTER TABLE taxonomy_worms DROP CONSTRAINT {fk}")
        not_deleted = self.delete_unused_taxa(ids_to_suppress)
        for sql in (FOREIGN_KEY_1, FOREIGN_KEY_2):
            self.exec_sql(sql)

        self.unmark_cancelled_and_deleted(not_deleted) # was marked cancelled but could not be deleted

        self.mark_deleted_as_deprecated(not_deleted)

        self.compute_display_names()
        # self.add_deprecation_display_name()

        self.serverdb.commit()

        assert (
            len(deprecate_means_nothing) == 0
        ), deprecate_means_nothing  # Ensure fwd actions were consumed

        for action, fd in action_logs.items():
            fd.close()

        # synchronise table ecotaxoserver et ecotaxa
        # deltable = 'psql -U user2  -h host2 -p port2 -d dbtwo -c "DROP TABLE taxonomy_worms;"'
        # os.system(deltable)
        # copytable = "pg_dump -t taxonomy_worms -p port -h host -U user dbone | psql -U user2 -h host2 -p port2 dbtwo"
        # os.system(copytable)

    def log_query(
        self,
        fd: IO,
        qry: str,
        params: Any,
    ):
        with self.serverdb.cursor() as log_cur:
            final_qry = log_cur.mogrify(qry, params).decode("utf-8")
            fd.write(final_qry + "\n")

    @staticmethod
    def add_make_aphia_action(row: CsvRow, conflict_id: int, actions: List[CsvRow]):
        actions.append(
            CsvRow(
                i=len(actions) + 2,
                ecotaxa_id=conflict_id,
                new_id_ecotaxa=None,
                new_parent_id_ecotaxa=None,
                taxotype=NA,
                action=AJOUTER_APHIA_ID,
                details=NA,
                name_ecotaxa=NA,
                aphia_id=row.aphia_id,
                name_wrm=row.name_wrm,
                rank=row.rank,
            )
        )

    @staticmethod
    def add_deprecate_action(row: CsvRow, conflict_id: int, actions: List[CsvRow]):
        actions.append(
            CsvRow(
                i=len(actions) + 2,
                ecotaxa_id=row.ecotaxa_id,
                new_id_ecotaxa=conflict_id,
                new_parent_id_ecotaxa=None,
                taxotype=NA,
                action=DEPRECIER,
                details=NA,
                name_ecotaxa=NA,
                aphia_id=None,
                name_wrm=NA,
                rank=None,
            )
        )

    @staticmethod
    def order_for_creation(creations: List[CsvRow]) -> List[CsvRow]:
        # Re-arrange 'creations' so that parents are created before children, to respect the DB FK constraint
        new_ids = {row.ecotaxa_id for row in creations}
        ordered_creations = []
        already_done = set()
        while len(ordered_creations) < len(creations):
            for row in creations:
                if row.ecotaxa_id in already_done:
                    continue
                # If parent is not being created now OR parent is already created
                if (
                    row.new_parent_id_ecotaxa not in new_ids
                    or row.new_parent_id_ecotaxa in already_done
                ):
                    ordered_creations.append(row)
                    already_done.add(row.ecotaxa_id)
        return ordered_creations

    @staticmethod
    def deprecate(row: CsvRow) -> Tuple[str, ParamDictT]:
        # Note: In this case, WoRMS triplet is the _target_ taxon
        assert row.new_id_ecotaxa is not None, row.i
        # Note2: In this case, row.new_parent_id_ecotaxa is target's parent, BUT after eventual move.
        # assert tree.get_parent(row.new_id_ecotaxa) == row.new_parent_id_ecotaxa, row.i
        qry = (
            f"UPDATE /*DPR{row.i}*/ taxonomy_worms "
            "SET rename_to=%(new_id_ecotaxa)s,taxostatus='D',lastupdate_datetime=%(dt)s "
            "WHERE id=%(id)s;"
        )
        params = {
            "id": row.ecotaxa_id,
            "new_id_ecotaxa": row.new_id_ecotaxa,
            "dt": datetime.now(timezone.utc),
        }
        return qry, params

    @staticmethod
    def change_to_morpho(row: CsvRow) -> Tuple[str, ParamDictT]:
        # newname = row.name_ecotaxa
        qry = (
            f"UPDATE /*CTM{row.i}*/ taxonomy_worms SET taxotype='M',lastupdate_datetime=%(dt)s "
            "WHERE id=%(ecotaxa_id)s;"
        )
        params = {
            "ecotaxa_id": row.ecotaxa_id,
            # "name": row.name_wrm,
            "dt": datetime.now(timezone.utc),
        }
        return qry, params

    @staticmethod
    def change_to_phylo(row: CsvRow) -> Tuple[str, ParamDictT]:
        # newname = row.name_ecotaxa
        qry = (
            f"UPDATE /*CTP{row.i}*/ taxonomy_worms SET taxotype='P',lastupdate_datetime=%(dt)s "
            "WHERE id=%(ecotaxa_id)s;"
        )
        params = {
            "ecotaxa_id": row.ecotaxa_id,
            # "name": row.name_wrm,
            "dt": datetime.now(timezone.utc),
        }
        return qry, params
    @staticmethod
    def add_aphia_id(row: CsvRow, tree: MiniTree) -> Tuple[str, ParamDictT]:
        assert row.name_wrm != NA, row.i
        qry = (
            f"UPDATE /*AAI{row.i}*/ taxonomy_worms "
            "SET name=%(name)s,aphia_id=%(aphia_id)s,rank=%(rank)s,source_url=%(url)s,lastupdate_datetime=%(dt)s "
            "WHERE id=%(id)s;"
        )
        params = {
            "id": row.ecotaxa_id,
            "name": row.name_wrm,
            "aphia_id": row.aphia_id,
            "rank": row.rank,
            "url": WORMS_URL + str(row.aphia_id),
            "dt": datetime.now(timezone.utc),
        }
        tree.set_name(params["id"], params["name"])
        return qry, params

    @staticmethod
    def change_name(row: CsvRow, tree: MiniTree) -> Tuple[str, ParamDictT]:
        assert row.name_wrm != NA, row.i
        qry = (
            f"UPDATE /*CNA{row.i}*/ taxonomy_worms "
            "SET name=%(name)s,lastupdate_datetime=%(dt)s "
            "WHERE id=%(id)s;"
        )
        params = {
            "id": row.ecotaxa_id,
            "name": row.name_wrm,
            "dt": datetime.now(timezone.utc),
        }
        tree.set_name(params["id"], params["name"])
        return qry, params

    @staticmethod
    def change_parent(row: CsvRow, tree: MiniTree) -> Tuple[str, ParamDictT]:
        # Details are for sanity check
        if row.details == CHANGER_LE_PARENT:
            pass
        elif row.details == BRANCHER_A_NOUVEL_ECOTAXA_ID:
            if row.i not in (5100,):
                assert row.new_parent_id_ecotaxa is not None, row.i
                assert row.new_parent_id_ecotaxa > START_OF_WORMS, row.i
        elif row.details == MORPHO_PARENT_DEPRECIE:
            assert row.taxotype == "M", row.i
        elif row.details == PAS_MATCH_WORMS_BRANCHE_HAUT:
            assert row.aphia_id is None, row.i

        params: ParamDictT = {
            "id": row.ecotaxa_id,
            "dt": datetime.now(timezone.utc),
            "new_parent_id_ecotaxa": row.new_parent_id_ecotaxa,
        }
        qryplus = ""
        if row.aphia_id is not None:
            qryplus += (
                ",name=%(name)s,aphia_id=%(aphia_id)s,rank=%(rank)s,source_url=%(url)s"
            )
            params["name"] = row.name_wrm
            params["aphia_id"] = row.aphia_id
            params["rank"] = row.rank
            params["url"] = WORMS_URL + str(row.aphia_id)
            tree.set_name(params["id"], params["name"])
        qry = (
            f"UPDATE /*CPR{row.i}*/ taxonomy_worms "
            f"SET parent_id=%(new_parent_id_ecotaxa)s,lastupdate_datetime=%(dt)s {qryplus} "
            "WHERE id=%(id)s;"
        )
        tree.change_parent(params["id"], params["new_parent_id_ecotaxa"])
        return qry, params

    @staticmethod
    def create_row(row: CsvRow, tree: MiniTree) -> Tuple[str, ParamDictT]:
        params: ParamDictT = {
            "id": row.ecotaxa_id,
            "parent_id": row.new_parent_id_ecotaxa,
            "dt": datetime.now(timezone.utc),
        }
        pluskeys = ""
        plusvalues = ""
        if row.aphia_id is not None:
            qry = (
                f"INSERT /*WCR{row.i}*/ INTO taxonomy_worms(id,aphia_id,name,parent_id,rank,creation_datetime,lastupdate_datetime {pluskeys}) "
                f"VALUES(%(id)s,%(aphia_id)s,%(name)s,%(parent_id)s,%(rank)s,%(dt)s,%(dt)s  {plusvalues}); "
            )
            params["name"] = row.name_wrm
            params["aphia_id"] = row.aphia_id
            params["rank"] = row.rank
        else:  # Non-WoRMS creation
            qry = (
                f"INSERT /*NWC{row.i}*/ INTO taxonomy_worms(id,name,parent_id,creation_datetime,lastupdate_datetime {pluskeys}) "
                f"VALUES(%(id)s, %(name)s,%(parent_id)s,%(dt)s,%(dt)s {plusvalues}); "
            )
            params["name"] = row.name_ecotaxa
        tree.store_child(
            params["parent_id"],
            params["name"],
            params["id"],
        )
        return qry, params

    def mark_cancelled(self, ids_to_cancel: Set[int]) -> None:
        to_cancel_from_csv = list(ids_to_cancel)
        qry = (
            f"UPDATE taxonomy_worms SET name = {DELETE_MARK}||name "
            f"WHERE id=ANY(%s) AND LEFT(name,1) != {DELETE_MARK} "
        )
        chunk = 64
        for i in range(0, len(to_cancel_from_csv), chunk):
            params = (to_cancel_from_csv[i : i + chunk],)
            # print("marking ", i, " of ", len(to_cancel_from_csv))
            self.exec_sql(qry, params)

    def unmark_cancelled_and_deleted(self, ids_to_uncancel: Set[int]) -> None:
        to_uncancel_from_csv = list(ids_to_uncancel)
        qry = (
            f"UPDATE taxonomy_worms SET name = {DELETE_MARK}||SUBSTRING(name from 3) "
            f"WHERE id=ANY(%s) AND LEFT(name,2) = {DELETE_MARK}||{CANCEL_MARK} "
            #"AND id NOT IN (SELECT parent_id FROM taxonomy_worms WHERE parent_id IS NOT NULL)"
        )
        chunk = 64
        for i in range(0, len(to_uncancel_from_csv), chunk):
            params = (to_uncancel_from_csv[i : i + chunk],)
            # print("marking ", i, " of ", len(to_cancel_from_csv))
            self.exec_sql(qry, params)

    def mark_deleted_as_deprecated(self, ids_to_mark: Set[int]) -> None:
        to_uncancel_from_csv = list(ids_to_mark)
        qry = (
            f"UPDATE taxonomy_worms "
            f"SET name = SUBSTRING(name from 2), taxostatus = 'D', lastupdate_datetime=%s "
            f"WHERE id=ANY(%s) AND LEFT(name,1) = {DELETE_MARK}"
        )
        chunk = 1
        for i in range(0, len(to_uncancel_from_csv), chunk):
            params = (datetime.now(timezone.utc), to_uncancel_from_csv[i : i + chunk])
            self.exec_sql(qry, params)
        qry = (
            f"UPDATE taxonomy_worms "
            f"SET name = {DEPRECATED_MARK}||SUBSTRING(name from 2), taxostatus = 'D', lastupdate_datetime=%s "
            f"WHERE id=ANY(%s) AND LEFT(name,1) = {DELETE_MARK}"
        )
        chunk = 1
        for i in range(0, len(to_uncancel_from_csv), chunk):
            params = (datetime.now(timezone.utc), to_uncancel_from_csv[i : i + chunk])
            self.exec_sql(qry, params)

    def delete_unused_taxa(self, ids_to_delete: Set[int]) -> Set[int]:
        to_del_from_csv = list(ids_to_delete)
        safe_ids_deleted = set()
        while True:
            # Build safe list: taxa with no objects and leaves of the tree
            qry = (
                "SELECT id FROM taxonomy_worms "
                "WHERE id=ANY(%s) "
                "AND id NOT IN (SELECT parent_id FROM taxonomy_worms WHERE parent_id IS NOT NULL)"
                "AND NOT EXISTS (SELECT 1 FROM ecotaxainststat "
                "                 WHERE id_taxon=id AND id_instance in(1, 8) /*LOV + Watertools*/)"
            )
            res = self.get_all(qry, (to_del_from_csv,))
            safe_ids = [cat_id for (cat_id,) in res]
            # print("About to delete safely", len(safe_ids))

            self.serverdb.commit()
            if len(safe_ids) == 0:
                break

            qry = ("INSERT INTO gone_taxa (id, aphia_id, parent_id, name, taxotype, display_name, source_url, source_desc, "
                   "creator_email, creation_datetime, id_instance, taxostatus, rename_to, rank, nbrobj, nbrobjcum) "
            "SELECT id, aphia_id, parent_id, name, taxotype, display_name, source_url, source_desc, "
            "creator_email, creation_datetime, id_instance, 'X', rename_to, rank, nbrobj, nbrobjcum "
            "FROM taxonomy_worms WHERE id=ANY(%s)")
            chunk = 64
            for i in range(0, len(safe_ids), chunk):
                params = (safe_ids[i : i + chunk],)
                self.exec_sql(qry, params)

            qry = "DELETE FROM taxonomy_worms WHERE id=ANY(%s)"
            chunk = 64
            for i in range(0, len(safe_ids), chunk):
                params = (safe_ids[i : i + chunk],)
                # print("deleting ", row.i, " of ", len(safe_ids))
                self.exec_sql(qry, params)
            # print("Deleted ", len(safe_ids), " taxons")
            safe_ids_deleted.update(safe_ids)

        print("To delete: ", len(to_del_from_csv), " safe: ", len(safe_ids_deleted))
        not_deleted = set(to_del_from_csv).difference(safe_ids_deleted)
        print("Not deleted: ", not_deleted)
        return not_deleted

    def clone_taxo_table(self) -> None:
        for qry in WORMS_TAXO_DDL:
            self.exec_sql(qry)
        self.serverdb.commit()
        return None

    def compute_display_names(self):
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
                                              left JOIN taxonomy_worms sp3 on sp2.parent_id = sp3.id)
              """
        Duplicates = []
        def noCross(name:str):
            return name
            if name is None:
                return name
            if name.startswith(CANCEL_MARK[1]):
                return name[1:]
            return name
        for id_, tname, pname, p2name, p3name, display_name, taxostatus in self.get_all(
            sql, {}
        ):
            Duplicates.append(
                {
                    "id": id_,
                    "tname": noCross(tname),
                    "pname": noCross(pname),
                    "p2name": noCross(p2name),
                    "p3name": noCross(p3name),
                    "display_name": display_name,
                    "taxostatus": taxostatus,
                }
            )

        starttime = datetime.now()
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
        app.logger.debug(
            "Compute time %s ", (datetime.now() - starttime).total_seconds()
        )
        starttime = datetime.now()
        UpdateParam = []
        for D in Duplicates:
            if D["display_name"] != D["newname"]:
                UpdateParam.append((int(D["id"]), D["newname"]))
        if len(UpdateParam) > 0:
            dt = datetime.now(timezone.utc)
            with self.serverdb.cursor() as cur:
                # The execute_values doesn't easily support extra params outside the VALUES list for all rows in the same way.
                # Actually, row.it can if we include it in each row of UpdateParam or if we use a different approach.
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
            (datetime.now() - starttime).total_seconds(),
            len(UpdateParam),
        )

    POSSIBLE_ACTIONS = [
        (CHANGER_LE_PARENT, "Changement parent, demande Camille M."),
        (CHANGER_LE_PARENT, "NA"),
        (CHANGER_LE_PARENT, CHANGER_LE_PARENT),
        (CHANGER_LE_PARENT, MORPHO_PARENT_DEPRECIE),
        (CHANGER_LE_PARENT, PAS_MATCH_WORMS_BRANCHE_HAUT),
        (CHANGER_LE_PARENT, BRANCHER_A_NOUVEL_ECOTAXA_ID),
        (RIEN_FAIRE, "NA"),
        (RIEN_FAIRE, RIEN_FAIRE),
        (RIEN_FAIRE, "Rien : Root French"),
        (RIEN_FAIRE, RIEN_MORPHO_PARENT_P_NON_DEPRECIE),
        (RIEN_FAIRE, PAS_MATCH_WORMS_BRANCHE_HAUT),
        (RIEN_FAIRE, "Enfant de French, garder hors arbre Worms"),
        (RIEN_FAIRE, "Rien : child of not-living"),
        (RIEN_FAIRE, "Rien : t0 or taxa not matchable"),
        (RIEN_FAIRE, "Rien : morpho racine Not-living"),
        (RIEN_FAIRE, "Rien : parent direct morpho"),
        (DEPRECIER, "NA"),
        (DEPRECIER, "deprecate to new id"),
        (DEPRECIER, "temporary associate to Biota"),
        (DEPRECIER, "deprecate to morpho"),
        (CHANGER_TYPE_EN_MORPHO, "Changer en Morpho"),
        (CHANGER_TYPE_EN_PHYLO, "Changer en Phylo"),
        (AJOUTER_APHIA_ID, "NA"),
        (CREER_NOUVELLE_CATEGORIE, "NA"),
        (CREER_NOUVELLE_CATEGORIE, CHANGER_LE_PARENT),
        (CREER_NOUVELLE_CATEGORIE, RIEN_FAIRE),
        (CREER_NOUVELLE_CATEGORIE, BRANCHER_A_NOUVEL_ECOTAXA_ID),
        (CREER_NOUVELLE_CATEGORIE, "Nouvel ecotaxa_id"),
        (
            CREER_NOUVELLE_CATEGORIE,
            "Nouvel ecotaxa_id + pas de match Worms (rattache + haut)",
        ),
        (A_SUPPRIMER, "NA"),
    ]
    POSSIBLE_RANKS = (
        "Kingdom",
        "Subkingdom",
        "Infrakingdom",
        "Phylum (Division)",
        "Phylum",
        "Subphylum",
        "Infraphylum",
        "Parvphylum",
        "Superclass",
        "Class",
        "Subclass",
        "Infraclass",
        "Superorder",
        "Order",
        "Suborder",
        "Infraorder",
        "Superfamily",
        "Family",
        "Subfamily",
        "Tribe",
        "Genus",
        "Subgenus",
        "Species",
        "Subspecies",
        "Variety",
        "Superdomain",
        "Gigaclass",
        "Section",
        "Forma",
        "Subphylum (Subdivision)",
        "Superphylum",
        "Parvorder",
        "Subsection",
        "Subterclass",
        "Epifamily",
        "Megaclass",
    )

    def read_and_check_csv(self, tree: MiniTree) -> List[CsvRow]:
        rows = []
        trans_table = str.maketrans({"é": "e", "à": "a", "ç": "c", "ä": "a"})
        with open(self.filename, "r") as csvfile:
            reader = csv.DictReader(csvfile)
            line = 2
            for row in reader:
                # Clean and strip all values
                cleaned_row = {k: (v.strip() if v else "") for k, v in row.items()}

                action, details = cleaned_row["action"], cleaned_row["details"]
                assert (action, details) in self.POSSIBLE_ACTIONS, (
                    line,
                    action,
                    details,
                )
                ecotaxa_id, name_ecotaxa, taxotype = (
                    int(cleaned_row["ecotaxa_id"]),
                    cleaned_row["name_ecotaxa"],
                    cleaned_row["taxotype"],
                )
                db_parent_id = None
                if ecotaxa_id < START_OF_WORMS:
                    db_id, db_name, db_parent_id = tree.get_one(ecotaxa_id)
                    if db_id is not None:
                        db_name = db_name.translate(trans_table)
                        if db_name != name_ecotaxa:
                            print(
                                f"XLS not present line {line}",
                                ecotaxa_id,
                                name_ecotaxa,
                            )
                    else:
                        print("Not found taxon ID", ecotaxa_id, name_ecotaxa)
                aphia_id, name_wrm, rank = (
                    cleaned_row["aphia_id"],
                    cleaned_row["name_wrm"],
                    cleaned_row["rank"],
                )
                if (aphia_id, name_wrm, rank) == (NA, NA, NA) or (
                    aphia_id != NA and name_wrm != NA and rank != NA
                ):
                    pass  # OK, all of them or none of them
                else:
                    if aphia_id == NA and name_wrm != NA and rank == NA:
                        # Special case for Morpho renaming
                        assert taxotype == "M"
                        assert action == RIEN_FAIRE
                        assert details == RIEN_MORPHO_PARENT_P_NON_DEPRECIE
                    else:
                        print(
                            f"XLS worms inconsistent line {line}",
                            aphia_id,
                            name_wrm,
                            rank,
                        )
                aphia_id = int(aphia_id) if aphia_id != NA else None
                rank = rank if rank != NA else None
                if rank is not None:
                    assert rank in self.POSSIBLE_RANKS, (line, rank)

                new_id_ecotaxa = (
                    int(cleaned_row["new_id_ecotaxa"])
                    if cleaned_row["new_id_ecotaxa"] != NA
                    else None
                )
                new_parent_id_ecotaxa = (
                    int(cleaned_row["new_parent_id_ecotaxa"])
                    if cleaned_row["new_parent_id_ecotaxa"] != NA
                    else None
                )

                assert "'" not in name_wrm, (line, name_wrm)

                rows.append(
                    CsvRow(
                        i=line,
                        ecotaxa_id=ecotaxa_id,
                        new_id_ecotaxa=new_id_ecotaxa,
                        new_parent_id_ecotaxa=new_parent_id_ecotaxa,
                        aphia_id=aphia_id,
                        taxotype=taxotype,
                        action=action,
                        details=details,
                        name_ecotaxa=name_ecotaxa,
                        name_wrm=name_wrm,
                        rank=rank,
                    )
                )
                line += 1
        return rows

    def add_deprecation_display_name(self):
        sql = ("UPDATE taxonomy_worms SET display_name = display_name||' (Deprecated)' "
        "WHERE taxostatus='D'")
        self.exec_sql(sql)

wormssynchro = WormsSynchronisation2(INPUT_CSV)
wormssynchro.do_worms_synchronisation()


def branch_taxon_parent(parent_id):
    print("")


def search_worms_parent(aphia_id):
    print("")
