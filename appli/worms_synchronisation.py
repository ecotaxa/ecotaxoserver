# -*- coding: utf-8 -*-
import csv
from appli import app,db,ntcv
import psycopg2.extras,datetime,os
class WormsSynchronisation(object):
    def __init__(self,filename="/home/imev/PycharmProjects/ecotaxoserver/appli/static/tableau_ecotaxa_worms_17122025_QC.csv"):
        self.serverdb = db.engine.raw_connection()
        self.filename = filename

    def exec_sql(self, sql, params=None, debug=False):
        with self.serverdb.cursor() as cur:
            result = cur.execute(sql,params)
        cur.close()
        return result

    def get_all(self, sql, params=None, debug=False, cursor_factory=psycopg2.extras.RealDictCursor):
        with self.serverdb.cursor(cursor_factory=cursor_factory) as cur:
            cur.execute(sql, params)
            res = cur.fetchall()
        cur.close()
        return res

    def do_worms_synchronisation(self):

        res = self.exec_sql("DROP TABLE taxonomy_worms;")
        print("session res=", res)
        qry = """
              DROP SEQUENCE IF EXISTS public.seq_taxonomy_worms;"""
        _ = self.exec_sql(qry)

        qry = """ CREATE TABLE taxonomy_worms (
                id INTEGER PRIMARY KEY,
                aphia_id INTEGER,
                parent_id INTEGER,
                name VARCHAR(100)  NOT NULL,
                taxotype CHAR DEFAULT 'P' NOT NULL,
                display_name VARCHAR(200),
                source_url VARCHAR(200),
                source_desc VARCHAR(1000),
                creator_email VARCHAR(255),
                creation_datetime TIMESTAMP,
                lastupdate_datetime TIMESTAMP,
                id_instance INTEGER,
                taxostatus CHAR DEFAULT 'A' NOT NULL,
                rename_to INTEGER,
                rank VARCHAR(24),
                nbrobj INTEGER,
                nbrobjcum INTEGER);"""
        print("session res=", res)
        _ = self.exec_sql(qry)
        qry = """
                INSERT INTO taxonomy_worms(id,parent_id,name,taxotype,display_name,source_url, source_desc, creator_email,creation_datetime,lastupdate_datetime,id_instance,taxostatus,rename_to,nbrobj,nbrobjcum) select id,parent_id,name,taxotype,display_name,source_url,source_desc,creator_email,creation_datetime,lastupdate_datetime,id_instance,taxostatus,rename_to,nbrobj,nbrobjcum from taxonomy;"""
        _ = self.exec_sql(qry)
        qry = "SELECT max(id) from taxonomy;"
        with db.engine.connect() as conn:
            res = conn.execute(qry)
        conn.close()
        for r in res:
            print("records===", r[0])
            num=r[0]
        qry = """
                CREATE SEQUENCE public.seq_taxonomy_worms
                START WITH %s
                INCREMENT BY 1
                NO MINVALUE
                NO MAXVALUE
                CACHE 1
                OWNED BY taxonomy_worms.id;""" % str(num)
        _ = self.exec_sql(qry)
        qry = """ALTER TABLE public.seq_taxonomy_worms OWNER TO postgres;"""
        _ = self.exec_sql(qry)
        errorfile = open("static/db_update/error.log", "w")
        NA = "NA"
        verifs = {}
        with open(self.filename, "r") as csvfile:
            reader = csv.DictReader(csvfile)
            print("file open", reader)
            i = 0
            index = 1

            for row in reader:
                i += 1
                computename = True
                dt = datetime.datetime.now(datetime.timezone.utc)
                row["action"] = row["action"].strip()
                row["name_ecotaxa"] = row["name_ecotaxa"].strip()
                row["name_wrm"] = row["name_wrm"].strip()
                if i > 0 and i == index * 50000:
                    index += 1
                filesql = "static/db_update/" + row["action"].replace(" ", "_") + ".sql"
                if os.path.exists(filesql):
                    wr = "a"
                else:
                    wr = "w"
                if row["aphia_id"] != NA and row["name_wrm"] != NA:
                    obj = {}
                    obj[str(row["aphia_id"])] = (
                        row["aphia_id"],
                        row["new_parent_id_ecotaxa"],
                    )
                    verifs.update(obj)
                if row["rank"] == NA:
                    rank = None
                else:
                    rank = row["rank"]
                if row["name_ecotaxa"] != row["name_wrm"] and row["name_wrm"] != NA:
                    newname = row["name_wrm"].replace("'", "''")
                elif row["name_ecotaxa"] != NA:
                    newname = row["name_ecotaxa"].replace("'", "''")
                else:
                    newname = NA + str(row["ecotaxa_id"])
                    if row["action"] == "Creer nouvelle categorie":
                        qry = "INSERT INTO taxonomy_worms(id, name,parent_id,rank,creation_datetime,lastupdate_datetime) VALUES({ecotaxa_id}, '{name}',{new_parent_id_ecotaxa},{rank},to_timestamp('{dt}','YYYY-MM-DD HH24:MI:SS'),to_timestamp('{dt}','YYYY-MM-DD HH24:MI:SS')); ".format(
                            ecotaxa_id=row["ecotaxa_id"],
                            name=newname,
                            new_parent_id_ecotaxa=row["new_parent_id_ecotaxa"],
                            rank=rank,
                            dt=dt
                        )
                        _ = self.exec_sql(qry)
                    else:
                        errorfile.write(" no name - " + ", ".join(row.values()) + "\n")
                    continue
                if (
                        row["action"] == "Creer nouvelle categorie"
                        or (row["action"] == "Rien" and int(row["ecotaxa_id"]) >= 100000)
                        or row["action"] == "Creer nouvelle categorie && deprecier"
                ):
                    if row["action"] != "Creer nouvelle categorie":
                        row["action"] = "Creer nouvelle categorie"
                    if row["action"] == "Creer nouvelle categorie && deprecier":
                        if row["new_id_ecotaxa"] != NA:
                            pluskeys = ", rename_to, taxostatus"
                            plusvalues = ", {new_id_ecotaxa}, 'D'"
                        else:
                            pluskeys = ", taxostatus"
                            plusvalues = ", 'D'"
                    else:
                        pluskeys = ""
                        plusvalues = ""

                    if row["aphia_id"] != NA:
                        qry = "INSERT INTO taxonomy_worms(id,aphia_id,name,parent_id,rank,creation_datetime,lastupdate_datetime {pluskeys}) VALUES({ecotaxa_id},{aphia_id}, '{name}',{new_parent_id_ecotaxa},'{rank}',to_timestamp('{dt}','YYYY-MM-DD HH24:MI:SS'),to_timestamp('{dt}','YYYY-MM-DD HH24:MI:SS')  {plusvalues}); ".format(
                            ecotaxa_id=row["ecotaxa_id"],
                            aphia_id=row["aphia_id"],
                            name=newname,
                            new_parent_id_ecotaxa=row["new_parent_id_ecotaxa"],
                            pluskeys=pluskeys,
                            plusvalues=plusvalues,
                            rank=rank, dt=dt
                        )
                    elif newname != NA:
                        qry = "INSERT INTO taxonomy_worms(id, name,parent_id,rank,creation_datetime,lastupdate_datetime {pluskeys}) VALUES({ecotaxa_id}, '{name}',{new_parent_id_ecotaxa},'{rank}',to_timestamp('{dt}','YYYY-MM-DD HH24:MI:SS'),to_timestamp('{dt}','YYYY-MM-DD HH24:MI:SS') {plusvalues}); ".format(
                            ecotaxa_id=row["ecotaxa_id"],
                            name=newname,
                            new_parent_id_ecotaxa=row["new_parent_id_ecotaxa"],
                            pluskeys=pluskeys,
                            plusvalues=plusvalues,
                            rank=rank, dt=dt
                        )
                elif row["action"] == "deprecier":
                    if row["new_id_ecotaxa"] != NA:
                        qry = "UPDATE taxonomy_worms SET rename_to={new_id_ecotaxa},taxostatus='D',rank='{rank}',lastupdate_datetime=to_timestamp('{dt}','YYYY-MM-DD HH24:MI:SS')  WHERE id={ecotaxa_id};".format(
                            ecotaxa_id=row["ecotaxa_id"],
                            new_id_ecotaxa=row["new_id_ecotaxa"],
                            rank=rank, dt=dt
                        )
                    else:
                        qry = ""
                        errorfile.write(
                            " no rename_to defined - " + ",".join(row.values()) + "\n"
                        )
                elif row["action"] == "A supprimer":
                    qry = "DELETE FROM taxonomy_worms WHERE id={ecotaxa_id};".format(
                        ecotaxa_id=row["ecotaxa_id"]
                    )
                    computename = False
                elif row["action"] == "changer type en Morpho":
                    qry = "UPDATE taxonomy_worms SET taxotype='M',name='{name}',lastupdate_datetime=to_timestamp('{dt}','YYYY-MM-DD HH24:MI:SS')  WHERE id={ecotaxa_id};".format(
                        ecotaxa_id=row["ecotaxa_id"], name=newname, dt=dt
                    )
                elif row["action"] == "Ajouter aphia_id":
                    qry = "UPDATE taxonomy_worms SET name='{name}',aphia_id={aphia_id},rank='{rank}',lastupdate_datetime=to_timestamp('{dt}','YYYY-MM-DD HH24:MI:SS')  WHERE id={ecotaxa_id};".format(
                        ecotaxa_id=row["ecotaxa_id"], name=newname, aphia_id=row["aphia_id"], rank=rank, dt=dt
                    )
                elif row["action"] == "Changer le parent":
                    if row["details"].strip() == "Changer le parent":
                        qry = "UPDATE taxonomy_worms SET name='{name}',aphia_id={aphia_id}, parent_id={new_parent_id_ecotaxa},rank='{rank}',lastupdate_datetime=to_timestamp('{dt}','YYYY-MM-DD HH24:MI:SS')  WHERE id={ecotaxa_id};".format(
                            ecotaxa_id=row["ecotaxa_id"],
                            name=newname,
                            aphia_id=row["aphia_id"],
                            new_parent_id_ecotaxa=row["new_parent_id_ecotaxa"],
                            rank=rank, dt=dt
                        )
                    elif row["details"].strip() == "morpho parent deprécié":
                        qry = "UPDATE taxonomy_worms SET name='{name}', parent_id={new_parent_id_ecotaxa},rank='{rank}',lastupdate_datetime=to_timestamp('{dt}','YYYY-MM-DD HH24:MI:SS')  WHERE id={ecotaxa_id};".format(
                            ecotaxa_id=row["ecotaxa_id"],
                            name=newname,
                            new_parent_id_ecotaxa=row["new_parent_id_ecotaxa"],
                            rank=rank, dt=dt
                        )
                    elif row["details"].strip() == "Brancher à nouvel ecotaxa_id":
                        qry = "UPDATE taxonomy_worms SET name='{name}', aphia_id={aphia_id},parent_id={new_parent_id_ecotaxa},rank='{rank}',lastupdate_datetime=to_timestamp('{dt}','YYYY-MM-DD HH24:MI:SS')  WHERE id={ecotaxa_id};".format(
                            ecotaxa_id=row["ecotaxa_id"],
                            name=newname,
                            new_parent_id_ecotaxa=row["new_parent_id_ecotaxa"],
                            aphia_id=row["aphia_id"],
                            rank=rank, dt=dt
                        )

                    elif (
                            row["details"].strip()
                            == "Pas de match avec Worms mais rattache plus haut"
                    ):
                        qry = "UPDATE taxonomy_worms SET name='{name}', parent_id={new_parent_id_ecotaxa},rank='{rank}',lastupdate_datetime=to_timestamp('{dt}','YYYY-MM-DD HH24:MI:SS')  WHERE id={ecotaxa_id};".format(
                            ecotaxa_id=row["ecotaxa_id"],
                            name=newname,
                            new_parent_id_ecotaxa=row["new_parent_id_ecotaxa"],
                            rank=rank, dt=dt
                        )
                elif (
                        row["action"]
                        == "Changer le parent + Pas de match avec Worms mais rattache plus haut"
                        or row["action"]
                        == "Rien + Pas de match avec Worms mais rattache plus haut"
                ):
                    qry = "UPDATE taxonomy_worms SET parent_id={new_parent_id_ecotaxa},rank='{rank}',lastupdate_datetime=to_timestamp('{dt}','YYYY-MM-DD HH24:MI:SS')  WHERE id={ecotaxa_id};".format(
                        ecotaxa_id=row["ecotaxa_id"],
                        new_parent_id_ecotaxa=row["new_parent_id_ecotaxa"], rank=rank, dt=dt
                    )
                elif row["action"] == "Rien":
                    if row["aphia_id"] != NA:
                        qryplus = ",aphia_id={aphia_id}".format(aphia_id=row["aphia_id"])
                    else:
                        qryplus = ""
                    if row["new_parent_id_ecotaxa"] != NA:
                        qryplus += " ,parent_id={new_parent_id_ecotaxa}".format(
                            new_parent_id_ecotaxa=row["new_parent_id_ecotaxa"]
                        )
                    qry = "UPDATE taxonomy_worms SET name='{name}',rank='{rank}',lastupdate_datetime=to_timestamp('{dt}','YYYY-MM-DD HH24:MI:SS')  {qryplus} WHERE id={ecotaxa_id};".format(
                        ecotaxa_id=row["ecotaxa_id"],
                        name=newname,
                        rank=rank, dt=dt,
                        qryplus=qryplus,
                    )
                else:
                    qry = ""
                    errorfile.write(" no sql defined - " + ",".join(row.values()) + "\n")
                if qry != "":
                    _ = self.exec_sql(qry)
                    with open(filesql, wr) as actionfile:
                        actionfile.write(qry + "\n")
                if computename:
                    self.compute_display_name([int(row["ecotaxa_id"])])
            db.session.commit()
            qry= "SELECT setval('seq_taxonomy_worms', COALESCE((SELECT MAX(id) FROM taxonomy_worms), 1), false);"
            with db.engine.connect() as conn:
                res = conn.execute(qry)
            conn.close()
            # synchronise table ecotaxoserver et ecotaxa
            deltable='psql -U postgres  -h 193.50.85.44 -p 5436 -d ecotaxa4 -c "DROP TABLE taxonomy_worms;"'
            os.system(deltable)
            copytable = "pg_dump -t taxonomy_worms -p 5436 -h 193.50.85.44 -U postgres  ecotaxoserver | psql -U postgres -h 193.50.85.44 -p 5436 ecotaxa4"
            os.system(copytable)


    def compute_display_name(self, taxolist):
        sql = """with duplicate as (select lower(name) as name from taxonomy_worms GROUP BY lower(name) HAVING count(*)>1)
              select t.id,t.name tname,p.name pname,p2.name p2name,p3.name p3name,t.display_name,t.taxostatus
              from taxonomy_worms t
              left JOIN duplicate d on lower(t.name)=d.name
              left JOIN taxonomy_worms p on t.parent_id=p.id
              left JOIN taxonomy_worms p2 on p.parent_id=p2.id
              left JOIN taxonomy_worms p3 on p2.parent_id=p3.id
              where d.name is not null or t.display_name is null 
              or lower(t.name) in (select lower(st.name) 
                                      from taxonomy_worms st
                                      left JOIN taxonomy_worms sp on st.parent_id=sp.id
                                      left JOIN taxonomy_worms sp2 on sp.parent_id=sp2.id
                                      left JOIN taxonomy_worms sp3 on sp2.parent_id=sp3.id
                                    where (st.id=any(%(taxo)s) or sp.id=any(%(taxo)s) or sp2.id=any(%(taxo)s) or sp3.id=any(%(taxo)s)  )  
                    )
              """
        Duplicates = self.get_all(sql, {'taxo': taxolist}, cursor_factory=psycopg2.extras.RealDictCursor)

        starttime = datetime.datetime.now()
        DStats = {}

        def AddToDefStat(clestat):
            clestat = clestat.lower()
            if clestat in DStats:
                DStats[clestat] += 1
            else:
                DStats[clestat] = 1

        for D in Duplicates:
            cle = ntcv(D['tname'])
            AddToDefStat(cle)
            cle += '<' + ntcv(D['pname'])
            AddToDefStat(cle)
            cle += '<' + ntcv(D['p2name'])
            AddToDefStat(cle)
            cle += '<' + ntcv(D['p3name'])
            AddToDefStat(cle)

        for i, D in enumerate(Duplicates):
            cle = ntcv(D['tname'])
            if DStats[cle.lower()] == 1:
                Duplicates[i]['newname'] = cle
            else:
                cle += '<' + ntcv(D['pname'])
                if DStats[cle.lower()] == 1:
                    Duplicates[i]['newname'] = cle
                else:
                    cle += '<' + ntcv(D['p2name'])
                    if DStats[cle.lower()] == 1:
                        Duplicates[i]['newname'] = cle
                    else:
                        cle += '<' + ntcv(D['p3name'])
                        Duplicates[i]['newname'] = cle
            if D['taxostatus'] == 'D':
                Duplicates[i]['newname'] += " (Deprecated)"
        app.logger.debug("Compute time %s ", (datetime.datetime.now() - starttime).total_seconds())
        starttime = datetime.datetime.now()
        UpdateParam = []
        for D in Duplicates:
            if D['display_name'] != D['newname']:
                UpdateParam.append((int(D['id']), D['newname']))
        if len(UpdateParam) > 0:
            dt = datetime.datetime.now(datetime.timezone.utc)
            with self.serverdb.cursor() as cur:
                psycopg2.extras.execute_values(cur, """UPDATE taxonomy_worms SET display_name = data.pdisplay_name,lastupdate_datetime=to_timestamp('{}','YYYY-MM-DD HH24:MI:SS') FROM (VALUES %s) AS data (pid, pdisplay_name) WHERE id = data.pid""".format(dt.strftime('%Y-%m-%d %H:%M:%S')), UpdateParam)
            cur.connection.commit()
            cur.close()
        app.logger.debug("Update time %s for %d rows", (datetime.datetime.now() - starttime).total_seconds(),
                         len(UpdateParam))

wormssynchro = WormsSynchronisation()
wormssynchro.do_worms_synchronisation()

def branch_taxon_parent(parent_id):
    print('')
def search_worms_parent(aphia_id):
    print('')