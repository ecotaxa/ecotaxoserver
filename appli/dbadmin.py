from flask import Blueprint, render_template, g, flash,request,url_for,json,escape
from flask_login import current_user
from appli import app,ObjectToStr,PrintInCharte,database,gvg,gvp,user_datastore,DecodeEqualList,ScaleForDisplay,ntcv,services
from pathlib import Path
from flask_security import Security, SQLAlchemyUserDatastore
from flask_security import login_required
from flask_security.decorators import roles_accepted
# from appli.search.leftfilters import getcommonfilters
import os,time,math,collections,appli,psycopg2.extras
from appli.database import GetAll,ExecSQL,db,GetAssoc
from sqlalchemy import text


@app.route('/dbadmin/viewsizes')
@login_required
@roles_accepted(database.AdministratorLabel)
def dbadmin_viewsizes():
    g.headcenter="Database objects size (public schema only)<br><a href=/admin/>Back to admin home</a>"

    sql="""SELECT c.relname, c.relkind, CASE WHEN c.relkind='i' THEN c2.tablename ELSE c.relname END fromtable,pg_relation_size(('"' || c.relname || '"')::regclass)/(1024*1024) szMB
FROM
 pg_namespace ns,
 pg_class c LEFT OUTER JOIN
 pg_indexes c2 ON c.relname = c2.indexname
WHERE c.relnamespace = ns.oid
 AND ns.nspname = 'public'
 AND c.relkind IN ('r' ,'i')
ORDER BY c.relkind DESC, pg_relation_size(('"' || c.relname || '"')::regclass) DESC
"""
    res = GetAll(sql) #,debug=True
    txt="""<table class='table table-bordered table-condensed table-hover' style="width:500px;">
            <tr><th width=200>Object</td><th witdth=200>Table</td><th width=100>Size (Mb)</td></tr>"""
    for r in res:
        txt+="""<tr><td>{0}</td>
        <td>{2}</td>
        <td>{3}</td>

        </tr>""".format(*r)
    txt+="</table>"

    return PrintInCharte(txt)


@app.route('/dbadmin/viewtaxoerror')
@login_required
@roles_accepted(database.AdministratorLabel)
def dbadmin_viewtaxoerror():
    g.headcenter="Database Taxonomy errors<br><a href=/admin/>Back to admin home</a>"

    sql="""Select 'Missing parent' reason,t.id,t.parent_id,t.name,t.aphia_id
from taxonomy_worms t where parent_id not in (select id from taxonomy);
"""
    cur = db.engine.raw_connection().cursor()
    try:
        txt="<table class='table table-bordered table-condensed table-hover'>"
        cur.execute(sql)
        txt+="<tr><td>"+("</td><td>".join([x[0] for x in cur.description]))+"</td></tr>"
        for r in cur:
            txt+="<tr><td>"+("</td><td>".join([str(x) for x in r]))+"</td></tr>"
        txt+="</table>"
    finally:
        cur.close()

    return PrintInCharte(txt)


@app.route('/dbadmin/viewbloat')
@login_required
@roles_accepted(database.AdministratorLabel)
def dbadmin_viewbloat():
    g.headcenter="Database objects wasted space<br><a href=/admin/>Back to admin home</a>"
    sql="""SELECT
        schemaname, tablename, reltuples::bigint, relpages::bigint, otta,
        ROUND(CASE WHEN otta=0 THEN 0.0 ELSE sml.relpages/otta::numeric END,1) AS tbloat,
        relpages::bigint - otta AS wastedpages,
        bs*(sml.relpages-otta)::bigint AS wastedbytes,
        pg_size_pretty((bs*(relpages-otta))::bigint) AS wastedsize,
        iname, ituples::bigint, ipages::bigint, iotta,
        ROUND(CASE WHEN iotta=0 OR ipages=0 THEN 0.0 ELSE ipages/iotta::numeric END,1) AS ibloat,
        CASE WHEN ipages < iotta THEN 0 ELSE ipages::bigint - iotta END AS wastedipages,
        CASE WHEN ipages < iotta THEN 0 ELSE bs*(ipages-iotta) END AS wastedibytes,
        CASE WHEN ipages < iotta THEN '0' ELSE pg_size_pretty((bs*(ipages-iotta))::bigint) END AS wastedisize
      FROM (
        SELECT
          schemaname, tablename, cc.reltuples, cc.relpages, bs,
          CEIL((cc.reltuples*((datahdr+ma-
            (CASE WHEN datahdr%ma=0 THEN ma ELSE datahdr%ma END))+nullhdr2+4))/(bs-20::float)) AS otta,
          COALESCE(c2.relname,'?') AS iname, COALESCE(c2.reltuples,0) AS ituples, COALESCE(c2.relpages,0) AS ipages,
          COALESCE(CEIL((c2.reltuples*(datahdr-12))/(bs-20::float)),0) AS iotta -- very rough approximation, assumes all cols
        FROM (
          SELECT
            ma,bs,schemaname,tablename,
            (datawidth+(hdr+ma-(case when hdr%ma=0 THEN ma ELSE hdr%ma END)))::numeric AS datahdr,
            (maxfracsum*(nullhdr+ma-(case when nullhdr%ma=0 THEN ma ELSE nullhdr%ma END))) AS nullhdr2
          FROM (
            SELECT
              schemaname, tablename, hdr, ma, bs,
              SUM((1-null_frac)*avg_width) AS datawidth,
              MAX(null_frac) AS maxfracsum,
              hdr+(
                SELECT 1+count(*)/8
                FROM pg_stats s2
                WHERE null_frac<>0 AND s2.schemaname = s.schemaname AND s2.tablename = s.tablename
              ) AS nullhdr
            FROM pg_stats s, (
              SELECT
                (SELECT current_setting('block_size')::numeric) AS bs,
                CASE WHEN substring(v,12,3) IN ('8.0','8.1','8.2') THEN 27 ELSE 23 END AS hdr,
                CASE WHEN v ~ 'mingw32' THEN 8 ELSE 4 END AS ma
              FROM (SELECT version() AS v) AS foo
            ) AS constants
            GROUP BY 1,2,3,4,5
          ) AS foo
        ) AS rs
        JOIN pg_class cc ON cc.relname = rs.tablename
        JOIN pg_namespace nn ON cc.relnamespace = nn.oid AND nn.nspname = rs.schemaname
        LEFT JOIN pg_index i ON indrelid = cc.oid
        LEFT JOIN pg_class c2 ON c2.oid = i.indexrelid
      ) AS sml
      WHERE sml.relpages - otta > 0 OR ipages - iotta > 10
      ORDER BY wastedbytes DESC, wastedibytes DESC
"""
    cur = db.engine.raw_connection().cursor()
    try:
        txt="<table class='table table-bordered table-condensed table-hover'>"
        cur.execute(sql)
        txt+="<tr><td>"+("</td><td>".join([x[0] for x in cur.description]))+"</td></tr>"
        for r in cur:
            txt+="<tr><td>"+("</td><td>".join([str(x) for x in r]))+"</td></tr>"
        txt+="</table>"
    finally:
        cur.close()
    return PrintInCharte(txt)

@app.route('/dbadmin/recomputestat')
@login_required
@roles_accepted(database.AdministratorLabel)
def dbadmin_recomputestat():
    g.headcenter="Statistics recompute<br><a href=/admin/>Back to admin home</a>"
    appli.services.RefreshTaxoStat()
    return PrintInCharte("Statistics recompute done")


@app.route('/dbadmin/console', methods=['GET', 'POST'])
@login_required
@roles_accepted(database.AdministratorLabel)
def dbadmin_console():
    sql=gvp("sql")
    if len(request.form)>0 and request.referrer!=request.url: # si post doit venir de cette page
        return PrintInCharte("Invalid referer")
    g.headcenter="<font color=red style='font-size:18px;'>Warning : This screen must be used only by experts</font><br><a href=/admin/>Back to admin home</a>"
    txt="<form method=post>SQL : <textarea name=sql rows=15 cols=100>%s</textarea><br>"%escape(sql)
    txt+="""<input type=submit class='btn btn-primary' name=doselect value='Execute Select'>
    <input type=submit class='btn btn-primary' name=dodml value='Execute DML'>
    Note : For DML ; can be used, but only the result of the last query displayed
    </form>"""
    if gvp("doselect"):
        txt+="<br>Select Result :"
        cur = db.engine.raw_connection().cursor()
        try:
            cur.execute(sql)
            txt+="<table class='table table-condensed table-bordered'>"
            for c in cur.description:
                txt+="<td>%s</td>"%c[0]
            for r in cur:
                s="<tr>"
                for c in r:
                    s+="<td>%s</td>"%c
                txt+=s+"</tr>"
            txt+="</table>"

        except Exception as e :
            txt+="<br>Error = %s"%e
            cur.connection.rollback()
        finally:
            cur.close()
    if gvp("dodml"):
        txt+="<br>DML Result :"
        cur = db.engine.raw_connection().cursor()
        try:
            cur.execute(sql)
            txt+="%s rows impacted"%cur.rowcount
            cur.connection.commit()
        except Exception as e :
            txt+="<br>Error = %s"%e
            cur.connection.rollback()
        finally:
            cur.close()


    return PrintInCharte(txt)

@app.route('/dbadmin/taxoworms')
def dbadmin_taxoworms():
    import csv
    import os

    rowdef = ['ecotaxa_id', 'new_id_ecotaxa', 'new_parent_id_ecotaxa', 'aphia_id', 'action', 'details', 'name_ecotaxa',
              'name_wrm']
    filename = 'tableau_ecotaxa_worms_211025.csv'
    action = ''
    get_old_parent_id = "(SELECT parent_id FROM taxonomy a WHERE a.id = {ecotaxa_id})"
    get_old_type = "(SELECT type FROM taxonomy a WHERE a.id = {ecotaxa_id})"
    result = db.engine.execute(text("DROP TABLE taxonomy_worms;"))
    qry = """ CREATE TABLE taxonomy_worms (
        id INTEGER,
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
        kingdom VARCHAR(128),
        rank VARCHAR(24),
        lsid VARCHAR(257),
        nbrobj INTEGER,
        nbrobjcum INTEGER);"""
    result = db.engine.execute(text(qry));
    qry = """
        INSERT INTO taxonomy_worms(id,parent_id,name,taxotype,display_name,source_url, source_desc, creator_email,creation_datetime,lastupdate_datetime,id_instance,taxostatus,rename_to,nbrobj,nbrobjcum) select id,parent_id,name,taxotype,display_name,source_url,source_desc,creator_email,creation_datetime,lastupdate_datetime,id_instance,taxostatus,rename_to,nbrobj,nbrobjcum from taxonomy;"""
    result = db.engine.execute(text(qry));
    fileworms = open('static/db_update/worms.sql', 'w');
    errorfile = open('static/db_update/error.log', 'w');
    NA = "NA";
    verifs = {};
    with open('static/' + filename, 'r') as csvfile:
        reader = csv.DictReader(csvfile)
        i = 0

        index = 1
        for row in reader:
            i += 1
            row['action'] = row['action'].strip()
            row['name_ecotaxa'] = row['name_ecotaxa'].strip()
            row['name_wrm'] = row['name_wrm'].strip()
            if i > 0 and i == index * 50000:
                index += 1;
            filesql = 'static/db_update/' + row['action'].replace(' ', '_') + str(index) + '.sql'
            if os.path.exists(filesql):
                wr = "a"
            else:
                wr = "w"
            if row['aphia_id'].strip() != NA and row['name_wrm'] != NA:
                obj = {};
                obj[str(row['aphia_id'])] = (row['aphia_id'], row['new_parent_id_ecotaxa']);
                verifs.update(obj);
                # sqlworms="INSERT INTO worms(aphia_id, scientificname, url, authority, status,unacceptreason,taxon_rank_id,rank,valid_aphia_id,valid_name,valid_authority,parent_name_usage_id,kingdom,phylum,class_, family, genus, citation, lsid, is_marine, is_brackish,is_freshwater,is_terrestrial,is_extinct,match_type) VALUES({aphia_id},'{name_wrm}', '', '', '','',0,0,{aphia_id},'{name_wrm}','',{new_parent_id_ecotaxa},'','','','', '', '', '', True, True,False,False,False,'');".format(aphia_id=row['aphia_id'],name_wrm=row['name_wrm'],new_parent_id_ecotaxa=row['new_parent_id_ecotaxa'])
                # fileworms.write(sqlworms+"\n")

            if row['name_ecotaxa'] != row['name_wrm'] and row['name_wrm'] != NA:
                newname = row['name_wrm'].replace("'", "''")
            elif row['name_ecotaxa'] != NA:
                newname = row['name_ecotaxa'].replace("'", "''")
            else:
                newname = NA + str(row['ecotaxa_id'])
                if row['action'] == 'Creer nouvelle categorie':
                    qry = "INSERT INTO taxonomy_worms(id, name,parent_id) VALUES({ecotaxa_id}, '{name}',{new_parent_id_ecotaxa}); ".format(
                        ecotaxa_id=row['ecotaxa_id'], new_id_ecotaxa=row['new_id_ecotaxa'], name=newname,
                        new_parent_id_ecotaxa=row['new_parent_id_ecotaxa'])
                    errorfile.write(qry + "\n")
                else:
                    errorfile.write(" no name - " + ", ".join(row.values()) + "\n")
                continue
            if row['action'] == 'Creer nouvelle categorie' or (
                    row['action'] == 'Rien' and int(row['ecotaxa_id']) >= 100000):
                if row['action'] == 'Rien':
                    row['action'] = 'Creer nouvelle categorie'
                if row['aphia_id'] != NA:
                    qry = "INSERT INTO taxonomy_worms(id,aphia_id, name,parent_id) VALUES({ecotaxa_id},{aphia_id}, '{name}',{new_parent_id_ecotaxa}); ".format(
                        ecotaxa_id=row['ecotaxa_id'], new_id_ecotaxa=row['new_id_ecotaxa'], aphia_id=row['aphia_id'],
                        name=newname, new_parent_id_ecotaxa=row['new_parent_id_ecotaxa'])
                elif newname != NA:
                    qry = "INSERT INTO taxonomy_worms(id, name,parent_id) VALUES({ecotaxa_id}, '{name}',{new_parent_id_ecotaxa}); ".format(
                        ecotaxa_id=row['ecotaxa_id'], new_id_ecotaxa=row['new_id_ecotaxa'], name=newname,
                        new_parent_id_ecotaxa=row['new_parent_id_ecotaxa'])
            elif row['action'] == 'deprecier':
                qry = "UPDATE taxonomy_worms SET rename_to={new_id_ecotaxa},taxostatus='D' WHERE id={ecotaxa_id};".format(
                    ecotaxa_id=row['ecotaxa_id'], new_id_ecotaxa=row['new_id_ecotaxa'])
            elif row['action'] == 'A supprimer':
                qry = "UPDATE taxonomy_worms SET taxostatus ='C' WHERE id={ecotaxa_id};".format(
                    ecotaxa_id=row['ecotaxa_id'])
            elif row['action'] == 'changer type en Morpho':
                qry = "UPDATE taxonomy_worms SET taxotype='M' WHERE id={ecotaxa_id};".format(
                    ecotaxa_id=row['ecotaxa_id'], name=newname)
            elif row['action'] == 'Ajouter aphia_id':
                qry = "UPDATE taxonomy_worms SET name='{name}',aphia_id={aphia_id} WHERE id={ecotaxa_id};".format(
                    ecotaxa_id=row['ecotaxa_id'], name=newname, aphia_id=row['aphia_id'])
            elif row['action'] == 'Changer le parent':
                if row['details'].strip() == 'Changer le parent':
                    qry = "UPDATE taxonomy_worms SET name='{name}',aphia_id={aphia_id}, parent_id={new_parent_id_ecotaxa} WHERE id={ecotaxa_id};".format(
                        ecotaxa_id=row['ecotaxa_id'], name=newname, aphia_id=row['aphia_id'],
                        new_parent_id_ecotaxa=row['new_parent_id_ecotaxa'])
                elif row['details'].strip() == 'morpho parent deprécié':
                    qry = "UPDATE taxonomy_worms SET name='{name}', parent_id={new_parent_id_ecotaxa} WHERE id={ecotaxa_id};".format(
                        ecotaxa_id=row['ecotaxa_id'], name=newname, new_parent_id_ecotaxa=row['new_parent_id_ecotaxa'])
                elif row['details'].strip() == 'brancher un nouvel ecotaxaid':
                    qry = "UPDATE taxonomy_worms SET name='{name}', aphia_id={aphia_id},parent_id={new_parent_id_ecotaxa} WHERE id={ecotaxa_id};".format(
                        ecotaxa_id=row['ecotaxa_id'], name=newname, new_parent_id_ecotaxa=row['new_parent_id_ecotaxa'],
                        aphia_id=row['aphia_id'])
                elif row['details'].strip() == "Pas de match avec Worms mais rattache plus haut":
                    qry = "UPDATE taxonomy_worms SET name='{name}', parent_id={new_parent_id_ecotaxa} WHERE id={ecotaxa_id};".format(
                        ecotaxa_id=row['ecotaxa_id'], name=newname,
                        new_parent_id_ecotaxa=row['new_parent_id_ecotaxa'])
            elif row['action'] == 'Changer le parent + Pas de match avec Worms mais rattache plus haut' or row[
                'action'] == 'Rien + Pas de match avec Worms mais rattache plus haut':
                qry = "UPDATE taxonomy_worms SET parent_id={new_parent_id_ecotaxa} WHERE id={ecotaxa_id};".format(
                    ecotaxa_id=row['ecotaxa_id'], new_parent_id_ecotaxa=row['new_parent_id_ecotaxa'])
            else:
                qry = ""
                errorfile.write(" no sql defined - " + ",".join(row.values()) + "\n")
            if qry != "":
                result = db.engine.execute(text(qry))
                with open(filesql, wr) as actionfile:
                    actionfile.write(qry + "\n")