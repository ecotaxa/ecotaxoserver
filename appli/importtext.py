# -*- coding: utf-8 -*-
from flask import Blueprint, render_template, g, request,url_for,Response
from flask_login import current_user
from appli import app,ObjectToStr,PrintInCharte,database,db,gvg,gvp,FormatError,FormatSuccess,XSSEscape,ntcv
from appli.database import GetAll,ExecSQL
from appli.services import ComputeDisplayName
import json,re,traceback,datetime
from flask_security.decorators import roles_accepted,login_required
import csv,tempfile
from io import TextIOWrapper

@app.route('/importtext/')
@login_required
@roles_accepted(database.AdministratorLabel)
def importtext():
    return render_template('taxoimport_create.html')

@app.route('/doimporttext/',methods=['POST'])
@login_required
@roles_accepted(database.AdministratorLabel)
def doimporttext():
    # test avec D:\temp\Downloads\taxoexport_20181228_101007.tsv
    txt=""
    uploadfile = request.files.get("uploadfile")
    if uploadfile is None:
        return PrintInCharte(FormatError("You must send a file"))
    Desctxt="{0} on {1:%Y-%m-%d %H:%M:%S} ".format(uploadfile.filename,datetime.datetime.now())
    app.logger.info('Load file {} by {}'.format(uploadfile.filename,current_user))
    #creation d'un fichier temporaire qui s'efface automatiquement
    tmpfile =tempfile.TemporaryFile(mode='w+b')
    # app.logger.info('TMP file is {}'.format(tmpfile.name))
    uploadfile.save(tmpfile) # on copie le contenu dedants
    tmpfile.seek(0) # on se remet au debut
    fichier= TextIOWrapper(tmpfile, encoding = 'latin_1', errors = 'replace') # conversion du format binaire au format texte

    app.logger.info("Analyzing file %s" % (fichier))
    # lecture en mode dictionnaire basé sur la premiere ligne
    rdr = csv.reader(fichier, delimiter='\t', quotechar='"', )
    # lecture la la ligne des titre
    LType = rdr.__next__()
    # Lecture du contenu du fichier
    RowCount = 0
    ExecSQL("truncate table temp_taxo")
    sqlinsert = "INSERT INTO temp_taxo(idparent,idtaxo,name,status,typetaxo) values(%s,%s,%s,%s,%s)"
    for lig in rdr:
        if lig[0].strip() == '':  # Ligne vide
            continue
        database.ExecSQL(sqlinsert, (
        lig[0].strip(), lig[1].strip(), lig[2].replace('+', ' ').replace('_', ' ').strip(), lig[3].strip(),
        lig[4].strip()))
        if RowCount > 0 and RowCount % 1000 == 0:
            app.logger.info("Inserted %s lines" % RowCount)
        RowCount += 1
    app.logger.info("count=%d" % RowCount)

        # app.logger.info(str(r))
    # if len(UpdatedTaxon)>0:
    #     ComputeDisplayName(UpdatedTaxon)
    txt+="<p style='color: green'> %s taxon loaded </p> "%(RowCount)

    # MAJ des IDFinal dans la table temp pour tout ce qui existe.
    n = ExecSQL("""UPDATE temp_taxo tt set idfinal=tf.id
                from taxonomy_worms tf where tf.aphia_id=tt.idtaxo or (lower(tf.name)=lower(tt.name) and tf.aphia_id is null)""")
    app.logger.info("%d Nodes already exists " % n)

    TSVal="to_timestamp('{}','YYYY-MM-DD HH24:MI:SS')".format(datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S'))
    TSUpdate="lastupdate_datetime="+TSVal
    # insertion des nouveaux noeud racines
    n = ExecSQL("""INSERT INTO taxonomy_worms (id,aphia_id, parent_id,rank, name, lastupdate_datetime,source_desc)
    select nextval('seq_taxonomy_worms'),NULL,t.name,t.idtaxo,t.rank,{} ,%s
    from temp_taxo t where idparent='-1' and idfinal is null and status='1'""".format(TSVal),["Created by "+Desctxt])
    app.logger.info("Inserted %d Root Nodes" % n)

    # MAJ de la table import existante
    n = ExecSQL("""UPDATE temp_taxo tt set idfinal=tf.id 
                from taxonomy_worms tf where tf.aphia_id=tt.idtaxo
                and tt.idfinal is null and idparent='-1'""")
    app.logger.info("Updated %d inserted Root Nodes" % n)

    while True:
        # insertion des nouveaux noeud enfants à partir des parents deja insérés
        # n=ExecSQL("""INSERT INTO taxonomy (id, parent_id, name, id_source)
        #     select nextval('seq_taxonomy'),ttp.idfinal,tt.name,tt.idtaxo from temp_taxo tt join temp_taxo ttp on tt.idparent=ttp.idtaxo
        #     where tt.idfinal is null and ttp.idfinal is not null and status='1'""")
        n = ExecSQL("""INSERT INTO taxonomy_worms (id, parent_id, name,aphia_id,rank,taxotype,lastupdate_datetime,source_desc)
            select nextval('seq_taxonomy_worms'),ttp.id,tt.name,tt.idtaxo,tt.rank,case when lower(tt.typetaxo)='taxa' then 'P' else 'M' end,{},%s
            from temp_taxo tt join taxonomy_worms ttp on tt.idparent=ttp.aphia_id
            where tt.idfinal is null and status='1'""".format(TSVal),["Created by "+Desctxt])
        if n == 0:
            app.logger.info("No more data to import")
            break
        else:
            app.logger.info("Inserted %d Child Nodes" % n)

        # MAJ de la table import existante
        n = ExecSQL("""UPDATE temp_taxo tt set idfinal=tf.id
                    from taxonomy_worms tf where tf.aphia_id=tt.idtaxo
                    and tt.idfinal is null """)
        app.logger.info("Updated %d inserted Child Nodes" % n)

    n = ExecSQL("""UPDATE taxonomy_worms tf set name=tt.name,{},taxotype=case when lower(tt.typetaxo)='taxa' then 'P' else 'M' end
                      ,source_desc=%s  
                from temp_taxo tt where tf.aphia_id=tt.idtaxo
                and tt.status='1' and  (tf.name!=tt.name
                  or tf.taxotype!=case when lower(tt.typetaxo)='taxa' then 'P' else 'M' end )
                """.format(TSUpdate),["Updated by "+Desctxt])
    app.logger.info("Updated %d Nodes names" % n)

    n = ExecSQL("""UPDATE taxonomy_worms tfu set parent_id=sq.idfinal,{},source_desc=%s
                from (select tf.id, ttp.idfinal from taxonomy_worms tf
                ,temp_taxo tt LEFT JOIN temp_taxo ttp on tt.idparent=ttp.idtaxo  where tf.aphia_id=tt.idtaxo
                and tt.status='1' and coalesce(tf.parent_id,-1)!=coalesce(ttp.idfinal,-1)
                and (ttp.idfinal is not null or tt.idparent='-1' )) sq where tfu.id=sq.id""".format(TSUpdate),["Updated by "+Desctxt])
    app.logger.info("Updated %d Nodes Parents" % n)

    # while True:
    #     n = ExecSQL("""delete from taxonomy t
    #             using temp_taxo tt
    #             where t.id=tt.idfinal and tt.status='0'
    #             and not exists (select 1 from taxonomy where parent_id=t.id )
    #             and not exists (select 1 from objects where classif_id=t.id or classif_auto_id=t.id)""")
    #     if n == 0:
    #         app.logger.info("No more data to delete")
    #         break
    #     else:
    #         app.logger.info("Deleted %d Nodes" % n)

    # Lst = GetAll("""select t.name from taxonomy t,temp_taxo tt
    #             where t.id=tt.idfinal and tt.status='0'
    #             and (exists (select 1 from taxonomy where parent_id=t.id )
    #             or exists (select 1 from objects where classif_id=t.id or classif_auto_id=t.id))""")
    # for r in Lst:
    #     app.logger.info("Can't Delete '%s' because it's used " % r[0])
    txt+="<br><a href='/browsetaxo/' class='btn btn-primary'><i class='fas fa-arrow-left'></i> Back to taxonomy</a>"
    LstId=[x['idfinal'] for x in GetAll("select idfinal from temp_taxo where idfinal is not null")]
    ComputeDisplayName(LstId)
    app.logger.info("Updated Display name" )

    # if len(Errors):
    #     txt += "<p style='color: red'> %s errors <ul> " % (len(Errors))
    #     txt += "\n".join("<li>%s</li>"%x for x in Errors)
    #     txt += "</ul></p> "

    g.bodydivmargin="10px"
    return PrintInCharte(txt)

