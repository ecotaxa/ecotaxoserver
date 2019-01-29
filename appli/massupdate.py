# -*- coding: utf-8 -*-
from flask import Blueprint, render_template, g, request,url_for,Response
from flask_login import current_user
from appli import app,ObjectToStr,PrintInCharte,database,db,gvg,gvp,FormatError,FormatSuccess,XSSEscape,ntcv
from appli.database import GetAll
from appli.services import ComputeDisplayName
import json,re,traceback,datetime
from flask_security.decorators import roles_accepted,login_required
import csv,tempfile
from io import TextIOWrapper

@app.route('/importmassupdate/')
@login_required
@roles_accepted(database.AdministratorLabel)
def importmassupdate():
    return render_template('importmassupdate.html')

@app.route('/doimportmassupdate/',methods=['POST'])
@login_required
@roles_accepted(database.AdministratorLabel)
def doimportmassupdate():
    # test avec D:\temp\Downloads\taxoexport_20181228_101007.tsv
    txt=""
    uploadfile = request.files.get("fichier")
    if uploadfile is None:
        return PrintInCharte(FormatError("You must send a file"))
    app.logger.info('Load file {} by {}'.format(uploadfile.filename,current_user))
    #creation d'un fichier temporaire qui s'efface automatiquement
    tmpfile =tempfile.TemporaryFile(mode='w+b')
    # app.logger.info('TMP file is {}'.format(tmpfile.name))
    uploadfile.save(tmpfile) # on copie le contenu dedants
    tmpfile.seek(0) # on se remet au debut
    f= TextIOWrapper(tmpfile, encoding = 'ascii', errors = 'replace') # conversion du format binaire au format texte
    rdr = csv.DictReader(f, delimiter='\t', quotechar='"', ) # ouverture sous forme de reader dictionnaire
    champs=rdr.fieldnames
    app.logger.info("Loading file with this columns : %s"%str(champs))
    if 'id' not in champs:
        return PrintInCharte(FormatError("A column named 'id' is required"))
    IdList={x['id'] for x in database.GetAll("select id from taxonomy")}
    InstanceList={x['id'] for x in database.GetAll("select id from ecotaxainst")}
    rowcount=0
    Errors=[]
    UpdatedTaxon=[]
    UpdatableCols=['parent_id', 'name', 'taxotype', 'taxostatus', 'id_source', 'source_url', 'source_desc', 'creator_email', 'creation_datetime', 'id_instance', 'rename_to']
    # 'lastupdate_datetime', 'display_name',
    for r in rdr:
        rowcount+=1
        id=int(r['id'])
        taxon=database.Taxonomy.query.filter_by(id=id).first()
        if taxon is None:
            Errors.append("id {} does not exists in the database".format(id))
            continue
        valueschanged=False
        SkipRow=False
        for c in champs:
            if c in UpdatableCols :
                oldvalue=str(ntcv(getattr(taxon,c))).replace('\r','')
                newvalue=r[c].replace("\\n","\n").strip()
                if c in ('parent_id','rename_to') and newvalue!='':
                    if int(newvalue) not in IdList:
                        Errors.append("id {} : {} {} does not exists in the database".format(id,c,newvalue))
                        SkipRow = True
                        continue
                if c =='taxotype':
                    if newvalue not in ('P','M'):
                        Errors.append("id {} : Invalid taxotype {} ".format(id,newvalue))
                        SkipRow = True
                        continue
                if c =='taxostatus':
                    if newvalue not in database.TaxoStatus:
                        Errors.append("id {} : Invalid status {} ".format(id,newvalue))
                        SkipRow=True
                        continue
                if c =='id_instance' and newvalue!='':
                    if int(newvalue) not in InstanceList:
                        Errors.append("id {} : {} is not a valid instance id".format(id, newvalue))
                        SkipRow = True
                        continue
                if oldvalue!=newvalue :
                    valueschanged=True
                    setattr(taxon, c,newvalue)
                    app.logger.info("id {} : update {} to {}".format(id,oldvalue,newvalue))
        if SkipRow:
            continue
        if valueschanged:
            # db.session.add(taxon)
            UpdatedTaxon.append(id)
            taxon.lastupdate_datetime=datetime.datetime.utcnow()
            db.session.commit()
        # app.logger.info(str(r))
    if len(UpdatedTaxon)>0:
        ComputeDisplayName(UpdatedTaxon)
    txt+="<p style='color: green'> %s taxon updated </p> "%(len(UpdatedTaxon))
    if len(Errors):
        txt += "<p style='color: red'> %s errors <ul> " % (len(Errors))
        txt += "\n".join("<li>%s</li>"%x for x in Errors)
        txt += "</ul></p> "
    txt += "<a href='/browsetaxo/' class='btn btn-primary'><i class='fas fa-arrow-left'></i> Back to Browse Taxonomy</a>"
    g.bodydivmargin="10px"
    return PrintInCharte(txt)

