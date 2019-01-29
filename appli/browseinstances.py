# -*- coding: utf-8 -*-
from flask import Blueprint, render_template, g, request,url_for,Response
from flask_login import current_user
from appli import app,ObjectToStr,PrintInCharte,database,db,gvg,gvp,FormatError,FormatSuccess,XSSEscape,ntcv
from appli.database import GetAll
import json,re,traceback,datetime
from appli.services import ComputeDisplayName
from flask_security.decorators import roles_accepted,login_required


@app.route('/browseinstance/')
def browseinstance():
    lst=GetAll("""select id,name,url,to_char(laststatupdate_datetime,'yyyy-mm-dd hh24:mi') laststatupdate_datetime,ecotaxa_version
    from ecotaxainst
    order by id    
    """)

    return render_template('browseinstance.html',lst=lst)


@app.route('/browseinstanceeditpopup/<string:instanceid>')
@login_required
@roles_accepted(database.AdministratorLabel)
def browseinstanceeditpopup(instanceid):
    if instanceid=='new':
        instance = {'id':'new'}
    else:
        sql = """select i.*
            from ecotaxainst i 
            where i.id = %(id)s
            """
        instance= GetAll(sql,{'id':instanceid})[0]
    return render_template('browseinstanceeditpopup.html', instance=instance)

@app.route('/browseinstancesavepopup/',methods=['POST'])
@login_required
@roles_accepted(database.AdministratorLabel)
def browseinstancesavepopup():
    try:
        # txt = json.dumps(request.form)
        instance_id=gvp('id')
        if instance_id!='new':
            instance=database.EcotaxaInst.query.filter_by(id=instance_id).first()
            if instance is None:
                raise Exception("Instance not found in Database")
        else:
            instance = database.EcotaxaInst()
            db.session.add(instance)
        instance.url=gvp('url')
        instance.name = gvp('name')
        instance.sharedsecret = gvp('sharedsecret')
        db.session.commit()
        return "<script>location.reload(true);</script>"
    except Exception as e:
        tb_list = traceback.format_tb(e.__traceback__)
        return FormatError("Saving Error : {}\n{}",e,"__BR__".join(tb_list[::-1]))

@app.route('/browseinstancedelpopup/',methods=['POST'])
@login_required
@roles_accepted(database.AdministratorLabel)
def browseinstancedelpopup():
    try:
        instance_id=gvp('id')
        instance=database.EcotaxaInst.query.filter_by(id=instance_id).first()
        if instance is None:
            raise Exception("Instance not found in Database")
        database.ExecSQL("delete from ecotaxainststat where id_instance=%s",[instance.id])
        db.session.delete(instance)
        db.session.commit()
        return "<script>location.reload(true);</script>"
    except Exception as e:
        tb_list = traceback.format_tb(e.__traceback__)
        return FormatError("Saving Error : {}\n{}",e,"__BR__".join(tb_list[::-1]))
