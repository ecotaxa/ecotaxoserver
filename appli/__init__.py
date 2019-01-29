# -*- coding: utf-8 -*-
# This file is part of Ecotaxa, see license.md in the application root directory for license informations.
# Copyright (C) 2015-2016  Picheral, Colin, Irisson (UPMC-CNRS)

import os,sys,pathlib,urllib.parse,html


VaultRootDir=os.path.join(os.path.dirname(os.path.realpath(__file__)), "..","vault")
if not os.path.exists(VaultRootDir):
    os.mkdir(VaultRootDir)
TempTaskDir=os.path.join(os.path.dirname(os.path.realpath(__file__)), "..","temptask")
if not os.path.exists(TempTaskDir):
    os.mkdir(TempTaskDir)

from flask import Flask,render_template,request,g
from flask_sqlalchemy import SQLAlchemy
from flask_security import Security,SQLAlchemyUserDatastore
import inspect,html,math,threading,time,traceback



app = Flask("appli")
app.config.from_pyfile('config.cfg')
app.config['SECURITY_MSG_DISABLED_ACCOUNT']=('Your account is disabled. Email to the User manager (list on the left) to re-activate.','error')
app.logger.setLevel(10)

if 'PYTHONEXECUTABLE' in app.config:
    app.PythonExecutable=app.config['PYTHONEXECUTABLE']
else:
    app.PythonExecutable="TBD"

app.config['SQLALCHEMY_TRACK_MODIFICATIONS']=False
db = SQLAlchemy(app,session_options={'expire_on_commit':True}) # expire_on_commit évite d'avoir des select quand on manipule les objets aprés un commit.

import appli.database
# Setup Flask-Security
user_datastore = SQLAlchemyUserDatastore(db, database.users, database.roles)
security = Security(app, user_datastore)


def ObjectToStr(o):
    return str([(n, v) for n, v in inspect.getmembers(o) if(('method' not in str(v))and  (not inspect.isfunction(v))and  (n!='__module__')and  (n!='__doc__') and  (n!='__dict__') and  (n!='__dir__') and  (n!='__weakref__') )])

def PrintInCharte(txt,title=None):
    """
    Permet d'afficher un texte (qui ne sera pas echapé dans la charte graphique
    :param txt: Texte à affiche
    :return: Texte rendu
    """
    AddTaskSummaryForTemplate()
    module='' # Par defaut c'est Ecotaxa
    if request.path.find('/part')>=0:
        module = 'part'
    if not title:
        if module == 'part':
            title='EcoPart'
        else:
            title = 'EcoTaxa'
    return render_template('layout.html',bodycontent=txt,module=module,title=title)

def ErrorFormat(txt):
    return """
<div class='cell panel ' style='background-color: #f2dede; margin: 15px;'><div class='body' >
				<table style='background-color: #f2dede'><tr><td width='50px' style='color: red;font-size: larger'> <span class='glyphicon glyphicon-exclamation-sign'></span></td>
				<td style='color: red;font-size: larger;vertical-align: middle;'><B>%s</B></td>
				</tr></table></div></div>
    """%(txt)

def AddTaskSummaryForTemplate():
    from flask_login import current_user
    # if getattr(current_user, 'id', -1) > 0:
    #     g.tasksummary = appli.database.GetAssoc2Col(
    #         "SELECT taskstate,count(*) from temp_tasks WHERE owner_id=%(owner_id)s group by taskstate"
    #         , {'owner_id': current_user.id})
    # g.google_analytics_id = app.config.get('GOOGLE_ANALYTICS_ID', '')


def gvg(varname,defvalue=''):
    """
    Permet de récuperer une variable dans la Chaine GET ou de retourner une valeur par defaut
    :param varname: Variable à récuperer
    :param defvalue: Valeur par default
    :return: Chaine de la variable ou valeur par default si elle n'existe pas
    """
    return request.args.get(varname, defvalue)

def gvp(varname,defvalue=''):
    """
    Permet de récuperer une variable dans la Chaine POST ou de retourner une valeur par defaut
    :param varname: Variable à récuperer
    :param defvalue: Valeur par default
    :return: Chaine de la variable ou valeur par default si elle n'existe pas
    """
    return request.form.get(varname, defvalue)

def ntcv(v):
    """
    Permet de récuperer une chaine que la source soit une chaine ou un None issue d'une DB
    :param v: Chaine potentiellement None
    :return: V ou chaine vide
    """
    if v is None:
        return ""
    return v

def nonetoformat(v,fmt :str):
    """
    Permet de faire un formatage qui n'aura lieu que si la donnée n'est pas nulle et permet récuperer une chaine que la source soit une données ou un None issue d'une DB
    :param v: Chaine potentiellement None
    :param fmt: clause de formatage qui va etre générée par {0:fmt}
    :return: V ou chaine vide
    """
    if v is None:
        return ""
    return ("{0:"+fmt+"}").format(v)

def DecodeEqualList(txt):
    res={}
    for l in str(txt).splitlines():
        ls=l.split('=',1)
        if len(ls)==2:
            res[ls[0].strip().lower()]=ls[1].strip().lower()
    return res
def EncodeEqualList(map):
    l=["%s=%s"%(k,v) for k,v in map.items()]
    l.sort()
    return "\n".join(l)

def ScaleForDisplay(v):
    """
    Permet de supprimer les decimales supplementaires des flottant en fonctions de la valeur et de ne rien faire au reste
    :param v: valeur à ajuste
    :return: Texte formaté
    """
    if isinstance(v, (float)):
        if(abs(v)<100):
            return "%0.2f"%(v)
        else: return "%0.f"%(v)
    elif v is None:
        return ""
    else:
        return v

def XSSEscape(txt):
    return html.escape(txt)

def XSSUnEscape(txt):
    return html.unescape(txt)

def FormatError(Msg,*args,DoNotEscape=False,**kwargs):
    caller_frameinfo=inspect.getframeinfo(sys._getframe(1))
    txt = Msg.format(*args, **kwargs)
    app.logger.error("FormatError from {} : {}".format(caller_frameinfo.function,txt))
    if not DoNotEscape:
        Msg=Msg.replace('\n','__BR__')
    txt=Msg.format(*args,**kwargs)
    if not DoNotEscape:
        txt=XSSEscape(txt)
    txt=txt.replace('__BR__','<br>')
    return "<div class='alert alert-danger' role='alert'>{}</div>".format(txt)

def FormatSuccess(Msg,*args,DoNotEscape=False,**kwargs):
    txt=Msg.format(*args,**kwargs)
    if not DoNotEscape:
        txt=XSSEscape(txt)
    if not DoNotEscape:
        Msg=Msg.replace('\n','__BR__')
    txt=Msg.format(*args,**kwargs)
    if not DoNotEscape:
        txt=XSSEscape(txt)
    txt=txt.replace('__BR__','<br>')
    return "<div class='alert alert-success' role='alert'>{}</div>".format(txt)


# def GetAppManagerMailto():
#     if 'APPMANAGER_EMAIL' in app.config and 'APPMANAGER_NAME' in app.config:
#         return "<a href='mailto:{APPMANAGER_EMAIL}'>{APPMANAGER_NAME} ({APPMANAGER_EMAIL})</a>".format(**app.config)
#     return ""


# Ici les imports des modules qui definissent des routes
import appli.main
import appli.adminusers
import appli.adminothers
import appli.dbadmin
import appli.browsetaxo
import appli.browseinstances
import appli.services
import appli.search
import appli.massupdate
import appli.importtext


@app.errorhandler(404)
def not_found(e):
    return render_template("errors/404.html"), 404

@app.errorhandler(403)
def forbidden(e):
    return render_template("errors/403.html"), 403

@app.errorhandler(500)
def internal_server_error(e):
    app.logger.exception(e)
    return render_template("errors/500.html"), 500

@app.errorhandler(Exception)
def unhandled_exception(e):
    # Ceci est imperatif si on veux pouvoir avoir des messages d'erreurs à l'écran sous apache
    app.logger.exception(e)
    # Ajout des informations d'exception dans le template custom
    tb_list = traceback.format_tb(e.__traceback__)
    s = "<b>Error:</b> %s <br><b>Description: </b>%s \n<b>Traceback:</b>" % (html.escape(str(e.__class__)), html.escape(str(e)))
    for i in tb_list[::-1]:
        s += "\n" + html.escape(i)
    db.session.rollback()
    return render_template('errors/500.html' ,trace=s), 500

def JinjaFormatDateTime(d,format='%Y-%m-%d %H:%M:%S'):
    if d is None:
        return ""
    return d.strftime(format)

def JinjaNl2BR(t):
    return t.replace('\n', '<br>\n');

def JinjaGetManagerList(sujet=""):
    LstUsers=database.GetAll("""select distinct u.email,u.name,Lower(u.name)
FROM users_roles ur join users u on ur.user_id=u.id
where ur.role_id=2
and u.active=TRUE and email like '%@%'
order by Lower(u.name)""")
    if sujet:
        sujet="?"+urllib.parse.urlencode({"subject":sujet}).replace('+','%20')
    return " ".join(["<li><a href='mailto:{1}{0}'>{2} ({1})</a></li> ".format(sujet,*r) for r in LstUsers ])

def IsAdmin():
    from flask_login import current_user
    return current_user.has_role(database.AdministratorLabel)

app.jinja_env.filters['datetime'] = JinjaFormatDateTime
app.jinja_env.filters['nl2br'] = JinjaNl2BR
app.jinja_env.globals.update(GetManagerList=JinjaGetManagerList)

app.jinja_env.globals.update(IsAdmin=IsAdmin)
