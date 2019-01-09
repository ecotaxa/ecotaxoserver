# -*- coding: utf-8 -*-
# This file is part of Ecotaxa, see license.md in the application root directory for license informations.
# Copyright (C) 2015-2016  Picheral, Colin, Irisson (UPMC-CNRS)
from flask import Blueprint, render_template, g, request,url_for
from flask_login import current_user
from appli import app,ObjectToStr,PrintInCharte,database,db,gvg,ntcv
from appli.database import  GetAll
from psycopg2.extensions import QuotedString
from flask_security.decorators import roles_accepted
import os,json


@app.route('/')
def index():
    txt="""<div style='margin:5px;'><div id="homeText"'>"""
    #lecture du message de l'application manager
    NomFichier='appli/static/home/appmanagermsg.html'
    if os.path.exists(NomFichier):
        with open(NomFichier, 'r',encoding='utf8') as f:
            message=f.read()
            if len(message)>5:
                txt+="""
                    <div class="alert alert-warning alert-dismissable" role="alert">
                        <button type="button" class="close" data-dismiss="alert" aria-label="Close"><span aria-hidden="true">&times;</span></button>
                        <p><strong>Message from the application manager</strong></p>{0}
                    </div>
                """.format(message)
    # Lecture de la partie Haute
    NomFichier='appli/static/home/home.html'
    if not os.path.exists(NomFichier):
        NomFichier='appli/static/home/home-model.html'
    with open(NomFichier, 'r',encoding='utf8') as f:
        txt+=f.read()
    # txt+="""<br><a href='/privacy'>Privacy</a></div></div></div>"""
    return PrintInCharte(txt)

@app.before_request
def before_request_security():
    # time.sleep(0.1)
    # print("URL="+request.url)
    # app.logger.info("URL="+request.url)
    # g.db=None
    if "/static" in request.url:
        return
    # print(request.form)
    # current_user.is_authenticated
    g.cookieGAOK = request.cookies.get('GAOK', '')
    g.menu = []
    g.menu.append((url_for("index"),"Home"))
    g.menu.append(("/browsetaxo/","Browse Taxonomy"))
    g.menu.append(("/browseinstance/","Browse Intances"))
    # if current_user.is_authenticated:
    #     g.menu.append(("/part/prj/","Particle projects management"))
    g.useradmin=False
    g.appliadmin=False
    if current_user.has_role(database.AdministratorLabel) :
        g.menu.append(("/importmassupdate/", "Import Mass Update"))
        g.menu.append(("/importtext/","Import Taxonomy"))
        # g.menu.append(("/Task/Create/TaskExportDb","Export Database"))
        # g.menu.append(("/Task/Create/TaskImportDB","Import Database"))
        # g.menu.append(("/Task/listall","Task Manager"))
        g.appliadmin=True

    g.menu.append(("","SEP"))
    g.menu.append(("/change","Change Password"))

@app.teardown_appcontext
def before_teardown_commitdb(error):
    try:
        if 'db' in g:
            try:
                g.db.commit()
            except:
                g.db.rollback()
    except Exception as e: # si probleme d'accés à g.db ou d'operation sur la transaction on passe silencieusement
        app.logger.error("before_teardown_commitdb : Unhandled exception (can be safely ignored) : {0} ".format(e))

@app.after_request
def after_request(response):
    response.headers[
        'Content-Security-Policy'] = "default-src 'self' 'unsafe-inline' 'unsafe-eval' blob: data: cdnjs.cloudflare.com server.arcgisonline.com www.google.com www.gstatic.com www.google-analytics.com cdn.ckeditor.com;frame-ancestors 'self';form-action 'self';"
    return response


