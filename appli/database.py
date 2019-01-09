# -*- coding: utf-8 -*-
# This file is part of Ecotaxa, see license.md in the application root directory for license informations.
# Copyright (C) 2015-2016  Picheral, Colin, Irisson (UPMC-CNRS)
from appli import db,app,g
from flask_security import  UserMixin, RoleMixin
from flask_login import current_user
from sqlalchemy.dialects.postgresql import BIGINT,FLOAT,VARCHAR,DATE,TIME,DOUBLE_PRECISION,INTEGER,CHAR,TIMESTAMP,REAL
from sqlalchemy import Index,Sequence,func
from sqlalchemy.exc import DBAPIError
from sqlalchemy.orm import foreign,remote
import json,psycopg2.extras,datetime,os,time

AdministratorLabel="Application Administrator"

ClassifQual={'P':'predicted','D':'dubious','V':'validated'}
TaxoType={'P':'Phylo','M':'Morpho'}
TaxoStatus={'A' : 'Active', 'D' : 'Deprecated' , 'N' : 'Not reviewed'}

users_roles = db.Table('users_roles',
        db.Column('user_id', db.Integer(), db.ForeignKey('users.id'), primary_key=True),
        db.Column('role_id', db.Integer(), db.ForeignKey('roles.id'), primary_key=True))

class roles(db.Model, RoleMixin ):
    id = db.Column(db.Integer(), primary_key=True)  #,db.Sequence('seq_roles')
    name = db.Column(db.String(80), unique=True,nullable=False)
    def __str__(self):
        return self.name

class users(db.Model, UserMixin):
    id = db.Column(db.Integer,db.Sequence('seq_users'), primary_key=True)
    email = db.Column(db.String(255), unique=True,nullable=False)
    password = db.Column(db.String(255))
    name = db.Column(db.String(255),nullable=False)
    organisation = db.Column(db.String(255))
    active = db.Column(db.Boolean(),default=True)
    roles = db.relationship('roles', secondary=users_roles,
                            backref=db.backref('users', lazy='dynamic')) #
    def __str__(self):
        return "{0} ({1})".format(self.name,self.email)

class Taxonomy(db.Model):
    __tablename__ = 'taxonomy'
    id  = db.Column(INTEGER,db.Sequence('seq_taxonomy'), primary_key=True)
    parent_id  = db.Column(INTEGER)
    name   = db.Column(VARCHAR(100),nullable=False)
    taxotype   = db.Column(CHAR(1),nullable=False,server_default='P') # P = Phylo , M = Morpho
    display_name = db.Column(VARCHAR(200))
    id_source  = db.Column(VARCHAR(20))
    source_url = db.Column(VARCHAR(200))
    source_desc = db.Column(VARCHAR(1000))
    creator_email = db.Column(VARCHAR(255))
    creation_datetime = db.Column(TIMESTAMP(precision=0))
    lastupdate_datetime = db.Column(TIMESTAMP(precision=0))
    id_instance=db.Column(INTEGER)
    taxostatus = db.Column(CHAR(1),nullable=False,server_default='A')
    rename_to=db.Column(INTEGER)
    nbrobj  = db.Column(INTEGER)
    nbrobjcum  = db.Column(INTEGER)
    def __str__(self):
        return "{0} ({1})".format(self.name,self.id)
Index('IS_TaxonomyParent',Taxonomy.__table__.c.parent_id)
Index('IS_TaxonomySource',Taxonomy.__table__.c.id_source)
Index('IS_TaxonomyNameLow',func.lower(Taxonomy.__table__.c.name))


class EcotaxaInst(db.Model):
    __tablename__ = 'ecotaxainst'
    id  = db.Column(INTEGER,db.Sequence('seq_ecotaxainst'), primary_key=True)
    name   = db.Column(VARCHAR(100),nullable=False)
    url   = db.Column(VARCHAR(100))
    sharedsecret = db.Column(VARCHAR(100),nullable=False)


class Projects(db.Model):
    __tablename__ = 'projects'
    projid  = db.Column(INTEGER,db.Sequence('seq_projects'), primary_key=True)
    title   = db.Column(VARCHAR(255),nullable=False)
    visible = db.Column(db.Boolean(),default=True)
    status = db.Column(VARCHAR(40),default="Annotate") # Annotate, ExploreOnly, Annotate No Prediction
    mappingobj   = db.Column(VARCHAR)
    mappingsample   = db.Column(VARCHAR)
    mappingacq   = db.Column(VARCHAR)
    mappingprocess   = db.Column(VARCHAR)
    objcount = db.Column(DOUBLE_PRECISION)
    pctvalidated = db.Column(DOUBLE_PRECISION)
    pctclassified = db.Column(DOUBLE_PRECISION)
    classifsettings  = db.Column(VARCHAR) # Settings for Automatic classification.
    initclassiflist  = db.Column(VARCHAR) # Initial list of categories
    classiffieldlist  = db.Column(VARCHAR) # Fields available on sort & displayed field of Manual classif screen
    popoverfieldlist  = db.Column(VARCHAR) # Fields available on popover of Manual classif screen
    comments  = db.Column(VARCHAR)
    projtype  = db.Column(VARCHAR(50))
    fileloaded  = db.Column(VARCHAR)
    rf_models_used = db.Column(VARCHAR)
    cnn_network_id=db.Column(VARCHAR(50))

    def __str__(self):
        return "{0} ({1})".format(self.title,self.projid)

class ProjectsTaxoStat(db.Model):
    __tablename__ = 'projects_taxo_stat'
    projid = db.Column(INTEGER,db.ForeignKey('projects.projid',ondelete="CASCADE"), primary_key=True)
    id  = db.Column(INTEGER, primary_key=True)
    nbr  = db.Column(INTEGER)
    nbr_v  = db.Column(INTEGER)
    nbr_d  = db.Column(INTEGER)
    nbr_p  = db.Column(INTEGER)

class TempTaxo(db.Model):
    __tablename__ = 'temp_taxo'
    idtaxo = db.Column(VARCHAR(20), primary_key=True)
    idparent = db.Column(VARCHAR(20))
    name = db.Column(VARCHAR(100))
    status = db.Column(CHAR(1))
    typetaxo = db.Column(VARCHAR(20))
    idfinal = db.Column(INTEGER)
Index('IS_TempTaxoParent',TempTaxo.__table__.c.idparent)
Index('IS_TempTaxoIdFinal',TempTaxo.__table__.c.idfinal)

GlobalDebugSQL=False
GlobalDebugSQL=True
def GetAssoc(sql,params=None,debug=False,cursor_factory=psycopg2.extras.DictCursor,keyid=0):
    if 'db' not in g or g.db is None:
        g.db=db.engine.raw_connection()
    cur = g.db.cursor(cursor_factory=cursor_factory)
    # cur = db.engine.raw_connection().cursor(cursor_factory=cursor_factory)
    try:
        starttime=datetime.datetime.now()
        cur.execute(sql,params)
        res=dict()
        for r in cur:
            res[r[keyid]]=r
    except psycopg2.InterfaceError:
        app.logger.debug("Connection was invalidated!, Try to reconnect for next HTTP request")
        db.engine.connect()
        raise
    except:
        app.logger.debug("GetAssoc Exception SQL = %s %s",sql,params)
        cur.connection.rollback()
        raise
    finally:
        if debug or GlobalDebugSQL:
            app.logger.debug("GetAssoc (%s) SQL = %s %s",(datetime.datetime.now()-starttime).total_seconds(),sql,params)
        cur.close()
    return res

def GetAssoc2Col(sql,params=None,debug=False,dicttype=dict):
    if 'db' not in g or g.db is None:
        g.db=db.engine.raw_connection()
    cur = g.db.cursor()
    # cur = db.engine.raw_connection().cursor()
    try:
        starttime=datetime.datetime.now()
        cur.execute(sql,params)
        res=dicttype()
        for r in cur:
            res[r[0]]=r[1]
    except psycopg2.InterfaceError:
        app.logger.debug("Connection was invalidated!, Try to reconnect for next HTTP request")
        db.engine.connect()
        raise
    except:
        app.logger.debug("GetAssoc2Col  Exception SQL = %s %s",sql,params)
        cur.connection.rollback()
        raise
    finally:
        if debug or GlobalDebugSQL:
            app.logger.debug("GetAssoc2Col (%s) SQL = %s %s",(datetime.datetime.now()-starttime).total_seconds(),sql,params)
        cur.close()
    return res

# Les parametres doivent être passés au format (%s)
def GetAll(sql,params=None,debug=False,cursor_factory=psycopg2.extras.DictCursor):
    if 'db' not in g or g.db is None:
        g.db=db.engine.raw_connection()
    cur = g.db.cursor(cursor_factory=cursor_factory)
    # cur = db.engine.raw_connection().cursor(cursor_factory=cursor_factory)
    try:
        starttime=datetime.datetime.now()
        cur.execute(sql,params)
        res = cur.fetchall()
    except psycopg2.InterfaceError:
        app.logger.debug("Connection was invalidated!, Try to reconnect for next HTTP request")
        db.engine.connect()
        raise
    except:
        app.logger.debug("GetAll Exception SQL = %s %s",sql,params)
        cur.connection.rollback()
        raise
    finally:
        if debug or GlobalDebugSQL:
            app.logger.debug("GetAll (%s) SQL = %s %s",(datetime.datetime.now()-starttime).total_seconds(),sql,params)
        cur.close()
    return res

def ExecSQL(sql,params=None,debug=False):
    if 'db' not in g or g.db is None:
        g.db=db.engine.raw_connection()
    cur = g.db.cursor()
    # cur = db.engine.raw_connection().cursor()
    try:
        starttime=datetime.datetime.now()
        cur.execute(sql,params)
        LastRowCount=cur.rowcount
        cur.connection.commit()
    except psycopg2.InterfaceError:
        app.logger.debug("Connection was invalidated!, Try to reconnect for next HTTP request")
        db.engine.connect()
        raise
    except:
        app.logger.debug("ExecSQL Exception SQL = %s %s",sql,params)
        cur.connection.rollback()
        raise
    finally:
        if debug or GlobalDebugSQL:
            app.logger.debug("ExecSQL (%s) SQL = %s %s",(datetime.datetime.now()-starttime).total_seconds(),sql,params)
        cur.close()
    return LastRowCount

def GetDBToolsDir():
    toolsdir=app.config['DB_TOOLSDIR']
    if len(toolsdir)>0:
        if toolsdir[0]=='.': # si chemin relatif on calcule le path absolu par rapport à la racine de l'appli
            toolsdir=os.path.join( os.path.dirname(os.path.realpath(__file__)),"..",toolsdir)
            toolsdir=os.path.normpath(toolsdir)
    return toolsdir

def CSVIntStringToInClause(InStr):
    if InStr is None:
        return ""
    return ",".join([str(int(x)) for x in InStr.split(',')])


def GetTaxoNameFromIdList(IdList):
    sql = """SELECT tf.id, tf.name||case when p1.name is not null and tf.name not like '%% %%'  then ' ('||p1.name||')' else ' ' end as name
             FROM taxonomy tf
            left join taxonomy p1 on tf.parent_id=p1.id
            WHERE  tf.id = any (%s) 
            order by tf.name """
    return GetAll(sql,[IdList])
