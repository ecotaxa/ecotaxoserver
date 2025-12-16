# -*- coding: utf-8 -*-
# This file is part of Ecotaxa, see license.md in the application root directory for license informations.
# Copyright (C) 2015-2016  Picheral, Colin, Irisson (UPMC-CNRS)
from appli import db,app,g
from flask_security import  UserMixin, RoleMixin
from sqlalchemy.dialects.postgresql import VARCHAR,INTEGER,CHAR,TIMESTAMP
from sqlalchemy import Index,func
import psycopg2.extras,datetime

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
    __tablename__ = 'taxonomy_worms'
    id  = db.Column(INTEGER,db.Sequence('seq_taxonomy_worms'), primary_key=True)
    aphia_id  = db.Column(INTEGER)
    rank = db.Column(VARCHAR(24))
    parent_id  = db.Column(INTEGER)
    name   = db.Column(VARCHAR(100),nullable=False)
    taxotype   = db.Column(CHAR(1),nullable=False,server_default='P') # P = Phylo , M = Morpho
    display_name = db.Column(VARCHAR(200))
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
Index('IS_TaxonomyAphiaID',Taxonomy.__table__.c.aphia_id)
Index('IS_TaxonomyNameLow',func.lower(Taxonomy.__table__.c.name))
Index('is_taxo_parent_name', Taxonomy.__table__.c.parent_id, Taxonomy.__table__.c.name, unique=True)


class EcotaxaInst(db.Model):
    __tablename__ = 'ecotaxainst'
    id  = db.Column(INTEGER,db.Sequence('seq_ecotaxainst'), primary_key=True)
    name   = db.Column(VARCHAR(100),nullable=False)
    url   = db.Column(VARCHAR(100))
    laststatupdate_datetime = db.Column(TIMESTAMP(precision=0))
    ecotaxa_version = db.Column(VARCHAR(10))
    sharedsecret = db.Column(VARCHAR(100),nullable=False)

class EcotaxaInstStat(db.Model):
    __tablename__ = 'ecotaxainststat'
    id_instance  = db.Column(INTEGER, primary_key=True)
    id_taxon  = db.Column(INTEGER, primary_key=True)
    nbr  = db.Column(INTEGER,nullable=False)

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



