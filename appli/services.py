from flask import Blueprint, render_template, g, request,url_for,abort
from flask.json import jsonify
from flask_login import current_user
from appli import app,ObjectToStr,PrintInCharte,database,db,gvg,gvp
from flask_security.decorators import roles_accepted
import json,re,time,psycopg2.extras,random,datetime
from typing import List

def ComputeDisplayName(TaxoList:List[int]):
    """
    Compute display_name column in database, for the list of provided id
    :param TaxoList: 
    :return: None 
    """
    sql="""with nt as (select t.id,case when t.name like '%% %%' or p.id is null then t.name
                    when t.taxotype='M' and P.taxotype='M' then concat(p2.name||'>',p.name||'>',t.name)
                    else concat(p.name||'>',t.name) end newname
  from taxonomy t
  left JOIN taxonomy p on t.parent_id=p.id
  left JOIN taxonomy p2 on p.parent_id=p2.id
  where (t.id = any ( %(taxo)s ) or p.id = any ( %(taxo)s ) or p2.id = any ( %(taxo)s ))
)
update public.taxonomy t set display_name=newname,lastupdate_datetime=to_timestamp(%(ts)s,'YYYY-MM-DD HH24:MI:SS')
from nt 
where nt.id=t.id and display_name IS DISTINCT FROM newname """
    database.ExecSQL(sql,{'taxo':TaxoList,'ts':datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')})


def checktaxon(taxotype:str,name:str,parent='',updatetarget=''):
    """
    Check if a Taxon name follows rules
    :param taxotype: Taxon Type P or M
    :param name:  Taxon Name
    :param parent:  Parent ID
    :param updatetarget: ID of the updated taxon, used to avoid duplicate error when you update yourself
    :return: "ok" or Error message
    """
    if len(name)<3:
        return "Name too short 3 characters min."
    if parent:
        if len(database.GetAll("select id from taxonomy where id=%s",[parent]))!=1:
            return "invalid parent, doesn't exists in database"
    if taxotype == 'P' : # Phylo
        if not re.match(r"^[A-Z][a-z+\-']{2,} ?[a-z+\-']*$", name):
            return "Must contains only letters and -+' and not more than 1 whitespace, Start with a Uppercase"
        sql="select count(*) from taxonomy where lower(name)=lower( (%s) ) "
        if updatetarget and int(updatetarget)>0 :
            sql +=" and id!={}".format(int(updatetarget))
        Nbr=int(database.GetAll(sql,[name])[0][0])
        if Nbr!=0:
            return "duplicate name"
    elif taxotype == 'M' : # Morpho
        if not re.match(r"^[a-z+\-']{3,} ?[a-z+\-']*$", name):
            return "Must contains only letters and -+' and not more than 1 whitespace, be lowercase only and"
        if not parent:
            return "You must specify a parent to check morpho type"
        sql="select count(*) from taxonomy where lower(name)=lower( (%s) ) and parent_id={}".format(int(parent))
        if updatetarget and int(updatetarget)>0 :
            sql +=" and id!={}".format(int(updatetarget))
        Nbr=int(database.GetAll(sql,[name])[0][0])
        if Nbr!=0:
            return "duplicate child for this parent"
    else:
        return "Invalid taxotype"
    return "ok"

@app.route('/checktaxon/',methods=['POST'])
def routechecktaxon():
    CheckInstanceSecurity()
    taxotype=gvp('taxotype')
    name=gvp('name')
    parent= gvp('parent')
    updatetarget = gvp('updatetarget')
    res={"msg":checktaxon(taxotype,name,parent=parent,updatetarget=updatetarget)}
    return json.dumps(res)


class TaxoOperationLocker:
    def __init__(self):
        database.ExecSQL("select 1") # initialize g.db
        self.cur = g.db.cursor()

    def __enter__(self):
        self.cur.execute("lock table taxonomy in SHARE UPDATE EXCLUSIVE mode nowait")
        # app.logger.info('Got Lock')

    def __exit__(self, typeparam, value, traceback):
        self.cur.connection.commit()

def CheckInstanceSecurity():
    id_instance=gvp('id_instance','')
    if not id_instance.isdigit():
        abort(jsonify(msg="invalid instance id format"))
    Instance=database.GetAll("select sharedsecret from ecotaxainst WHERE id=%s",[int(id_instance)])
    if len(Instance)!=1:
        abort(jsonify(msg="invalid instance"))
    if Instance[0]['sharedsecret']!=gvp('sharedsecret'):
        time.sleep(random.random())
        abort(jsonify(msg="invalid instance secret"))

def CheckTaxonUpdateRight(Taxon):
    if Taxon is None:
        abort(jsonify(msg="unable to update a non existing id"))
    if Taxon.taxostatus!='N':
        abort(jsonify(msg="unable to update a validated taxon"))
    if Taxon.id_instance != int(gvp('id_instance')):
        abort(jsonify(msg="Only instance author of a taxon can update it"))

def UpdateObjectFromForm(taxon):
    taxon.parent_id= gvp('parent_id')
    taxon.name= gvp('name')
    taxon.taxotype= gvp('taxotype')
    taxon.id_source= gvp('id_source')
    taxon.source_url= gvp('source_url')
    taxon.source_desc= gvp('source_desc')
    taxon.creator_email= gvp('creator_email')
    if taxon.taxostatus is None:
        taxon.taxostatus = 'N'
    elif taxon.taxostatus != 'N':
        abort(jsonify(msg="Only non validated status can be updated"))
    elif taxon.id_instance != int(gvp('id_instance')):
        abort(jsonify(msg="Only instance author of a taxon can update it"))
    if taxon.id is None:
        taxon.creation_datetime = datetime.datetime.utcnow()
        taxon.id_instance = int(gvp('id_instance'))
    taxon.lastupdate_datetime= datetime.datetime.utcnow()
    if gvp('rename_to')!='':
        if len(database.GetAll("select id from taxonomy where id=%s",[int(gvp('rename_to'))]))!=1:
            abort(jsonify(msg="invalid rename_to value"))
    taxon.rename_to= gvp('rename_to') or None

@app.route('/settaxon/',methods=['POST'])
def routesettaxon():
    CheckInstanceSecurity()
    taxotype=gvp('taxotype')
    name=gvp('name')
    parent= gvp('parent')
    taxonid = gvp('id','')

    with TaxoOperationLocker():
        app.logger.info('In Locker')
        msg=checktaxon(taxotype, name, parent=parent,updatetarget=taxonid)
        if msg!='ok':
            return jsonify(msg=msg)
        if taxonid!='':
            Taxon = database.Taxonomy.query.filter_by(id=int(taxonid)).first()
            CheckTaxonUpdateRight(Taxon)
        else:
            Taxon=database.Taxonomy()
            db.session.add(Taxon)
        UpdateObjectFromForm(Taxon)
        db.session.commit()
        ComputeDisplayName([Taxon.id])
        return jsonify(msg='ok',id=Taxon.id)

@app.route('/deltaxon/',methods=['POST'])
def routedeltaxon():
    CheckInstanceSecurity()
    with TaxoOperationLocker():
        Taxon = database.Taxonomy.query.filter_by(id=int(gvp('id'))).first()
        CheckTaxonUpdateRight(Taxon)
        db.session.delete(Taxon)
        db.session.commit()
    return jsonify(msg='ok')


@app.route('/gettaxon/',methods=['POST'])
def routegettaxon():
    sql="""select t.id,t.parent_id,t.name,t.taxotype,t.display_name,t.id_source,t.source_url,t.source_desc
          ,t.creator_email
          ,to_char(t.creation_datetime,'YYYY-MM-DD HH24:MI:SS') creation_datetime
          ,to_char(t.lastupdate_datetime,'YYYY-MM-DD HH24:MI:SS') lastupdate_datetime,t.id_instance,t.taxostatus,t.rename_to 
          from taxonomy t where 1=1 """
    sqlparam={}
    filtertype = gvp('filtertype')
    if filtertype=='id':
        sql+=" and id=%(id)s "
        sqlparam['id']=int(gvp('id'))
    elif filtertype=='since':
        sql+=""" and lastupdate_datetime>=to_timestamp(%(startdate)s,'YYYY-MM-DD HH24:MI:SS') 
                 and (taxostatus!='N' or id_instance=%(id_instance)s)   
                    """
        sqlparam['startdate']=gvp('startdate')
        sqlparam['id_instance']=gvp('id_instance')
    else:
        return jsonify(msg='filtertype required')
    sql += " order by lastupdate_datetime "
    res=database.GetAll(sql,sqlparam,cursor_factory=psycopg2.extras.RealDictCursor)
    if len(res)==0 and filtertype=='id':
        return jsonify(msg='no data found')
    return jsonify(res)