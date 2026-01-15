from datetime import datetime
from typing import Dict,List, Any
from flask import  g,abort,make_response
from flask.json import jsonify
from appli import app,database,db,gvp,FormatSuccess,FormatError,ntcv
from appli.WoRMS import WoRMSFinder
from appli.database import ExecSQL, Taxonomy
import json,re,time,psycopg2.extras,random,datetime
WORMS_STATUS_ACCEPTED ='accepted'
DEFAULT_WORMS_STATUS='A'
DEFAULT_WORMS_TYPE='P'
WORMS_URL = "https://www.marinespecies.org/aphia.php?p=taxdetails&id="
def ComputeDisplayName(TaxoList:list):
    """
    Compute display_name column in database, for the list of provided id
    :param TaxoList: 
    :return: None 
    """
    sql: str = """with duplicate as (select lower(name) as name from taxonomy_worms GROUP BY lower(name) HAVING count(*)>1)
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
    Duplicates = database.GetAll(sql, {'taxo':TaxoList},cursor_factory=psycopg2.extras.RealDictCursor)
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

    app.logger.debug("Compute time %s ", (datetime.datetime.now() - starttime).total_seconds())
    starttime = datetime.datetime.now()
    UpdateParam = []
    for D in Duplicates:
        if D['display_name'] != D['newname']:
            UpdateParam.append((int(D['id']), D['newname']))
    if len(UpdateParam) > 0:
        cur = g.db.cursor()
        psycopg2.extras.execute_values(cur
                                       , """UPDATE taxonomy_worms SET display_name = data.pdisplay_name,lastupdate_datetime=to_timestamp('{}','YYYY-MM-DD HH24:MI:SS') 
               FROM (VALUES %s) AS data (pid, pdisplay_name)
               WHERE id = data.pid""".format(datetime.datetime.now(datetime.timezone.utc).strftime('%Y-%m-%d %H:%M:%S'))
                                       , UpdateParam)
        cur.connection.commit()
        cur.close()

    app.logger.debug("Update time %s for %d rows", (datetime.datetime.now() - starttime).total_seconds(), len(UpdateParam))


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
        parent = database.GetAll("select id, aphia_id, taxostatus from taxonomy_worms where id=%s",[parent])
        if len(parent)!=1:
            return "invalid parent, doesn't exists in database"
        # TODO: It's a bit more tricky
        # if parent[0]["aphia_id"] is not None:
        #     return "cannot create a WoRMS child using this form"
        if parent[0]["taxostatus"] == 'D':
            return "cannot create in a deprecated category"
    if taxotype == 'P' : # Phylo
        # if not re.match(r"^[A-Z][a-z+\-']{2,} ?[a-z+\-']*$", name):
        if not re.match(r"^[A-Z][a-z+\-']{2,}( [a-z+\-']+)*$", name):
            return "Name must start with an Uppercase letter, then contain only lowercase letters, the symbols +, -, ' or a single space; minimum length is 3 characters."
        sql="select count(*) from taxonomy_worms where lower(name)=lower( (%s) ) "
        if updatetarget and int(updatetarget)>0 :
            sql +=" and id!={}".format(int(updatetarget))
        Nbr=int(database.GetAll(sql,[name])[0][0])
        if Nbr!=0:
            return "duplicate name"
    elif taxotype == 'M' : # Morpho
        if not re.match(r"^[a-z0-9+\-']{3,}( [a-z0-9+\-']+)*$", name):
            return "Name must contain lowercase letters, digits, the symbols +, -, ', or a single space; minimum length is 3 characters."
        if not parent:
            return "You must specify a parent to check morpho type"
        sql="select count(*) from taxonomy_worms where lower(name)=lower( (%s) ) and parent_id={}".format(int(parent))
        if updatetarget and int(updatetarget)>0 :
            sql +=" and id!={}".format(int(updatetarget))
        Nbr=int(database.GetAll(sql,[name])[0][0])
        if Nbr!=0:
            return "duplicate child for this parent"
    else:
        return "Invalid taxotype"
    return "ok"


def get_lineage(res:List[Dict])->List[Dict]:
    taxons:List[Dict]=[]
    wormsfinder = WoRMSFinder()
    def getlineage(r):
        lineage = wormsfinder.aphia_classif_by_id(int(r["AphiaID"]))
        if lineage['child'] is not None:
            flipped = WoRMSFinder.reverse_lineage(lineage)
        else:
            flipped = {}
        taxons.append(
            dict({"aphia_id": r["AphiaID"], "name": r["scientificname"], "rank": r["rank"], "status": r["status"],
                  "lineage": flipped}))
    for r in res:
        if (r["valid_AphiaID"] != r["AphiaID"]):
        #if r["status"] != WORMS_STATUS_ACCEPTED or (r["valid_AphiaID"] != r["AphiaID"]):
            continue
        getlineage(r)
    return taxons


@app.route('/addwormstaxon/',methods=['POST'])
def routeaddwormstaxon():
    CheckInstanceSecurity()
    aphia_id = gvp("aphia_id")
    creator = gvp("creator_email")
    if aphia_id is None or creator is None:
        return "Invalid parameters"
    try:
        aphia_id = int(aphia_id)
    except ValueError:
        return "Invalid parameters"
    rsp = add_worms_taxon(aphia_id, creator)
    return rsp

def add_worms_taxon(aphia_id:int, creator:str) -> Any:
    with TaxoOperationLocker():
        app.logger.info('In Locker')
        taxo_exist = Taxonomy.query.filter(Taxonomy.aphia_id == aphia_id).first()
        if taxo_exist is not None:
            err = {"error":"taxon with aphia_id %s already exist." % aphia_id}
            response = make_response(json.dumps(err), 422)
            return response
        worms_finder = WoRMSFinder()
        lineage = worms_finder.aphia_classif_by_id(aphia_id, flatten=True)
        # Check both ends of lineage
        assert lineage[0]["scientificname"] == "Biota"
        to_create = lineage[-1]
        assert to_create["AphiaID"] == aphia_id
        parents = create_worms_lineage(lineage, creator)
        ComputeDisplayName(parents)
        msg = {"success":"taxon "+to_create["scientificname"]+" and lineage added"}
        response = make_response(json.dumps(msg), 200)
        return response


@app.route('/wormstaxon/<name>',methods=['GET', 'POST'])
def routewormstaxon(name):
    # CheckInstanceSecurity()
    wormsfinder=WoRMSFinder()
    res=wormsfinder.aphia_records_by_name_sync(name)
    taxons:List[Dict]=get_lineage(res)
    aphiaids = [taxon["aphia_id"] for taxon in taxons]
    rows = Taxonomy.query.filter(Taxonomy.aphia_id.in_(aphiaids)).all()
    founds = {row.aphia_id:row.id for row in rows}
    for taxon in taxons:
        if taxon["aphia_id"] in founds.keys():
            taxon.update({"id":founds[taxon["aphia_id"]]})
    return json.dumps(taxons)

@app.route('/checktaxon/',methods=['POST'])
def routechecktaxon():
    CheckInstanceSecurity()
    taxotype=gvp('taxotype')
    name=gvp('name')
    parent= gvp('parent_id')
    updatetarget = gvp('updatetarget')
    res={"msg":checktaxon(taxotype,name,parent=parent,updatetarget=updatetarget)}
    return json.dumps(res)

@app.route('/checktaxonHTML/',methods=['POST'])
def routechecktaxonHTML():
    # CheckInstanceSecurity()
    taxotype=gvp('taxotype')
    name=gvp('name')
    parent= gvp('parent_id')
    updatetarget = gvp('updatetarget')
    msg=checktaxon(taxotype,name,parent=parent,updatetarget=updatetarget)
    if msg=='ok':
        return FormatSuccess("Naming rules OK")
    else:
        return FormatError(msg)

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

    g.ecotaxa_version=gvp('ecotaxa_version')
    g.ecotaxa_version_int=0
    re_match=re.match(r"(\d+).(\d+).(\d+)",g.ecotaxa_version)
    if re_match:
        g.ecotaxa_version_int=int(re_match.group(1))*10000+int(re_match.group(2))*100+int(re_match.group(3))
    else:
        abort(jsonify(msg='invalid version'))
    g.MsgVersion="ok"
    if g.ecotaxa_version_int<20000:
        g.MsgVersion = "Current Ecotaxa version is 2.0.0 it's recommanded to upgrade your version (%s.%s.%s) ."%(
            re_match.group(1),re_match.group(2),re_match.group(3))
    if g.ecotaxa_version_int<20000:
        abort(jsonify(msg='Your Ecotaxa version is too old, you must upgrade'))


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
    taxon.taxotype= gvp('taxotype') if gvp('taxotype') else None
    taxon.aphia_id= gvp('aphia_id') if gvp('aphia_id') else None
    taxon.rank = gvp('rank')
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
        taxon.creation_datetime = datetime.datetime.now(datetime.timezone.utc)
        taxon.id_instance = int(gvp('id_instance'))
    taxon.lastupdate_datetime= datetime.datetime.now(datetime.timezone.utc)
    if gvp('rename_to')!='':
        if len(database.GetAll("select id from taxonomy_worms where id=%s",[int(gvp('rename_to'))]))!=1:
            abort(jsonify(msg="invalid rename_to value"))
    taxon.rename_to= gvp('rename_to') or None

def create_worms_lineage(wrms_lineage:List[Dict[str,str]], creator:str)->List[int]:
    impacted_ids=[]
    def add_lineage(lineage:List[Dict[str,str]], parent_id:int):
        wrms_taxon = lineage[0]
        search = Taxonomy.query.filter(
            Taxonomy.aphia_id==int(wrms_taxon["AphiaID"])).first()
        if search is None:
            dt = datetime.datetime.now(datetime.timezone.utc)
            parent = add_taxonomy_from_worms(wrms_taxon,parent_id,dt,creator)
            parent_id = parent.id
            assert parent_id is not None
        else:
            parent_id = search.id
        impacted_ids.append(parent_id)
        db.session.commit()
        if len(lineage)>1:
            add_lineage(lineage[1:], parent_id)

    add_lineage(wrms_lineage, -1)
    return impacted_ids

def add_taxonomy_from_worms(wrms_taxon: dict, parent_id:int, dt: datetime.datetime, creator: str) -> Taxonomy :
    added = Taxonomy()
    db.session.add(added)
    added.parent_id = parent_id
    added.aphia_id = wrms_taxon["AphiaID"]
    added.name = wrms_taxon["scientificname"]
    added.rank = wrms_taxon["rank"]
    added.creation_datetime = dt
    added.source_url = WORMS_URL+str(added.aphia_id)
    added.taxonstatus = DEFAULT_WORMS_STATUS
    added.taxotype = DEFAULT_WORMS_TYPE
    added.creator_email = creator
    added.lastupdate_datetime = added.creation_datetime
    try:
        added.id_instance = int(gvp('id_instance'))
    except:
        added.id_instance = 0
    db.session.commit()
    return added

@app.route('/settaxon/',methods=['POST'])
def routesettaxon():
    """
        Update a taxon or create a new one if 'id' parameter is not supplied.
    """
    CheckInstanceSecurity()
    taxotype = gvp('taxotype')
    name = gvp('name')
    parent = gvp('parent_id')
    aphia_id = gvp('aphia_id','')
    taxonid = gvp('id','')

    with TaxoOperationLocker():
        app.logger.info('In Locker')
        msg=checktaxon(taxotype, name, parent=parent,updatetarget=taxonid)
        if msg!='ok':
            return jsonify(msg=msg)
        if taxonid!='':
            Taxon = Taxonomy.query.filter(id==int(taxonid))
            CheckTaxonUpdateRight(Taxon)
        else:
            Taxon=Taxonomy()
            db.session.add(Taxon)
        UpdateObjectFromForm(Taxon)
        db.session.commit()
        taxonids=[Taxon.id]
        if aphia_id!='' and (taxonid=='' or (Taxon.aphia_id!=int(aphia_id))):
            wormsfinder=WoRMSFinder()
            lineage = wormsfinder.aphia_classif_by_id(int(aphia_id))
            if lineage['child'] is not None:
                flipped=WoRMSFinder.reverse_lineage(lineage)
                added=create_worms_lineage(Taxon,flipped)
                taxonids.extend(added)
            else:
                taxonids=[]
        ComputeDisplayName(taxonids)
        return jsonify(msg='ok',id=Taxon.id)

# Return changes on taxa tree, since given date if provided, for given id if provided
@app.route('/gettaxon/',methods=['POST'])
def routegettaxon():
    sql="""select id,aphia_id,aphia_id as id_source,parent_id,rank,name,taxotype,display_name,source_url,source_desc
          ,creator_email,creation_datetime,lastupdate_datetime,id_instance,taxostatus,rename_to 
          from (
            select t.id,t.aphia_id,t.parent_id,t.rank,t.name,t.taxotype,t.display_name,t.source_url,t.source_desc
            ,t.creator_email
            ,to_char(t.creation_datetime,'YYYY-MM-DD HH24:MI:SS') creation_datetime
            ,to_char(t.lastupdate_datetime,'YYYY-MM-DD HH24:MI:SS') lastupdate_datetime
            ,t.lastupdate_datetime as lastupdate_datetime_raw
            ,t.id_instance,t.taxostatus,t.rename_to 
            from taxonomy_worms t
            UNION ALL
            select g.id, g.aphia_id, g.parent_id, g.rank, g.name, g.taxotype, g.display_name, g.source_url, g.source_desc
            , g.creator_email
            , to_char(g.creation_datetime,'YYYY-MM-DD HH24:MI:SS') creation_datetime
            , to_char(g.lastupdate_datetime,'YYYY-MM-DD HH24:MI:SS') lastupdate_datetime
            , g.lastupdate_datetime as lastupdate_datetime_raw
            , g.id_instance, 'X' as taxostatus, g.rename_to 
            from gone_taxa g
          ) t where 1=1 """
    sqlparam={}
    filtertype = gvp('filtertype')
    if filtertype=='id':
        sql+=" and id=%(id)s "
        sqlparam['id']=int(gvp('id'))
    elif filtertype=='since':
        sql+=""" and lastupdate_datetime_raw>=to_timestamp(%(startdate)s,'YYYY-MM-DD HH24:MI:SS') 
                    """
        # and (taxostatus!='N' or id_instance=%(id_instance)s)     ==> Désactivé dans un premier temps
        sqlparam['startdate']=gvp('startdate')
        # sqlparam['id_instance']=gvp('id_instance')
    else:
        return jsonify(msg='filtertype required')
    sql += " order by lastupdate_datetime_raw "
    res=database.GetAll(sql,sqlparam,cursor_factory=psycopg2.extras.RealDictCursor)
    if len(res)==0 and filtertype=='id':
        return jsonify(msg='no data found')
    return jsonify(res)

@app.route('/setstat/',methods=['POST'])
def routesetstat():
    CheckInstanceSecurity()
    id_instance=int(gvp('id_instance'))

    database.ExecSQL("delete from ecotaxainststat WHERE id_instance=%s"%id_instance)
    # sql="insert into ecotaxainststat(id_instance, id_taxon, nbr) VALUES ({},%s,%s)".format(id_instance)
    sql="insert into ecotaxainststat(id_instance, id_taxon, nbr) VALUES "
    stat=json.loads(gvp('data'))
    if len(stat)>0: # to avoid error on empty DB
        sql += ",".join("({},{},{})".format(id_instance,int(k),int(v)) for (k,v) in stat.items())
        database.ExecSQL(sql)
    database.ExecSQL("update ecotaxainst set laststatupdate_datetime=timezone('utc',now()),ecotaxa_version=%s WHERE id=%s"
                     ,[g.ecotaxa_version,id_instance])
    PurgeDeprecatedTaxon()
    return jsonify(msg='ok',msgversion=g.MsgVersion)
@app.route('/wormstaxonset')
def PurgeDeprecatedTaxon():
    # purge les stat sur des instances supprimées
    ExecSQL("delete from ecotaxainststat where id_instance not in (select id from ecotaxainst)")
    # purge ceux qui ont été déclaré D avant toutes les synchro UP (normalement précédée d'un down)
    # et qui n'ont pas d'enfants (d'ou le fait de l'executer 20 fois pour purger une branche entière)
    for i in range(20):
        rowcount=ExecSQL("""delete
            from taxonomy_worms t
            where taxostatus='D'
            and lastupdate_datetime<(select min(laststatupdate_datetime) from ecotaxainst)
            and not exists (select 1 from ecotaxainststat s where s.id_taxon=t.id)
            and not exists (select 1 from taxonomy c where c.parent_id=t.id)
            """)
        if rowcount==0:
            break

def RefreshTaxoStat():
    n=ExecSQL("UPDATE taxonomy_worms SET  nbrobj=Null,nbrobjcum=null where nbrobj is NOT NULL or nbrobjcum is not null")
    app.logger.info("RefreshTaxoStat cleaned %d taxo"%n)

    app.logger.info("Refresh projects_taxo_stat")
    # for r in GetAll('select projid from projects'):
    #     RecalcProjectTaxoStat(r['projid'])

    n=ExecSQL("""UPDATE taxonomy_worms
                SET  nbrobj=q.nbr
                from (select id_taxon classif_id, sum(nbr) nbr 
                      from ecotaxainststat pts
                       group by id_taxon)q
                where taxonomy_worms.id=q.classif_id""")
    app.logger.info("RefreshTaxoStat updated %d 1st level taxo"%(n))

    n=ExecSQL("""UPDATE taxonomy_worms
                SET  nbrobjcum=q.nbr
                from (select parent_id,sum(nbrobj) nbr from taxonomy
                      where nbrobj is NOT NULL
                      group by parent_id ) q
                where taxonomy_worms.id=q.parent_id""")
    app.logger.info("RefreshTaxoStat updated %d 2st level taxo"%(n))
    for i in range(50):
        n=ExecSQL("""UPDATE taxonomy_worms
                    SET  nbrobjcum=q.nbr
                    from (select parent_id,sum(nbrobjcum+coalesce(nbrobj,0)) nbr from taxonomy_worms
                          where nbrobjcum is NOT NULL
                          group by parent_id  ) q
                    where taxonomy_worms.id=q.parent_id
                    and coalesce(taxonomy.nbrobjcum,0)<>q.nbr""")
        print("RefreshTaxoStat updated %d level %d taxo"%(n,i))
        if n==0:
            break
    # appli.part.prj.GlobalTaxoCompute()
