from flask import Blueprint, render_template, g, request,url_for,abort
from flask.json import jsonify
from flask_login import current_user
from appli import app,ObjectToStr,PrintInCharte,database,db,gvg,gvp,FormatSuccess,FormatError,ntcv
from appli.database import ExecSQL,GetAll
from flask_security.decorators import roles_accepted
import json,re,time,psycopg2.extras,random,datetime
# from typing import List  # Pas supporté python 3.4 serveur demo

# def ComputeDisplayName(TaxoList:List[int]):
def ComputeDisplayName(TaxoList):
    """
    Compute display_name column in database, for the list of provided id
    :param TaxoList: 
    :return: None 
    """
#     sql="""with duplicate as (select name from taxonomy GROUP BY name HAVING count(*)>1),
#               nt as (select t.id,case when t.name like '%% %%' or p.id is null then t.name
#                     when t.taxotype='M' and P.taxotype='M' then concat(p2.name||'>',p.name||'>',t.name)
#                     when t.name in (select name from duplicate) then concat(p.name||'>',t.name)
#                     else t.name end
#                     ||case when t.taxostatus='D' then ' (Deprecated)' else '' end
#                      newname
#   from taxonomy t
#   left JOIN taxonomy p on t.parent_id=p.id
#   left JOIN taxonomy p2 on p.parent_id=p2.id
#   where (t.id = any ( %(taxo)s ) or p.id = any ( %(taxo)s ) or p2.id = any ( %(taxo)s ))
# )
# update public.taxonomy t set display_name=newname,lastupdate_datetime=to_timestamp(%(ts)s,'YYYY-MM-DD HH24:MI:SS')
# from nt
# where nt.id=t.id and display_name IS DISTINCT FROM newname """
#     # Pour chaque nom on cherche à determiner à quelle hauteur il n'y a plus de doublons
#     # quand plus de 2 doublons ça peut conduire à une inflation car on va prendre le plus long pour tous
#     # alors que par forcement necessaire ex : A<B , A<C<D , A<C<E  A<B sera A<B<X inutilement rallongé
#     sql="""
#     with duplicate as (
#     select t.name, count(distinct t.id) cid,
#       count(distinct concat(t.name,'<'||p.name)) c2,
#       count(distinct concat(t.name,'<'||p.name,'<'||p2.name)) c3,
#       count(distinct concat(t.name,'<'||p.name,'<'||p2.name,'<'||p3.name)) c4
#       from taxonomy t
#       left JOIN taxonomy p on t.parent_id=p.id
#       left JOIN taxonomy p2 on p.parent_id=p2.id
#       left JOIN taxonomy p3 on p2.parent_id=p3.id
#       group by t.name
#     having count(distinct t.id)>1 )
#         ,nt as (select t.id,case when d.name is null then t.name
#                                  when cid=c2 then concat(t.name,'<'||p.name)
#                                  when cid=c3 then concat(t.name,'<'||p.name,'<'||p2.name)
#                                  else  concat(t.name,'<'||p.name,'<'||p2.name,'<'||p3.name)
#                              end newname
#       from taxonomy t
#       left JOIN duplicate d on t.name=d.name
#       left JOIN taxonomy p on t.parent_id=p.id
#       left JOIN taxonomy p2 on p.parent_id=p2.id
#       left JOIN taxonomy p3 on p2.parent_id=p3.id
#       where (t.id = any ( %(taxo)s ) or p.id = any ( %(taxo)s ) or p2.id = any(%(taxo)s) or p3.id = any(%(taxo)s)  )
#     )
#     update public.taxonomy t set display_name=newname
#     from nt
#       where nt.id=t.id
#
#     """
#     database.ExecSQL(sql,{'taxo':TaxoList,'ts':datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')})
    # on recalcule tous les doublons + ceux qui n'ont pas de noms + ceux ayant le même nom que ceux demandés dans leur lineage 3.
    sql = """with duplicate as (select lower(name) as name from taxonomy GROUP BY lower(name) HAVING count(*)>1)
          select t.id,t.name tname,p.name pname,p2.name p2name,p3.name p3name,t.display_name,t.taxostatus
          from taxonomy t
          left JOIN duplicate d on lower(t.name)=d.name
          left JOIN taxonomy p on t.parent_id=p.id
          left JOIN taxonomy p2 on p.parent_id=p2.id
          left JOIN taxonomy p3 on p2.parent_id=p3.id
          where d.name is not null or t.display_name is null 
          or lower(t.name) in (select lower(st.name) 
                                  from taxonomy st
                                  left JOIN taxonomy sp on st.parent_id=sp.id
                                  left JOIN taxonomy sp2 on sp.parent_id=sp2.id
                                  left JOIN taxonomy sp3 on sp2.parent_id=sp3.id
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
        if D['taxostatus']=='D':
            Duplicates[i]['newname'] += " (Deprecated)"

    app.logger.debug("Compute time %s ", (datetime.datetime.now() - starttime).total_seconds())
    starttime = datetime.datetime.now()
    UpdateParam = []
    for D in Duplicates:
        if D['display_name'] != D['newname']:
            UpdateParam.append((int(D['id']), D['newname']))
    if len(UpdateParam) > 0:
        cur = g.db.cursor()
        psycopg2.extras.execute_values(cur
                                       , """UPDATE taxonomy SET display_name = data.pdisplay_name,lastupdate_datetime=to_timestamp('{}','YYYY-MM-DD HH24:MI:SS') 
               FROM (VALUES %s) AS data (pid, pdisplay_name)
               WHERE id = data.pid""".format(datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S'))
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
        if len(database.GetAll("select id from taxonomy where id=%s",[parent]))!=1:
            return "invalid parent, doesn't exists in database"
    if taxotype == 'P' : # Phylo
        # if not re.match(r"^[A-Z][a-z+\-']{2,} ?[a-z+\-']*$", name):
        if not re.match(r"^[A-Z][a-z+\-']{2,}( [a-z+\-']+)*$", name):
            return "Must contains only letters and -+' and not more than 1 consecutive whitespace, Start with a Uppercase"
        sql="select count(*) from taxonomy where lower(name)=lower( (%s) ) "
        if updatetarget and int(updatetarget)>0 :
            sql +=" and id!={}".format(int(updatetarget))
        Nbr=int(database.GetAll(sql,[name])[0][0])
        if Nbr!=0:
            return "duplicate name"
    elif taxotype == 'M' : # Morpho
        if not re.match(r"^[a-z+\-']{3,}( [a-z+\-']+)*$", name):
            return "Must contains only letters and -+' and not more than 1 consecutive whitespace, be lowercase only"
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
    parent= gvp('parent_id')
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
                    
                    """
        # and (taxostatus!='N' or id_instance=%(id_instance)s)     ==> Désactivé dans un premier temps
        sqlparam['startdate']=gvp('startdate')
        # sqlparam['id_instance']=gvp('id_instance')
    else:
        return jsonify(msg='filtertype required')
    sql += " order by lastupdate_datetime "
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

def PurgeDeprecatedTaxon():
    # purge les stat sur des instances supprimées
    ExecSQL("delete from ecotaxainststat where id_instance not in (select id from ecotaxainst)")
    # purge ceux qui ont été déclaré D avant toutes les synchro UP (normalement précédée d'un down)
    # et qui n'ont pas d'enfants (d'ou le fait de l'executer 20 fois pour purger une branche entière)
    for i in range(20):
        rowcount=ExecSQL("""delete
            from taxonomy t
            where taxostatus='D'
            and lastupdate_datetime<(select min(laststatupdate_datetime) from ecotaxainst)
            and not exists (select 1 from ecotaxainststat s where s.id_taxon=t.id)
            and not exists (select 1 from taxonomy c where c.parent_id=t.id)
            """)
        if rowcount==0:
            break

def RefreshTaxoStat():
    n=ExecSQL("UPDATE taxonomy SET  nbrobj=Null,nbrobjcum=null where nbrobj is NOT NULL or nbrobjcum is not null")
    app.logger.info("RefreshTaxoStat cleaned %d taxo"%n)

    app.logger.info("Refresh projects_taxo_stat")
    # for r in GetAll('select projid from projects'):
    #     RecalcProjectTaxoStat(r['projid'])

    n=ExecSQL("""UPDATE taxonomy
                SET  nbrobj=q.nbr
                from (select id_taxon classif_id, sum(nbr) nbr 
                      from ecotaxainststat pts
                       group by id_taxon)q
                where taxonomy.id=q.classif_id""")
    app.logger.info("RefreshTaxoStat updated %d 1st level taxo"%(n))

    n=ExecSQL("""UPDATE taxonomy
                SET  nbrobjcum=q.nbr
                from (select parent_id,sum(nbrobj) nbr from taxonomy
                      where nbrobj is NOT NULL
                      group by parent_id ) q
                where taxonomy.id=q.parent_id""")
    app.logger.info("RefreshTaxoStat updated %d 2st level taxo"%(n))
    for i in range(50):
        n=ExecSQL("""UPDATE taxonomy
                    SET  nbrobjcum=q.nbr
                    from (select parent_id,sum(nbrobjcum+coalesce(nbrobj,0)) nbr from taxonomy
                          where nbrobjcum is NOT NULL
                          group by parent_id  ) q
                    where taxonomy.id=q.parent_id
                    and coalesce(taxonomy.nbrobjcum,0)<>q.nbr""")
        print("RefreshTaxoStat updated %d level %d taxo"%(n,i))
        if n==0:
            break
    # appli.part.prj.GlobalTaxoCompute()
