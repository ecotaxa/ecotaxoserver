# -*- coding: utf-8 -*-
from flask import Blueprint, render_template, g, request,url_for,Response
from flask_login import current_user
from appli import app,ObjectToStr,PrintInCharte,database,db,gvg,gvp,FormatError,FormatSuccess,XSSEscape,ntcv
from appli.database import GetAll
import json,re,traceback,datetime
from appli.services import ComputeDisplayName
from flask_security.decorators import roles_accepted,login_required

# Version avec la depth
# SQLTreeSelect="""case  when t1 is null then 1 when t2 is null then 2 when t3 is null then 3 when t4 is null then 4
#       when t5 is null then 5 when t6 is null then 6 when t7 is null then 7 when t8 is null then 8
#       when t9 is null then 9 when t10 is null then 10 when t11 is null then 11 when t12 is null then 12
#       when t13 is null then 13 when t14 is null then 14 when t15 is null then 15 when t16 is null then 16
#       when t17 is null then 17 when t18 is null then 18 when t19 is null then 19 end depth
# ,concat(t14.name||'>',t13.name||'>',t12.name||'>',t11.name||'>',t10.name||'>',t9.name||'>',t8.name||'>',t7.name||'>',
#      t6.name||'>',t5.name||'>',t4.name||'>',t3.name||'>',t2.name||'>',t1.name||'>',t.name) tree"""

SQLTreeSelect="""concat(t14.name||'>',t13.name||'>',t12.name||'>',t11.name||'>',t10.name||'>',t9.name||'>',t8.name||'>',t7.name||'>',
     t6.name||'>',t5.name||'>',t4.name||'>',t3.name||'>',t2.name||'>',t1.name||'>',t.name) tree"""
SQLTreeJoin="""left join taxonomy_worms t1 on t.parent_id=t1.id
      left join taxonomy_worms t2 on t1.parent_id=t2.id
      left join taxonomy_worms t3 on t2.parent_id=t3.id
      left join taxonomy_worms t4 on t3.parent_id=t4.id
      left join taxonomy_worms t5 on t4.parent_id=t5.id
      left join taxonomy_worms t6 on t5.parent_id=t6.id
      left join taxonomy_worms t7 on t6.parent_id=t7.id
      left join taxonomy_worms t8 on t7.parent_id=t8.id
      left join taxonomy_worms t9 on t8.parent_id=t9.id
      left join taxonomy_worms t10 on t9.parent_id=t10.id
      left join taxonomy_worms t11 on t10.parent_id=t11.id
      left join taxonomy_worms t12 on t11.parent_id=t12.id
      left join taxonomy_worms t13 on t12.parent_id=t13.id
      left join taxonomy_worms t14 on t13.parent_id=t14.id"""


PackTreeTxtPattern= re.compile(r"^([^>]+>)(.*)((?:>.[^>]+){3})$")
def PackTreeTxt(txt):
    m=PackTreeTxtPattern.match(txt)
    if m is None:
        return txt
    return "{}<span class=TreeMiddle>{}</span><span class=TreeMiddleBtn></span>{}".format(m.group(1),m.group(2),m.group(3))


@app.route('/browsetaxo/')
def browsetaxo():
    lst=GetAll("""select t.id,t.aphia_id,t.parent_id,t.rank,t.display_name as name,t.taxotype,t.taxostatus,t.creator_email
      ,to_char(t.creation_datetime,'yyyy-mm-dd hh24:mi') creation_datetime,to_char(t.lastupdate_datetime,'yyyy-mm-dd hh24:mi') lastupdate_datetime,{}
    from taxonomy_worms t
    {}
    order by t.id
    LIMIT 200
    """.format(SQLTreeSelect,SQLTreeJoin))
    for lstitem in lst:
        # lstitem['tree']=PackTreeTxt(lstitem['tree']) #evite les problèmes de safe
        if lstitem['parent_id'] is None:
            lstitem['parent_id']=""

    nbrtaxon=GetAll("select count(*) from taxonomy_worms")[0][0]
    g.AdminLists=GetAll("select email,name from users where email like '%@%' and active=TRUE order by 2")
    return render_template('browsetaxo.html',lst=lst,nbrtaxon=nbrtaxon,taxon_id=gvg('id'))


@app.route('/browsetaxo/ajax',methods=['POST'])
def browsetaxoajax():
    sql="""select t.id,t.aphia_id,t.parent_id,t.rank,t.display_name as name,t.taxotype,t.taxostatus,t.creator_email
      ,to_char(t.creation_datetime,'yyyy-mm-dd hh24:mi') creation_datetime,to_char(t.lastupdate_datetime,'yyyy-mm-dd hh24:mi') lastupdate_datetime,
      {}, t.rename_to
    from taxonomy_worms t 
    {}
    where 1=1
    """.format(SQLTreeSelect,SQLTreeJoin)
    params={}
    sqlcrit=""
    start=0
    length=200
    if gvp('start').isdigit():
        start=int(gvp('start'))
    if gvp('length').isdigit():
        length=int(gvp('length'))

    if gvp('columns[0][search][value]').isdigit():
        sqlcrit += " and t.id = %(id)s"
        params['id']=int(gvp('columns[0][search][value]'))
    if gvp('columns[1][search][value]').isdigit():
        sqlcrit += " and t.aphia_id = %(aphia_id)s"
        params['aphia_id'] = int(gvp('columns[1][search][value]'))
    if gvp('columns[2][search][value]').isdigit():
        if int(gvp('columns[2][search][value]'))==0:
            sqlcrit += " and t.parent_id is null "
        else:
            sqlcrit += " and (t.parent_id = %(parent_id)s or t.id=%(parent_id)s ) " # or id pour faciliter la navigation
            params['parent_id']=int(gvp('columns[2][search][value]'))
    if gvp('columns[3][search][value]'):
        sqlcrit += " and t.rank ilike %(rank)s"
        params['rank']='%'+gvp('columns[3][search][value]')+'%'
    if gvp('columns[4][search][value]'):
        sqlcrit += " and t.display_name ilike %(name)s"
        params['name'] = '%' + gvp('columns[4][search][value]') + '%'
    if gvp('columns[5][search][value]'):
        sqlcrit += " and t.taxotype = %(taxotype)s"
        params['taxotype']=gvp('columns[5][search][value]')
    if gvp('columns[6][search][value]'):
        sqlcrit += " and t.taxostatus = %(taxostatus)s"
        params['taxostatus']=gvp('columns[6][search][value]')
    if gvp('columns[7][search][value]'):
        sqlcrit += " and t.creator_email ilike %(creator_email)s"
        params['creator_email']='%'+gvp('columns[7][search][value]')+'%'
    if gvp('columns[9][search][value]').isdigit():
        sqlcrit += " and lastupdate_datetime like %(lastupdate)s"
        params['lastupdate']=int(gvp('columns[9][search][value]'))
    if gvp('columns[10][search][value]'):
        sqlcrit += """ and tree ilike %(tree)s"""
        params['tree']='%'+gvp('columns[10][search][value]')+'%'

    sqlcount="select count(*) from taxonomy_worms t where 1=1 "
    if 'tree' in params:
        sqlcount = "select count(*) from ({}) t where 1=1 ".format(sql)
        sql="select t.* from ({}) t where 1=1 ".format(sql) # permet de mettre des critères sur la colonne calculée tree
    orderclause=""
    if gvp('order[0][column]').isdigit():
        orderclause=" order by {} {}".format(int(gvp('order[0][column]'))+1,'desc' if gvp('order[0][dir]')=='desc' else 'asc')
    sql += sqlcrit+orderclause+ (" offset {} limit {}".format(start,length))
    lst = GetAll(sql,params)
    for lstitem in lst: # Post traitement sur les chaines
        lstitem['tree']=PackTreeTxt(lstitem['tree'])
        lstitem['name']=XSSEscape(lstitem['name'] or '???')
        lstitem['name'] += "⇢"+str(lstitem['rename_to']) if lstitem['rename_to'] is not None else ""
        if lstitem['parent_id'] is None:
            lstitem['parent_id']=""

    sqlcount +=sqlcrit
    if len(lst)>=length or start!=0:
        recordsFiltered= GetAll(sqlcount,params)[0][0]
    else:
        recordsFiltered=len(lst)
    nbrtaxon = GetAll("select count(*) from taxonomy_worms")[0][0]
    res={'draw':int(gvp('draw')),'recordsTotal':nbrtaxon,'recordsFiltered':recordsFiltered,'data':lst}

    return json.dumps(res)

@app.route('/browsetaxotsvexport/')
def browsetaxotsvexport():
    sql="""select t.id,t.aphia_id,t.parent_id,t.rank,t.name,t.taxotype,t.taxostatus
      ,t.source_url,t.source_desc
      ,t.creator_email,to_char(t.creation_datetime,'yyyy-mm-dd hh24:mi,ss') creation_datetime
      ,t.id_instance,t.rename_to
      ,to_char(t.lastupdate_datetime,'yyyy-mm-dd hh24:mi:ss') lastupdate_datetime
      ,i.name instance_name,tr.display_name rename_to_name,t.nbrobj,t.nbrobjcum
      ,t.display_name,{}
    from taxonomy_worms t 
    LEFT JOIN ecotaxainst i on t.id_instance=i.id
    LEFT JOIN taxonomy_worms tr on tr.id=t.rename_to
    {}
    order by 1
    """.format(SQLTreeSelect,SQLTreeJoin)
    lst = GetAll(sql)
    t=[]
    t.append("id\taphia_id\tparent_id\trank\tname\ttaxotype\ttaxostatus\tsource_url\tsource_desc\tcreator_email\tcreation_datetime"
             +"\tid_instance\trename_to\tlastupdate_datetime\tinstance_name\trename_to_name\tnbrobj\tnbrobjcum\tdisplay_name\tlineage")
    for l in lst:
        # t.append("{id},{parent_id}".format(l))
        t.append("\t".join((str(ntcv(x)).replace("\n","\\n").replace("\t","\\t").replace("\r","") for x in l[0:99])))
    return Response( "\n".join(t) ,mimetype="text/tsv",headers={
        "Content-Disposition" : "attachment; filename=taxoexport_%s.tsv"%datetime.datetime.utcnow().strftime('%Y%m%d_%H%M%S') })

@app.route('/browsetaxoviewpopup/<int:taxoid>')
def browsetaxoviewpopup(taxoid):
    sql = """select t.*,i.name inst_name,url inst_url,concat(rt.name,'<'||rt2.name,'<'||rt3.name,'<'||rt4.name) rename_to_name
            ,p.name parentname,to_char(t.creation_datetime,'YYYY-MM-DD HH24:MI') creationdatetimefmt,{}
        from taxonomy_worms t 
        {}
        left join ecotaxainst i on i.id=t.id_instance
        left join taxonomy_worms rt on t.rename_to=rt.id
        left join taxonomy_worms p on t.parent_id=p.id
        left join taxonomy_worms rt2 on rt.parent_id=rt2.id
        left join taxonomy_worms rt3 on rt2.parent_id=rt3.id
        left join taxonomy_worms rt4 on rt3.parent_id=rt4.id
        where t.id = %(id)s
        """.format(SQLTreeSelect, SQLTreeJoin)
    taxon= GetAll(sql,{'id':taxoid})[0]
    g.TaxoType=database.TaxoType
    g.TaxoStatus = database.TaxoStatus
    stat= GetAll("""select i.name,i.url,s.nbr
    from ecotaxainststat s join ecotaxainst i on i.id=s.id_instance 
    where id_taxon=%(id)s
    order by i.name """,{'id':taxoid})
    return render_template('browsetaxoviewpopup.html', taxon=taxon,stat=stat)

@app.route('/browsetaxoeditpopup/<int:taxoid>')
@login_required
@roles_accepted(database.AdministratorLabel)
def browsetaxoeditpopup(taxoid):
    sql = """select t.*,i.name inst_name,url inst_url,concat(rt.display_name) rename_to_name
            ,p.display_name parentname,to_char(t.creation_datetime,'YYYY-MM-DD HH24:MI') creationdatetimefmt,{}
        from taxonomy_worms t 
        {}
        left join ecotaxainst i on i.id=t.id_instance
        left join taxonomy_worms rt on t.rename_to=rt.id
        left join taxonomy_worms p on t.parent_id=p.id
        where t.id = %(id)s
        """.format(SQLTreeSelect, SQLTreeJoin)
    taxon= GetAll(sql,{'id':taxoid})[0]
    g.TaxoType=database.TaxoType
    g.TaxoStatus = database.TaxoStatus
    return render_template('browsetaxoeditpopup.html', taxon=taxon)



@app.route('/browsetaxosavepopup/',methods=['POST'])
@login_required
@roles_accepted(database.AdministratorLabel)
def browsetaxosavepopup():
    txt=""
    try:
        # txt = json.dumps(request.form)
        taxonid=int(gvp('id'))
        if taxonid>0:
            taxon=database.Taxonomy.query.filter_by(id=taxonid).first()
            if taxon is None:
                raise Exception("Taxon not found in Database")
        else:
            raise Exception("Taxon missing")
        taxon.source_url=gvp('source_url')
        taxon.name = gvp('name')
        taxon.aphia_id = gvp('aphia_id')
        taxon.parent_id = gvp('parent_id')
        taxon.rank = gvp('rank')
        taxon.taxotype = gvp('taxotype')
        taxon.source_url = gvp('source_url')
        taxon.source_desc = gvp('source_desc')
        taxon.creator_email = gvp('creator_email')
        taxon.taxostatus = gvp('taxostatus')
        taxon.rename_to = gvp('rename_to') or None
        db.session.commit()
        ComputeDisplayName([taxonid])
        database.ExecSQL("UPDATE public.taxonomy_worms t SET lastupdate_datetime=to_timestamp(%s,'YYYY-MM-DD HH24:MI:SS') WHERE id=%s"
                         , [datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S'),taxonid])

        # txt=FormatSuccess("POST = {}",txt)
        txt="""<script>
$('#tbl').DataTable().draw();
At2PopupClose(0);
</script>"""

        return txt
    except Exception as e:
        tb_list = traceback.format_tb(e.__traceback__)
        return FormatError("Saving Error : {}\n{}",e,"__BR__".join(tb_list[::-1]))
