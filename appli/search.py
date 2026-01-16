# -*- coding: utf-8 -*-
# This file is part of Ecotaxa, see license.md in the application root directory for license informations.
# Copyright (C) 2015-2016  Picheral, Colin, Irisson (UPMC-CNRS)
from appli import app,gvg,ntcv
from appli.database import  GetAll
from psycopg2.extensions import QuotedString
import json

@app.route('/search/taxo')
def searchtaxo():
    term=gvg("q")
    if len(term)<=2:
        return "[]"
    terms=[x.strip().lower()+R"%" for x in term.split('*')]
    # psycopg2.extensions.QuotedString("""c'est ok "ici" à  """).getquoted()
    param={'term':terms[-1]} # le dernier term est toujours dans la requete
    terms=[QuotedString(x).getquoted().decode('iso-8859-15','strict').replace("%","%%") for x in terms[0:-1]]
    ExtraWhere=ExtraFrom=""
    if terms:
        for t in terms:
            ExtraWhere +="\n and ("
            # SQLI insensible, protégé par quotedstring
            ExtraWhere +=' or '.join(("lower(p{0}.name) like {1}".format(i,t) for i in range(1,6)))+")"
        ExtraFrom="\n".join(["left join taxonomy_worms p{0} on p{1}.parent_id=p{0}.id".format(i,i-1) for i in range(2,6)])

    sql="""SELECT tf.id, tf.display_name as name
          ,0 FROM taxonomy_worms tf
          left join taxonomy_worms p1 on tf.parent_id=p1.id
          {0}
          WHERE  lower(tf.name) LIKE %(term)s  {1}
          order by tf.name limit 200""".format(ExtraFrom,ExtraWhere)

    res = GetAll(sql, param,debug=False)
    return json.dumps([dict(id=r[0],text=r[1],pr=r[2]) for r in res])

