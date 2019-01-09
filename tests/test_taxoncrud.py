import requests,json

url_ret="http://127.0.0.1:5001"
instancedata={1:{'id_instance':1,'sharedsecret':'uVyDqG6L24NgpNDwkup3gXddUrjzrG6LYKAOksPOjHgqNPjZkKd2DTB2VzJVQAOI'}
            , 2:{'id_instance':2,'sharedsecret':'aVyDqG6L24NgpNDwkup3gXddUrjzrG6LYKAOksPOjHgqNPjZkKd2DTB2VzJVQAOI'} }

def request_withinstanceinfo(urlend,params,id_instance=1):
    params.update(instancedata[id_instance])
    r=requests.post(url_ret+urlend,params)
    return r.json()

def test_tooshort():
    j =request_withinstanceinfo("/settaxon/",{'taxotype':'P','name':'aa'})
    assert("too" in j['msg'])

def test_invalidtaxotype():
    j=request_withinstanceinfo("/checktaxon/",{'taxotype':'X','name':'test invalidetype'})
    assert("taxotype" in j['msg'])

def test_MorphoInvalidParent():
    j=request_withinstanceinfo("/checktaxon/",{'taxotype':'M','name':'missing parent','parent':'999999999'})
    assert("parent" in j['msg'])

def test_GetMissingFilterType():
    j = request_withinstanceinfo("/gettaxon/",{})
    assert("filtertype required" in j['msg'])

def test_ok():
    j=request_withinstanceinfo("/checktaxon/",{'taxotype':'P','name':'Avalid name'})
    assert("ok" in j['msg'])
    j = request_withinstanceinfo("/gettaxon/",{'filtertype':'id','id':'2'})
    assert("Eukaryota" ==j[0]['name'])

def test_security():
    #Test Update
    j =request_withinstanceinfo("/settaxon/",{'taxotype':'P','name':'Test-update','parent_id':1,'id':1})
    assert("unable" in j['msg'])

    # Test Delete
    j =request_withinstanceinfo("/deltaxon/",{'id':1})
    assert("unable" in j['msg'])

def test_crud():
    #Test Insert
    j =request_withinstanceinfo("/settaxon/",{'taxotype':'P','name':'Test-creation','parent_id':1})
    assert("ok" in j['msg'])
    id=j['id']
    j = request_withinstanceinfo("/gettaxon/",{'filtertype':'id','id':id})
    assert("Test-creation" ==j[0]['name'])
    assert("living>Test-creation" ==j[0]['display_name'])

    # test securité effacement avec une autre instance
    j =request_withinstanceinfo("/deltaxon/",{'id':id},2)
    assert("Only" in j['msg']) # echec de l'effacement

    #Test Update
    j =request_withinstanceinfo("/settaxon/",{'taxotype':'P','name':'Test-update','parent_id':1,'id':id})
    assert("ok" in j['msg'])
    id=j['id']
    j = request_withinstanceinfo("/gettaxon/",{'filtertype':'id','id':id})
    assert("Test-update" ==j[0]['name'])
    assert("living>Test-update" ==j[0]['display_name'])

    # Test Delete
    j =request_withinstanceinfo("/deltaxon/",{'id':id})
    assert("ok" in j['msg'])
