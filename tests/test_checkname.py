import requests,json

url_ret="http://127.0.0.1:5001"
instancedata={1:{'id_instance':1,'sharedsecret':'uVyDqG6L24NgpNDwkup3gXddUrjzrG6LYKAOksPOjHgqNPjZkKd2DTB2VzJVQAOI','ecotaxa_version':'10.0.0'}
            , 2:{'id_instance':2,'sharedsecret':'aVyDqG6L24NgpNDwkup3gXddUrjzrG6LYKAOksPOjHgqNPjZkKd2DTB2VzJVQAOI','ecotaxa_version':'10.0.0'} }

def request_withinstanceinfo(urlend,params,id_instance=1):
    params.update(instancedata[id_instance])
    r=requests.post(url_ret+urlend,params)
    return r.json()


def test_tooshort():
    j=request_withinstanceinfo("/checktaxon/",{'taxotype':'P','name':'aa'})
    assert("too" in j['msg'])

def test_invalidchar():
    j = request_withinstanceinfo("/checktaxon/",{'taxotype':'P','name':' XXX'})
    assert("white" in j['msg'])
    j = request_withinstanceinfo("/checktaxon/",{'taxotype':'P','name':'2 espaces  interdit'})
    assert("white" in j['msg'])
    j = request_withinstanceinfo("/checktaxon/",{'taxotype':'P','name':'3 espaces \t interdit'})
    assert("white" in j['msg'])

def test_invalidchar2():
    j=request_withinstanceinfo("/checktaxon/",{'taxotype':'P','name':'XX_X'})
    assert("letter" in j['msg'])

def test_invalidchartype():
    j=request_withinstanceinfo("/checktaxon/",{'taxotype':'X','name':'test invalidetype'})
    assert("taxotype" in j['msg'])

def test_MorphoMissingParent():
    j=request_withinstanceinfo("/checktaxon/",{'taxotype':'M','name':'missing parent'})
    assert("parent" in j['msg'])


def test_MorphoDuplicate(): # Clausocalanidae ❭ Microcalanus ❭ male
    j=request_withinstanceinfo("/checktaxon/",{'taxotype':'M','name':'male','parent_id':'80134'})
    assert("duplicate" in j['msg'])

def test_MorphoDuplicateForUpdate(): # Clausocalanidae > Microcalanus > male (92252)
    j=request_withinstanceinfo("/checktaxon/",{'taxotype':'M','name':'male','parent_id':'80134','updatetarget':'92252'})
    assert("ok" in j['msg'])

def test_PhyloDuplicate(): # Clausocalanidae ❭ Microcalanus ❭ male
    j=request_withinstanceinfo("/checktaxon/",{'taxotype':'P','name':'Microcalanus'})
    assert("duplicate" in j['msg'])

def test_PhyloDuplicateForUpdate(): # Clausocalanidae ❭ Microcalanus = 80134
    j=request_withinstanceinfo("/checktaxon/",{'taxotype':'P','name':'Microcalanus','updatetarget':'80134'})
    assert("ok" in j['msg'])

def test_ok():
    j=request_withinstanceinfo("/checktaxon/",{'taxotype':'P','name':'Avalid name'})
    assert("ok" in j['msg'])
    # Test - autorisé
    j=request_withinstanceinfo("/checktaxon/",{'taxotype':'P','name':'A-a'})
    assert("ok" in j['msg'])
    # Test + autorisé
    j=request_withinstanceinfo("/checktaxon/",{'taxotype':'P','name':'A+a'})
    assert("ok" in j['msg'])
    # Test ' autorisé
    j=request_withinstanceinfo("/checktaxon/",{'taxotype':'P','name':"A'a"})
    assert("ok" in j['msg'])
    # Test multiple mots autorisé
    j=request_withinstanceinfo("/checktaxon/",{'taxotype':'P','name':"Aaa bbb ccc ddd"})
    assert("ok" in j['msg'])
    # Test multiple mots autorisé
    j=request_withinstanceinfo("/checktaxon/",{'taxotype':'M', 'parent_id': '80125','name':"aaa bbb ccc ddd"})
    assert("ok" in j['msg'])


    # Test Chiffres autorisé
    # j=request_withinstanceinfo("/checktaxon/",{'taxotype':'P','name':'a5a'})
    # 
    # assert("ok" in j['msg'])

    # Test male sous Canthocalanus pauper (80125)
    j=request_withinstanceinfo("/checktaxon/",{'taxotype':'M','name':'male','parent_id':'80125'})
    assert("ok" in j['msg'])

