import requests

def pipeline_output_collection(username,password,server,database,pipeline,source_collection,target_collection,append=True,
                 gateway_port='8443',database_port='27117',host='localhost',subdatabase='lmrm__sonarg'):
    params={'username':username,
            'pwd':password,
            'db':database,
            'name':pipeline,
            'col':source_collection,
            'output':'collection.'+target_collection,
            'type':'agg',
            'host':host,
            'port':str(database_port),
            'sdb':subdatabase,
            'anyCert':'true'}
    if append is not True:
        params['dropTarget']='force'
    url=concat_url(server,gateway_port)
    return requests.get(url=url,params=params,timeout=1,verify=False)

def pipeline_output_json(username,password,server,database,pipeline,source_collection,limit=None,
                 gateway_port='8443',database_port='27117',host='localhost',subdatabase='lmrm__sonarg'):
    params={'username':username,
            'pwd':password,
            'db':database,
            'name':pipeline,
            'col':source_collection,
            'type':'agg',
            'host':host,
            'port':str(database_port),
            'sdb':subdatabase,
            'anyCert':'true'}
    if limit is not None:
        params['limit']=limit
    url=concat_url(server,gateway_port)
    return requests.get(url=url,params=params,timeout=1,verify=False)

def insert_json_array(username,password,server,database,target_collection,json_array,
                 gateway_port='8443',database_port='27117',host='localhost',subdatabase='lmrm__sonarg'):
    params={'username':username,
            'pwd':password,
            'db':database,
            'name':'',
            'col':target_collection,
            'type':'insert',
            'insertJson':json_array,
            'host':host,
            'port':str(database_port),
            'sdb':subdatabase,
            'anyCert':'true'}
    url=concat_url(server,gateway_port)
    return requests.get(url=url,params=params,timeout=1,verify=False)

def concat_url(server,port,con_type='https://',endpoint='Gateway'):
    return con_type+str(server)+':'+str(port)+'/'+endpoint