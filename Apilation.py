################################################################################################################################
# Import Packages
import requests
import urllib3
import time
################################################################################################################################
# Turn off insecure request warnings that result from unverified ssl certificates
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
################################################################################################################################
# Runs an Aggregation Pipeline on the specified source_collection in SonarG and outputs the results into the target_collection
def pipeline_output_collection(username,password,server,database,pipeline,source_collection,target_collection,append=True,other_args=None,
            print_info=True,gateway_port='8443',database_port='27117',host='localhost',subdatabase='lmrm__sonarg'):
    true_false_vars=[append,print_info]
    for var in true_false_vars:
        assert var in [True,False]
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
    if other_args is not None:
        assert isinstance(other_args,dict)
        for key,val in other_args.items():
            params[key]=val
    url=concat_url(server,gateway_port)
    if print_info:
        print("Running Pipeline: "+pipeline)
        start=time.time()
    response = requests.get(url=url,params=params,timeout=1,verify=False)
    if print_info:
        print("  Execution Time: "+"{:.2f}s".format(time.time()-start))
        print("  Response: "+response.text)
    return response
################################################################################################################################
# Runs an Aggregation Pipeline on the specified source_collection in SonarG and provides the results as a json dictionary
def pipeline_output_json(username,password,server,database,pipeline,source_collection,limit=None,other_args=None,
            print_info=True,gateway_port='8443',database_port='27117',host='localhost',subdatabase='lmrm__sonarg'):
    true_false_vars=[print_info]
    for var in true_false_vars:
        assert var in [True,False]
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
        params['limit']=str(limit)
    if other_args is not None:
        assert isinstance(other_args,dict)
        for key,val in other_args.items():
            params[key]=val
    url=concat_url(server,gateway_port)
    if print_info:
        print("Running Pipeline: "+pipeline)
        start=time.time()
    response = requests.get(url=url,params=params,timeout=1,verify=False).json()
    if print_info:
        print("  Execution Time: "+"{:.2f}s".format(time.time()-start))
        print("  Found: "+str(len(response))+" Records")
    return response
################################################################################################################################
# Runs an Aggregation Pipeline on the specified source_collection in SonarG and provides the results as a json dictionary
def insert_json_array(username,password,server,database,target_collection,json_array,other_args=None,
            print_info=True,gateway_port='8443',database_port='27117',host='localhost',subdatabase='lmrm__sonarg'):
    true_false_vars=[print_info]
    for var in true_false_vars:
        assert var in [True,False]
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
    if other_args is not None:
        assert isinstance(other_args,dict)
        for key,val in other_args.items():
            params[key]=val
    url=concat_url(server,gateway_port)
    if print_info:
        print("Inserting Data into: "+target_collection)
        start=time.time()
    response = requests.get(url=url,params=params,timeout=1,verify=False)
    if print_info:
        print("  Execution Time: "+"{:.2f}s".format(time.time()-start))
        print("  Response: "+response.text)
    return response
################################################################################################################################
# Structures the server url with its connection type and endpoint
def concat_url(server,port,con_type='https://',endpoint='Gateway'):
    return con_type+str(server)+':'+str(port)+'/'+endpoint
################################################################################################################################
