from helper.sdk_helper.testdb_helper.host_pool_helper import HostSDKHelper
from helper.sdk_helper.testdb_helper.server_pool_helper import ServerPoolSDKHelper
import pandas as pd

def convert_tags_to_fields(tags, result):
    for tag in tags:
        if tag is isinstance(dict):
            convert_tags_to_fields(tag, result)
        else:
            result[tag] = tags[tag] 

def convert_all_nodes_csv():
    sdk_helper = ServerPoolSDKHelper()
    query_result = sdk_helper.fetch_all_nodes()
    docs = []
    for row in query_result:
        doc = row[sdk_helper.server_pool_collection]
        doc["doc_key"] = row["id"]
        
        if "tags" in doc:
            tags =  doc["tags"] 
            tags_result = {}
            convert_tags_to_fields(tags, tags_result)
            for tag in tags_result:
                doc[tag] = tags_result[tag]
            doc.pop("tags", None)

        docs.append(doc)
    df = pd.DataFrame(docs)
    df.to_csv('all_nodes.csv', index=False)

def convert_all_hosts_csv():
    sdk_helper = HostSDKHelper()
    query_result = sdk_helper.fetch_all_host()
    docs = []
    for row in query_result:
        doc = row[sdk_helper.host_collection_name]
        doc["doc_key"] = row["id"]
        
        if "tags" in doc:
            tags =  doc["tags"] 
            tags_result = {}
            convert_tags_to_fields(tags, tags_result)
            for tag in tags_result:
                doc[tag] = tags_result[tag]
            doc.pop("tags", None)
        
        docs.append(doc)
    df = pd.DataFrame(docs)
    df.to_csv('all_hosts.csv', index=False)

def convert_all_vms_csv():
    sdk_helper = HostSDKHelper()
    query_result = sdk_helper.fetch_all_vms()
    docs = []
    for row in query_result:
        doc = row[sdk_helper.vm_collection_name]
        doc["doc_key"] = row["id"]
        
        if "tags" in doc:
            tags =  doc["tags"] 
            tags_result = {}
            convert_tags_to_fields(tags, tags_result)
            for tag in tags_result:
                doc[tag] = tags_result[tag]
            doc.pop("tags", None)
            
        docs.append(doc)
    df = pd.DataFrame(docs)
    df.to_csv('all_hosts.csv', index=False)

convert_all_vms_csv()
convert_all_hosts_csv()
convert_all_nodes_csv()
