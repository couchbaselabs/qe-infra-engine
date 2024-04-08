import sys
import os

# Adding project path into the sys paths for scanning all the modules
script_dir = os.path.dirname(os.path.realpath(__file__))
project_paths = [
    os.path.join(script_dir, "..", ".."),
    os.path.join(script_dir, "..", "..", "util", "ssh_util")
]
for project_path in project_paths:
    if project_path not in sys.path:
        sys.path.append(project_path)

import logging.config
import argparse
from constants.doc_templates import NODE_TEMPLATE
import copy
import concurrent
import datetime
import json
from helper.SDKHelper.testdb_helper.server_pool_helper import ServerPoolSDKHelper
from util.ssh_util.node_infra_helper.remote_connection_factory import RemoteConnectionObjectFactory

logger = logging.getLogger("tasks")

def add_nodes_parallel(node_data, max_workers=None):
    if not max_workers:
        num_cores = os.cpu_count()
        max_workers = num_cores if num_cores is not None else 10

    final_result = {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(add_node, node): node for node in node_data}
        for future in concurrent.futures.as_completed(futures):
            node = futures[future]
            result = future.result()
            final_result[node["ipaddr"]] = result
            logger.critical(f"Deleting helper for remote {node['ipaddr']}")
            RemoteConnectionObjectFactory.delete_helper(ipaddr=node["ipaddr"])
    
    return final_result

def add_node(node):
    logger.critical(f"Node number {node['count']}")
    result = {}
    required_fields = ["ssh_username", "ssh_password", "vm_name", "poolId", "origin"]
    for field in required_fields:
        if field not in node:
            result["result"] = False
            result["reason"] = f"Field {field} not present for node {node['ipaddr']}"
            logger.error(result["reason"])
            return result
    
    logger.info(f"All required fields present for node {node['ipaddr']}")

    ipaddr = node['ipaddr']
    ssh_username = node['ssh_username']
    ssh_password = node['ssh_password']

    try:
        server_pool_helper = ServerPoolSDKHelper()
        logger.info(f"Connection to Server Pool successful")
    except Exception as e:
        result["result"] = False
        result["reason"] = f"Cannot connect to Server Pool using SDK : {e}"
        logger.error(result["reason"])
        return result

    try:
        remote_connection_helper = RemoteConnectionObjectFactory.fetch_helper(ipaddr=ipaddr,
                                                                              ssh_username=ssh_username,
                                                                              ssh_password=ssh_password)
        logger.info(f"Connection to node {node['ipaddr']} successful")
    except Exception as e:
        result["result"] = False
        result["reason"] = f"Cannot connect to IP : {e}"
        logger.error(result["reason"])
        return result

    try:
        mac_address = remote_connection_helper.find_mac_address()
        logger.info(f"Mac address for node {node['ipaddr']} found successfuly")
    except Exception as e:
        result["result"] = False
        result["reason"] = f"Cannot find Mac address for node {node['ipaddr']} : {e}"
        logger.error(result["reason"])
        return result
    
    try:
        memory = remote_connection_helper.find_memory_total()
        logger.info(f"Total Memory for node {node['ipaddr']} found successfuly")
    except Exception as e:
        result["result"] = False
        result["reason"] = f"Cannot find Total memory for node {node['ipaddr']} : {e}"
        logger.error(result["reason"])
        return result
    
    try:
        short_os, os_version = remote_connection_helper.find_os_version()
        logger.info(f"Operating System for node {node['ipaddr']} found successfuly")
    except Exception as e:
        result["result"] = False
        result["reason"] = f"Cannot find OS version for node {node['ipaddr']} : {e}"
        logger.error(result["reason"])
        return result
    
    try:
        result_init_node = remote_connection_helper.initialize_node()
        logger.info(f"Initialization for node {node['ipaddr']} completed successfuly")
    except Exception as e:
        result["result"] = False
        result["reason"] = f"Cannot Initialize node {node['ipaddr']}  : {e}"
        logger.error(result["reason"])
        return result

    doc = copy.deepcopy(NODE_TEMPLATE)

    doc['ipaddr'] = node['ipaddr']
    doc["mac_address"] = mac_address
    doc["vm_name"] = node["vm_name"]
    doc["memory"] = memory
    doc["origin"] = node["origin"]
    doc["os"] = short_os
    doc["os_version"] = os_version
    doc["poolId"] = node["poolId"]
    doc["prevUser"] = ""
    doc["username"] = ""
    doc["state"] = "available"
    doc["tags"] = {}

    try:
        res = server_pool_helper.add_node_to_server_pool(doc)
        if not res:
            result["result"] = False
            result["reason"] = f"Cannot add node {node['ipaddr']} to server pool"
            logger.error(result["reason"])
            return result
        logger.info(f"Document for node {node['ipaddr']} added to server pool successfuly")
    except Exception as e:
        result["result"] = False
        result["reason"] = f"Cannot add node {node['ipaddr']} to server pool : {e}"
        logger.error(result["reason"])
        return result
    
    result["result"] = True
    result["init_node_res"] = result_init_node
    return result

def parse_arguments():
    parser = argparse.ArgumentParser(description="A tool to add VMs to pool")
    parser.add_argument("--data", type=str, help="The data of all VMs")
    return parser.parse_args()

def main():
    logging_conf_path = os.path.join(script_dir, "..", "..", "logging.conf")
    logging.config.fileConfig(logging_conf_path)

    args = parse_arguments()
    if not args.data:
        logger.error("No node data is passed to add")
        return
    try:
        node_data = eval(args.data)
    except Exception as e:
        logger.error(f"The format of data is wrong : {e}")
        return

    logger.info(f"The number of nodes to add: {len(node_data)}")

    i = 1
    for node in node_data:
        if "ipaddr" not in node:
            logger.error("Field ipaddr missing from one of the nodes")
            return
        node["count"] = i
        i+=1

    results = add_nodes_parallel(node_data)

    current_time = datetime.datetime.now()
    timestamp_string = current_time.strftime('%Y_%m_%d_%H_%M_%S_%f')
    result_dir_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), "..", "..", f"results_{timestamp_string}")
    if not os.path.exists(result_dir_path):
        logger.info(f"Creating directory {result_dir_path}")
        try:
            os.makedirs(result_dir_path)
        except Exception as e:
            logger.error(f"Error creating directory {result_dir_path} : {e}")
            return 
    logger.info(f"Successfully created directory {result_dir_path}")
    local_file_path = os.path.join(result_dir_path, f"result.json")
    with open(local_file_path, "w") as json_file:
        json.dump(results, json_file)


if __name__ == "__main__":
    main()
