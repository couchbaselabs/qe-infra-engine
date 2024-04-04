import sys
import os

# Adding project path into the sys paths for scanning all the modules
script_dir = os.path.dirname(os.path.realpath(__file__))
project_path = os.path.join(script_dir, "..", "..")
if project_path not in sys.path:
    sys.path.append(project_path)

import logging.config
import argparse
from constants.node_template import NODE_TEMPLATE
import copy
import concurrent
import datetime
import json
from helper.SDKHelper.testdb_helper.server_pool_helper import ServerPoolSDKHelper
from helper.RemoteConnectionHelper.remote_connection_factory import RemoteConnectionObjectFactory
import tasks.node_maintainance.add_nodes as add_node_task

logger = logging.getLogger("tasks")

def change_nodes_parallel(node_data, max_workers=None):
    if not max_workers:
        num_cores = os.cpu_count()
        max_workers = num_cores if num_cores is not None else 10

    final_result = {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(change_node, node): node for node in node_data}
        for future in concurrent.futures.as_completed(futures):
            node = futures[future]
            result = future.result()
            final_result[node["new_ipaddr"]] = result
            logger.critical(f"Deleting helper for remote {node['new_ipaddr']}")
            RemoteConnectionObjectFactory.delete_helper(ipaddr=node["new_ipaddr"])
    
    return final_result

def change_node(node):
    old_ipaddr = node["old_ipaddr"]
    new_ipaddr = node["new_ipaddr"]
    result = {}
    try:
        server_pool_helper = ServerPoolSDKHelper()
        logger.info(f"Connection to Server Pool successful")
    except Exception as e:
        result["result"] = False
        result["reason"] = f"Cannot connect to Server Pool using SDK : {e}"
        logger.error(result["reason"])
        return result
    
    try:
        res = server_pool_helper.delete_node(old_ipaddr)
        if res:
            logger.info(f"Doc with old ipaddr {old_ipaddr} successfully deleted")
        else:
            result["result"] = False
            result["reason"] = f"Cannot delete old ipaddr {old_ipaddr} doc in server-pool : {e}"
            logger.error(result["reason"])
    except Exception as e:
        result["result"] = False
        result["reason"] = f"Cannot find old ipaddr {old_ipaddr} doc in server-pool : {e}"
        logger.error(result["reason"])
        return result

    node["ipaddr"] = new_ipaddr
    result = add_node_task.add_node(node)
    result["delete_old_ipaddr"] = True
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

    logger.info(f"The number of nodes to change: {len(node_data)}")

    for node in node_data:
        if "old_ipaddr" not in node:
            logger.error("Field old_ipaddr missing from one of the nodes")
            return
        if "new_ipaddr" not in node:
            logger.error("Field new_ipaddr missing from one of the nodes")
            return

    results = change_nodes_parallel(node_data)

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
