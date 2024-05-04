import sys
import os

# Adding project path into the sys paths for scanning all the modules
script_dir = os.path.dirname(os.path.realpath(__file__))
project_paths = [
    os.path.join(script_dir, "..", "..", ".."),
    os.path.join(script_dir, "..", "..", "..", "util", "ssh_util")
]
for project_path in project_paths:
    if project_path not in sys.path:
        sys.path.append(project_path)

import logging.config
import argparse
from constants.doc_templates import SLAVE_TEMPLATE
import copy
import concurrent
import datetime
import json
from helper.sdk_helper.testdb_helper.slave_pool_helper import SlavePoolSDKHelper
from util.ssh_util.node_infra_helper.remote_connection_factory import RemoteConnectionObjectFactory

logger = logging.getLogger("tasks")

def add_slaves_parallel(slave_data, max_workers=None):
    if not max_workers:
        num_cores = os.cpu_count()
        max_workers = num_cores if num_cores is not None else 10

    final_result = {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(add_slave, slave): slave for slave in slave_data}
        for future in concurrent.futures.as_completed(futures):
            slave = futures[future]
            result = future.result()
            final_result[slave["ipaddr"]] = result
            logger.critical(f"Deleting helper for remote {slave['ipaddr']}")
            RemoteConnectionObjectFactory.delete_helper(ipaddr=slave["ipaddr"])

    return final_result

def add_slave(slave):
    result = {}
    required_fields = ["ssh_username", "ssh_password", "name_label", "name", "description", "labels", "origin", "jenkins_host"]
    for field in required_fields:
        if field not in slave:
            result["result"] = False
            result["reason"] = f"Field {field} not present for node {slave['ipaddr']}"
            logger.error(result["reason"])
            return result

    logger.info(f"All required fields present for node {slave['ipaddr']}")

    ipaddr = slave['ipaddr']
    ssh_username = slave['ssh_username']
    ssh_password = slave['ssh_password']

    try:
        slave_pool_helper = SlavePoolSDKHelper()
        logger.info(f"Connection to Slave Pool successful")
    except:
        result["result"] = False
        result["reason"] = f"Cannot connect to Slave Pool using SDK : {e}"
        logger.error(result["reason"])
        return result

    try:
        remote_connection_helper = RemoteConnectionObjectFactory.fetch_helper(ipaddr=ipaddr,
                                                                              ssh_username=ssh_username,
                                                                              ssh_password=ssh_password)
        logger.info(f"Connection to slave {slave['ipaddr']} successful")
    except Exception as e:
        result["result"] = False
        result["reason"] = f"Cannot connect to IP : {e}"
        logger.error(result["reason"])
        return result

    try:
        mac_address = remote_connection_helper.find_mac_address()
        logger.info(f"Mac address for slave {slave['ipaddr']} found successfuly")
    except Exception as e:
        result["result"] = False
        result["reason"] = f"Cannot find Mac address for slave {slave['ipaddr']} : {e}"
        logger.error(result["reason"])
        return result

    try:
        memory = remote_connection_helper.find_memory_total()
        logger.info(f"Total Memory for slave {slave['ipaddr']} found successfuly")
    except Exception as e:
        result["result"] = False
        result["reason"] = f"Cannot find Total memory for slave {slave['ipaddr']} : {e}"
        logger.error(result["reason"])
        return result

    try:
        short_os, os_version = remote_connection_helper.find_os_version()
        logger.info(f"Operating System for slave {slave['ipaddr']} found successfuly")
    except Exception as e:
        result["result"] = False
        result["reason"] = f"Cannot find OS version for slave {slave['ipaddr']} : {e}"
        logger.error(result["reason"])
        return result

    doc = copy.deepcopy(SLAVE_TEMPLATE)

    doc['ipaddr'] = slave['ipaddr']
    doc['name'] = slave['name']
    doc['description'] = slave['description']
    doc['labels'] = slave['labels']
    doc['os'] = short_os
    doc['os_version'] = os_version
    doc['state'] = 'running'
    doc['memory'] = memory
    doc['tags'] = {}
    doc['mac_address'] = mac_address
    doc['origin'] = slave['origin']
    doc['name_label'] = slave['name_label']
    doc['jenkins_host'] = slave['jenkins_host']

    try:
        result_init_slave = remote_connection_helper.initialize_slave()
        logger.info(f"Initialization for slave {slave['ipaddr']} completed successfuly")
    except Exception as e:
        result["result"] = False
        result["reason"] = f"Cannot Initialize slave {slave['ipaddr']}  : {e}"
        logger.error(result["reason"])
        return result

    try:
        res = slave_pool_helper.upsert_slave_to_slave_pool(doc)
        if not res:
            result["result"] = False
            result["reason"] = f"Cannot add slave {slave['ipaddr']} to slave pool"
            logger.error(result["reason"])
            return result
        logger.info(f"Document for slave {slave['ipaddr']} added to slave pool successfuly")
    except Exception as e:
        result["result"] = False
        result["reason"] = f"Cannot add slave {slave['ipaddr']} to slave pool : {e}"
        logger.error(result["reason"])
        return result

    result["result"] = True
    result["init_node_res"] = result_init_slave
    return result

def parse_arguments():
    parser = argparse.ArgumentParser(description="A tool to add slaves to pool")
    parser.add_argument("--data", type=str, help="The data of all slaves")
    return parser.parse_args()

def main():
    logging_conf_path = os.path.join(script_dir, "..", "..", "..", "logging.conf")
    logging.config.fileConfig(logging_conf_path)

    args = parse_arguments()
    if not args.data:
        logger.error("No slave data is passed to add")
        return
    try:
        node_data = eval(args.data)
    except Exception as e:
        logger.error(f"The format of data is wrong : {e}")
        return

    logger.info(f"The number of nodes to add: {len(node_data)}")

    for node in node_data:
        if "ipaddr" not in node:
            logger.error("Field ipaddr missing from one of the nodes")
            return

    results = add_nodes_parallel(node_data)

    current_time = datetime.datetime.now()
    timestamp_string = current_time.strftime('%Y_%m_%d_%H_%M_%S_%f')
    result_dir_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), "..", "..", "..", f"results_{timestamp_string}")
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

# TODO - Add a pipeline to add slave directly to jenkins
