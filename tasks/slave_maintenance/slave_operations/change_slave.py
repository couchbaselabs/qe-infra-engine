import sys
import os

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
import concurrent
import datetime
import json
from helper.sdk_helper.testdb_helper.slave_pool_helper import SlavePoolSDKHelper
from util.ssh_util.node_infra_helper.remote_connection_factory import RemoteConnectionObjectFactory
import tasks.slave_maintenance.slave_operations.add_slave as add_slave_task

logger = logging.getLogger("tasks")

def change_slaves_parallel(slave_data, max_workers=None):
    if not max_workers:
        num_cores = os.cpu_count()
        max_workers = num_cores if num_cores is not None else 10

    final_result = {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(change_slave, slave): slave for slave in slave_data}
        for future in concurrent.futures.as_completed(futures):
            slave = futures[future]
            result = future.result()
            final_result[slave["new_ipaddr"]] = result
            logger.critical(f"Deleting helper for remote {slave['new_ipaddr']}")
            RemoteConnectionObjectFactory.delete_helper(ipaddr=slave["new_ipaddr"])

def change_slave(slave):
    old_ipaddr = slave["old_ipaddr"]
    new_ipaddr = slave["new_ipaddr"]

    result = {}
    try:
        slave_pool_helper = SlavePoolSDKHelper()
        logger.info(f"Connection to Slave Pool successful")
    except Exception as e:
        result["result"] = False
        result["reason"] = f"Cannot connect to Slave Pool using SDK : {e}"
        logger.error(result["reason"])
        return result
    if old_ipaddr == new_ipaddr:
        try:
            slave_doc = slave_pool_helper.get_slave(old_ipaddr)
            logger.info(f"Doc with old ipaddr {old_ipaddr} successfully found in slave-pool")
        except Exception as e:
            result["result"] = False
            result["reason"] = f"Cannot find old ipaddr {old_ipaddr} doc in slave-pool : {e}"
            logger.error(result["reason"])
            return result
        
        # Can only change labels, name, description, state, origin, name_label, jenkins_host
        changeable_fields = ['labels', 'name', 'description', 'state', 'origin', 'name_label', 'jenkins_host']
        for field in changeable_fields:
            if field in slave:
                slave_doc[field] = slave[field]
        
        try:
            res = slave_pool_helper.upsert_slave_to_slave_pool(slave_doc)
            if not res:
                result["result"] = False
                result["reason"] = f"Cannot update slave {slave['ipaddr']} doc in slave pool"
                logger.error(result["reason"])
                return result
            logger.info(f"Document for slave {slave['ipaddr']} updated in slave pool successfuly")
        except Exception as e:
            result["result"] = False
            result["reason"] = f"Cannot update slave {slave['ipaddr']} doc in slave pool : {e}"
            logger.error(result["reason"])
            return result
        
        result["result"] = True
        result["new_doc"] = slave_doc
        return result
    
    else:
        try:
            res = slave_pool_helper.delete_slave(old_ipaddr)
            if res:
                logger.info(f"Doc with old ipaddr {old_ipaddr} successfully deleted")
            else:
                result["result"] = False
                result["reason"] = f"Cannot delete old ipaddr {old_ipaddr} doc in slave-pool : {e}"
                logger.error(result["reason"])
        except Exception as e:
            result["result"] = False
            result["reason"] = f"Cannot find old ipaddr {old_ipaddr} doc in slave-pool : {e}"
            logger.error(result["reason"])
            return result

        slave["ipaddr"] = new_ipaddr
        result = add_slave_task.add_slave(slave)
        result["delete_old_ipaddr"] = True
        return result

def parse_arguments():
    parser = argparse.ArgumentParser(description="A tool to change slaves in pool")
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
        slave_data = eval(args.data)
    except Exception as e:
        logger.error(f"The format of data is wrong : {e}")
        return

    logger.info(f"The number of slaves to change: {len(slave_data)}")

    for slave in slave_data:
        if "old_ipaddr" not in slave:
            logger.error("Field old_ipaddr missing from one of the slaves")
            return
        if "new_ipaddr" not in slave:
            logger.error("Field new_ipaddr missing from one of the slaves")
            return

    results = change_slaves_parallel(slave_data)

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
