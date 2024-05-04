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
from helper.jenkins_helper.jenkins_helper_factory import JenkinsHelperFactory
import tasks.slave_maintenance.slave_operations.add_slave as add_slave_task

logger = logging.getLogger("tasks")

def remove_slaves_parallel(slave_data, max_workers=None):
    if not max_workers:
        num_cores = os.cpu_count()
        max_workers = num_cores if num_cores is not None else 10

    final_result = {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(remove_slave, slave): slave for slave in slave_data}
        for future in concurrent.futures.as_completed(futures):
            slave = futures[future]
            result = future.result()
            final_result[slave["ipaddr"]] = result

def remove_slave(slave):
    ipaddr = slave["ipaddr"]
    result = {}

    try:
        slave_pool_helper = SlavePoolSDKHelper()
        logger.info(f"Connection to Slave Pool successful")
    except Exception as e:
        result["result"] = False
        result["reason"] = f"Cannot connect to Slave Pool using SDK : {e}"
        logger.error(result["reason"])
        return result
    
    try:
        slave_doc = slave_pool_helper.get_slave(ipaddr)
        logger.info(f"Doc with ipaddr {ipaddr} successfully found in slave-pool")
    except Exception as e:
        result["result"] = False
        result["reason"] = f"Cannot find ipaddr {ipaddr} doc in slave-pool : {e}"
        logger.error(result["reason"])
        return result
    
    slave_name = slave_doc["name"]
    jenkins_host = slave_doc["jenkins_host"]

    try:
        jenkins_helper = JenkinsHelperFactory.fetch_helper(jenkins_host)
    except Exception as e:
        result["result"] = False
        result["reason"] = f"Cannot fetch helper for slave {ipaddr}:{slave_name} : {e}"
        logger.error(result["reason"])
        return result

    # Step 1 - Delete from jenkins
    try:
        res_jenkins = jenkins_helper.remove_slave(slave_name)
        logger.info(f"Slave with ipaddr {ipaddr} and name {slave_name} successfully removed from jenkins")
    except Exception as e:
        result["result"] = False
        result["reason"] = f"Cannot remove slave {ipaddr}:{slave_name} from jenkins : {e}"
        logger.error(result["reason"])
        return result
    
    # Step 2 - Delete from slave-pool
    try:
        res_slave_pool = slave_pool_helper.delete_slave(ipaddr)
        if res_slave_pool:
            logger.info(f"Doc with ipaddr {ipaddr} successfully deleted")
        else:
            result["result"] = False
            result["reason"] = f"Cannot delete ipaddr {ipaddr} doc in slave-pool : {e}"
            logger.error(result["reason"])
    except Exception as e:
        result["result"] = False
        result["reason"] = f"Cannot find ipaddr {ipaddr} doc in slave-pool : {e}"
        logger.error(result["reason"])
        return result
    
    result["remove_slave_from_jenkins"] = res_jenkins
    result["remove_slave_from_slave_pool"] = res_slave_pool
    result["result"] = True
    return result

def parse_arguments():
    parser = argparse.ArgumentParser(description="A tool to delete slaves in pool")
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

    logger.info(f"The number of slaves to delete: {len(slave_data)}")

    for slave in slave_data:
        if "ipaddr" not in slave:
            logger.error("Field new_ipaddr missing from one of the slaves")
            return

    results = remove_slaves_parallel(slave_data)

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
