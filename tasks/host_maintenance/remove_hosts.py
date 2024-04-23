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
import concurrent
import datetime
import json
from helper.sdk_helper.testdb_helper.host_pool_helper import HostSDKHelper

logger = logging.getLogger("tasks")

def _get_result_failure(reason, exception=None):
    result = {}
    result["result"] = False
    result["reason"] = f"{reason} : {str(exception)}" if exception else f"{reason}"
    logger.error(result["reason"])
    return result

def remove_hosts_parallel(hosts_data, max_workers=None):
    if not max_workers:
        num_cores = os.cpu_count()
        max_workers = num_cores if num_cores is not None else 1000

    final_result = {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(remove_host_docs, host): host for host in hosts_data}
        for future in concurrent.futures.as_completed(futures):
            host = futures[future]
            result = future.result()
            final_result[host["label"]] = result

    return final_result

def remove_host_docs(host):
    result = {}
    required_fields = ["label", "hostname"]
    for field in required_fields:
        if field not in host:
            return _get_result_failure(reason=f"Field {field} not present for node {host['label']}")

    logger.info(f"All required fields present for node {host['label']}")

    try:
        host_sdk_helper = HostSDKHelper()
        logger.info(f"Connection to Host Pool successful")
    except Exception as e:
        return _get_result_failure(reason="Cannot connect to Host Pool using SDK",
                                   exception=e)

    try:
        query_result = host_sdk_helper.fetch_vms_by_host(host["label"])
    except Exception as e:
        result["result"] = False
        result["reason"] = f"Cannot fetch all vms from host-pool : {e}"
        logger.error(result["reason"])
        return result

    vm_docs = []
    for row in query_result:
        row[host_sdk_helper.vm_collection_name]["doc_key"] = row["id"]
        vm_docs.append(row[host_sdk_helper.vm_collection_name])

    result["remove_vm_data"] = {}

    for doc in vm_docs:
        doc_key = doc["doc_key"]
        try:
            res = host_sdk_helper.remove_vm(doc_key)
            if not res:
                result["remove_vm_data"][doc_key] = _get_result_failure(reason=f"Cannot remove vm {doc_key} from host pool")
            else:
                logger.info(f"Document for vm {doc_key} removed from host pool successfuly")
                result["remove_vm_data"][doc_key] = True
        except Exception as e:
            result["remove_vm_data"][doc_key] =  _get_result_failure(reason=f"Cannot remove vm {doc_key} from host pool",
                                                            exception=e)
        
    result["remove_host_data"] = {}
    
    try:
        host_sdk_helper.remove_host(host["label"])
        if not res:
            result["remove_host_data"] = _get_result_failure(reason=f"Cannot remove host {host['label']} from host pool")
        else:
            logger.info(f"Document for host {host['label']} removed from host pool successfuly")
            result["remove_host_data"] = True
    except Exception as e:
        result["remove_host_data"] = _get_result_failure(reason=f"Cannot remove vm {host['label']} from host pool",
                                                         exception=e)
    
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
        logger.error("No host data is passed to add")
        return
    try:
        hosts_data = eval(args.data)
    except Exception as e:
        logger.error(f"The format of data is wrong : {e}")
        return

    logger.info(f"The number of hosts to remove: {len(hosts_data)}")

    for host in hosts_data:
        if "label" not in host:
            logger.error("Field label missing from one of the nodes")
            return
        if "hostname" not in host:
            logger.error("Field hostname missing from one of the nodes")
            return

    results = remove_hosts_parallel(hosts_data)

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
