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
import tasks.host_maintenance.add_hosts as add_hosts

logger = logging.getLogger("tasks")

def _get_result_failure(reason, exception=None):
    result = {}
    result["result"] = False
    result["reason"] = f"{reason} : {str(exception)}" if exception else f"{reason}"
    logger.error(result["reason"])
    return result

def update_hosts_parallel(hosts_data, max_workers=None):
    if not max_workers:
        num_cores = os.cpu_count()
        max_workers = num_cores if num_cores is not None else 1000

    final_result = {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(update_hosts_docs, host): host for host in hosts_data}
        for future in concurrent.futures.as_completed(futures):
            host = futures[future]
            result = future.result()
            final_result[host["label"]] = result

    return final_result

def update_hosts_docs(host_data):
    result = {}
    try:
        host_sdk_helper = HostSDKHelper()
        logger.info(f"Connection to Host Pool successful")
    except Exception as e:
        return _get_result_failure(reason="Cannot connect to Host Pool using SDK",
                                   exception=e)
    
    try:
        query_result = host_sdk_helper.fetch_vms_by_host(host_data["label"])
    except Exception as e:
        result["result"] = False
        result["reason"] = f"Cannot fetch all vms from host-pool : {e}"
        logger.error(result["reason"])
        return result
    
    vm_docs = []
    for row in query_result:
        row[host_sdk_helper.vm_collection_name]["doc_key"] = row["id"]
        vm_docs.append(row[host_sdk_helper.vm_collection_name])

    try:
        host_doc = eval(host_sdk_helper.fetch_host(host_data['label']))
    except Exception as e:
        result["result"] = False
        result["reason"] = f"Cannot fetch host doc for {host_data['label']} from host-pool : {e}"
        logger.error(result["reason"])
        return result
    
    host_data = {
        "username" : host_doc["xen_username"],
        "password" : host_doc["xen_password"],
        "group" : host_doc["group"],
        "label" : host_doc["name"]
    }

    result["update_host_data"] = add_hosts.add_host(host_data)
    if not result["update_host_data"]["result"]:
        return result
    
    result["remove_vm_data"] = {}
    for vm in vm_docs:
        if vm["name_label"] not in result["update_host_data"]["adding_vm_data"]:
            try:
                res = host_sdk_helper.remove_vm(vm["name_label"])
                if not res:
                    result["remove_vm_data"][vm["name_label"]] = _get_result_failure(reason=f"Cannot remove vm {vm['name_label']} from host pool")
                else:
                    logger.info(f"Document for vm {vm['name_label']} removed from host pool successfuly")
                    result["remove_vm_data"][vm["name_label"]] = True
            except Exception as e:
                result["remove_vm_data"][vm["name_label"]] =  _get_result_failure(reason=f"Cannot remove vm {vm['name_label']} from host pool",
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

    results = update_hosts_parallel(hosts_data)

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
