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

from helper.sdk_helper.testdb_helper.host_pool_helper import HostSDKHelper
from util.ssh_util.node_infra_helper.remote_connection_factory import RemoteConnectionObjectFactory
import tasks.host_maintenance.host_health_monitor.host_health_monitor_util as host_health_monitor_utils
import tasks.host_maintenance.host_health_monitor.vm_health_monitor_util as vm_health_monitor_utils
import logging.config
import concurrent
import datetime
import json
import argparse

logger = logging.getLogger("tasks")

def fetch_tasks(tasks : list, type : str):
    all_tasks_dic = None
    if type == "host_tasks":
        all_tasks_dic = host_health_monitor_utils.ALL_TASKS_DIC
    elif type == "vm_tasks":
        all_tasks_dic = vm_health_monitor_utils.ALL_TASKS_DIC
    else:
        result["result"] = False
        result["reason"] = f"Cannot find task type : {type}"
        logger.error(result["reason"])
        return result

    tasks_dic = {}
    result = {}
    if len(tasks) == 0:
        tasks_dic = all_tasks_dic
    else:
        for task in tasks:
            if task in all_tasks_dic:
                tasks_dic[task] = all_tasks_dic[task]
            else:
                result["result"] = False
                result["reason"] = f"Cannot find task : {task}"
                logger.error(result["reason"])
                return result
    result["result"] = True
    result["tasks_dic"] = tasks_dic
    return result

def fetch_vms(host):
    result = {}
    try:
        host_pool_helper = HostSDKHelper()
        logger.info(f"Connection to Host Pool successful")
    except Exception as e:
        result["result"] = False
        result["reason"] = f"Cannot connect to Host Pool using SDK : {e}"
        logger.error(result["reason"])
        return result
    try:
        query_result = host_pool_helper.fetch_vms_by_host(host)
    except Exception as e:
        result["result"] = False
        result["reason"] = f"Cannot fetch all hosts from host-pool : {e}"
        logger.error(result["reason"])
        return result

    vm_docs = []
    for row in query_result:
        row[host_pool_helper.vm_collection_name]["doc_key"] = row["id"]
        vm_docs.append(row[host_pool_helper.vm_collection_name])
    result["result"] = True
    result["vm_docs"] = vm_docs
    return result

def fetch_hosts(group : list):
    result = {}
    try:
        host_pool_helper = HostSDKHelper()
        logger.info(f"Connection to Host Pool successful")
    except Exception as e:
        result["result"] = False
        result["reason"] = f"Cannot connect to Host Pool using SDK : {e}"
        logger.error(result["reason"])
        return result
    try:
        if len(group) > 0:
            query_result = host_pool_helper.fetch_hosts_by_group(group)
        else:
            query_result = host_pool_helper.fetch_all_host()
    except Exception as e:
        result["result"] = False
        result["reason"] = f"Cannot fetch all hosts from host-pool : {e}"
        logger.error(result["reason"])
        return result

    host_docs = []
    for row in query_result:
        row[host_pool_helper.host_collection_name]["doc_key"] = row["id"]
        host_docs.append(row[host_pool_helper.host_collection_name])

    result["result"] = True
    result["host_docs"] = host_docs
    return result

def monitor_health_host(doc, vm_docs, tasks):
    result  = {}
    for task in tasks:
        logger.info(f'{task} for {doc["name"]}')
        result[task] = tasks[task](doc, vm_docs)
    return result

def monitor_health_vm(doc, tasks):
    result  = {}
    for task in tasks:
        logger.info(f'{task} for {doc["name_label"]}')
        result[task] = tasks[task](doc)
    return result

def monitor_health_hosts_parallel(host_docs, host_tasks, vm_tasks, max_workers=None):
    if not max_workers:
        num_cores = os.cpu_count()
        max_workers = num_cores if num_cores is not None else 10

    final_result = {}
    final_result_hosts = {}
    final_result_vms = {}

    if "update doc" in host_tasks:
        logger.info(f'Updating documents for hosts')
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures_hosts = {executor.submit(host_tasks["update doc"], host_doc) : host_doc for host_doc in host_docs}
        final_result_hosts["update_docs_task"] = {}
        for future in concurrent.futures.as_completed(futures_hosts):
            doc = futures_hosts[future]
            result = future.result()
            final_result_hosts["update_docs_task"][doc["name"]] = result
        host_tasks.pop("update doc", None)

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures_hosts = {}
        futures_vms = {}
        for host_doc in host_docs:
            vm_docs_result = fetch_vms(host_doc["name"])
            if not vm_docs_result["result"]:
                return vm_docs_result
            vm_docs = vm_docs_result["vm_docs"]

            futures_hosts[executor.submit(monitor_health_host, host_doc, vm_docs, host_tasks)] = host_doc

            for vm_doc in vm_docs:
                futures_vms[executor.submit(monitor_health_vm, vm_doc, vm_tasks)] = vm_doc

        for future in concurrent.futures.as_completed(futures_hosts):
            doc = futures_hosts[future]
            result = future.result()
            final_result_hosts[doc["name"]] = result

        for future in concurrent.futures.as_completed(futures_vms):
            doc = futures_vms[future]
            result = future.result()
            final_result_vms[doc["name_label"]] = result

    final_result["host_tasks"] = final_result_hosts
    final_result["vm_tasks"] = final_result_vms
    return final_result

def fetch_and_monitor_health(host_tasks, vm_tasks, group):

    result_host_tasks = fetch_tasks(tasks=host_tasks,
                                    type="host_tasks")
    if not result_host_tasks["result"]:
        return result_host_tasks

    result_vm_tasks = fetch_tasks(tasks=vm_tasks,
                                  type="vm_tasks")
    if not result_vm_tasks["result"]:
        return result_vm_tasks

    result_host_docs = fetch_hosts(group)
    if not result_host_docs["result"]:
        return result_host_docs

    return monitor_health_hosts_parallel(host_docs=result_host_docs["host_docs"],
                                         host_tasks=result_host_tasks["tasks_dic"],
                                         vm_tasks=result_vm_tasks["tasks_dic"],
                                         max_workers=750)

def parse_arguments():
    parser = argparse.ArgumentParser(description="A tool to monitors nodes")
    parser.add_argument("--group", type=str, help="The group")
    parser.add_argument("--host_tasks", type=str, help="The tasks")
    parser.add_argument("--vm_tasks", type=str, help="The tasks")
    return parser.parse_args()

def main():
    logging_conf_path = os.path.join(script_dir, "..", "..", "..", "logging.conf")
    logging.config.fileConfig(logging_conf_path)

    args = parse_arguments()
    if not args.group:
        args.group = []
    else:
        try:
            args.group = eval(args.group)
        except Exception as e:
            logger.error(f"The format of group is wrong : {e}")
            return
    if not args.host_tasks:
        args.host_tasks = []
    else:
        try:
            args.host_tasks = eval(args.host_tasks)
        except Exception as e:
            logger.error(f"The format of tasks is wrong : {e}")
            return

    if not args.vm_tasks:
        args.vm_tasks = []
    else:
        try:
            args.vm_tasks = eval(args.vm_tasks)
        except Exception as e:
            logger.error(f"The format of tasks is wrong : {e}")
            return

    results = fetch_and_monitor_health(group=args.group,
                                       host_tasks=args.host_tasks,
                                       vm_tasks=args.vm_tasks)

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