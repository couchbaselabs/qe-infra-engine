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

from helper.sdk_helper.testdb_helper.server_pool_helper import ServerPoolSDKHelper
from util.ssh_util.node_infra_helper.remote_connection_factory import RemoteConnectionObjectFactory
import tasks.node_maintainance.node_health_monitor.node_health_monitor_utils as node_health_monitor_utils
import logging.config
import concurrent
import datetime
import json
import argparse

logger = logging.getLogger("tasks")

def fetch_tasks(tasks : list):
    tasks_dic = {}
    result = {}
    if len(tasks) == 0:
        tasks_dic = node_health_monitor_utils.ALL_TASKS_DIC
    else:
        for task in tasks:
            if task in node_health_monitor_utils.ALL_TASKS_DIC:
                tasks_dic[task] = node_health_monitor_utils.ALL_TASKS_DIC[task]
            else:
                result["result"] = False
                result["reason"] = f"Cannot find task : {task}"
                logger.error(result["reason"])
                return result
    result["result"] = True
    result["tasks_dic"] = tasks_dic
    return result

def fetch_docs(poolId : list):
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
        if len(poolId) > 0:
            query_result = server_pool_helper.fetch_nodes_by_poolId(poolId)
        else:
            query_result = server_pool_helper.fetch_all_nodes()
    except Exception as e:
        result["result"] = False
        result["reason"] = f"Cannot fetch all docs from server-pool : {e}"
        logger.error(result["reason"])
        return result

    docs = []
    for row in query_result:
        row["_default"]["doc_key"] = row["id"]
        docs.append(row["_default"])

    result["result"] = True
    result["docs"] = docs
    return result

def monitor_health_node(doc, tasks):
    result  = {}
    for task in tasks:
        logger.info(f'{task} for {doc["ipaddr"]}')
        result[task] = tasks[task](doc)
    return result

def monitor_health_nodes_parallel(docs, tasks, max_workers=None):
    if not max_workers:
        num_cores = os.cpu_count()
        max_workers = num_cores if num_cores is not None else 10

    final_result = {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(monitor_health_node, doc, tasks): doc for doc in docs}
        for future in concurrent.futures.as_completed(futures):
            doc = futures[future]
            result = future.result()
            final_result[doc["ipaddr"]] = result
            logger.critical(f"Deleting helper for remote {doc['ipaddr']}")
            RemoteConnectionObjectFactory.delete_helper(ipaddr=doc["ipaddr"])

    return final_result

def fetch_and_monitor_health(tasks, poolId):
    
    result_tasks = fetch_tasks(tasks)
    if not result_tasks["result"]:
        return result_tasks

    result_docs = fetch_docs(poolId)
    if not result_docs["result"]:
        return result_docs

    return monitor_health_nodes_parallel(docs=result_docs["docs"],
                                         tasks=result_tasks["tasks_dic"],
                                         max_workers=750)

def parse_arguments():
    parser = argparse.ArgumentParser(description="A tool to monitors nodes")
    parser.add_argument("--poolId", type=str, help="The poolId")
    parser.add_argument("--tasks", type=str, help="The tasks")
    return parser.parse_args()

def main():
    logging_conf_path = os.path.join(script_dir, "..", "..", "..", "logging.conf")
    logging.config.fileConfig(logging_conf_path)

    args = parse_arguments()
    if not args.poolId:
        args.poolId = []
    else:
        try:
            args.poolId = eval(args.poolId)
        except Exception as e:
            logger.error(f"The format of poolId is wrong : {e}")
            return
    if not args.tasks:
        args.tasks = []
    else:
        try:
            args.tasks = eval(args.tasks)
        except Exception as e:
            logger.error(f"The format of tasks is wrong : {e}")
            return

    results = fetch_and_monitor_health(poolId=args.poolId,
                                       tasks=args.tasks)

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