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

from helper.sdk_helper.testdb_helper.host_pool_helper import HostSDKHelper
from helper.sdk_helper.testdb_helper.server_pool_helper import ServerPoolSDKHelper
from util.ssh_util.node_infra_helper.remote_connection_factory import RemoteConnectionObjectFactory
from constants.doc_templates import NODE_TEMPLATE
import logging.config
import paramiko, paramiko.ssh_exception
import socket
import concurrent
import copy
import datetime
import json
import argparse

logger = logging.getLogger("tasks")

# TODO tasks
'''
1. Checking state and move from booked to available if its in booked state for more than 48hrs
2. Checking status of ntp
3. Checking status of directories and permissions on the nodes
4. Checking status of reserved nodes
'''

def check_connectivity_node(doc):
    result = {}
    ipaddr = doc["ipaddr"]

    try:
        server_pool_helper = ServerPoolSDKHelper()
        logger.info(f"Connection to Server Pool successful")
    except Exception as e:
        result["result"] = False
        result["reason"] = f"Cannot connect to Server Pool using SDK : {e}"
        logger.error(result["reason"])
        return result

    if "tags" not in doc:
        doc["tags"] = {}

    try:
        RemoteConnectionObjectFactory.fetch_helper(ipaddr,"root","couchbase")
        doc["tags"]["connection_check"] = True
        # doc["state"] = "available" if doc["state"] == "unreachable" else doc["state"]
    except Exception as e:
        doc["tags"]["connection_check"] = False
        # doc["state"] = "unreachable"

    try:
        res = server_pool_helper.add_node_to_server_pool(doc)
        if not res:
            result["result"] = False
            result["reason"] = f"Cannot upsert node {ipaddr} with node-connectivity checks to server pool"
            logger.error(result["reason"])
            return result
        logger.info(f"Document for node {ipaddr} with node-connectivity checks upserted to server pool successfuly")
    except Exception as e:
        result["result"] = False
        result["reason"] = f"Cannot upsert node {ipaddr} with node-connectivity checks to server pool : {e}"
        logger.error(result["reason"])
        return result

    result["result"] = True
    result["connection_check"] = doc["tags"]["connection_check"]
    return result

# TODO - Remove this post cleanup
def check_connectivity_node_2(doc):
    ipaddr = doc["ipaddr"]
    result = {}

    try:
        server_pool_helper = ServerPoolSDKHelper()
        logger.info(f"Connection to Server Pool successful")
    except Exception as e:
        result["result"] = False
        result["reason"] = f"Cannot connect to Server Pool using SDK : {e}"
        logger.error(result["reason"])
        return result

    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    retries = 5
    connection = False
    connection_errors = set()
    # while retries > 0:
    for retry in range(retries):
        logger.info(f'Checking ssh connectivity for node {ipaddr} retry {retry + 1} / {retries}')
        try:
            ssh.connect(ipaddr,
                        username="root",
                        password="couchbase")
            ssh.close()
            connection = True
            connection_errors.add(None)
            break
        except paramiko.PasswordRequiredException as e:
            connection = False
            connection_errors.add("paramiko.PasswordRequiredException")
            logger.error(f'Unable to connect to {ipaddr} : {e}')
        except paramiko.BadAuthenticationType as e:
            connection = False
            connection_errors.add("paramiko.BadAuthenticationType")
            logger.error(f'Unable to connect to {ipaddr} : {e}')
        except paramiko.AuthenticationException as e:
            connection = False
            connection_errors.add("paramiko.AuthenticationException")
            logger.error(f'Unable to connect to {ipaddr} : {e}')
        except paramiko.BadHostKeyException as e:
            connection = False
            connection_errors.add("paramiko.BadHostKeyException")
            logger.error(f'Unable to connect to {ipaddr} : {e}')
        except paramiko.ChannelException as e:
            connection = False
            connection_errors.add("paramiko.ChannelException")
            logger.error(f'Unable to connect to {ipaddr} : {e}')
        except paramiko.ProxyCommandFailure as e:
            connection = False
            connection_errors.add("paramiko.ProxyCommandFailure")
            logger.error(f'Unable to connect to {ipaddr} : {e}')
        except paramiko.ConfigParseError as e:
            connection = False
            connection_errors.add("paramiko.ConfigParseError")
            logger.error(f'Unable to connect to {ipaddr} : {e}')
        except paramiko.CouldNotCanonicalize as e:
            connection = False
            connection_errors.add("paramiko.CouldNotCanonicalize")
            logger.error(f'Unable to connect to {ipaddr} : {e}')
        except paramiko.ssh_exception.NoValidConnectionsError as e:
            connection = False
            connection_errors.add("paramiko.NoValidConnectionsError")
            logger.error(f'Unable to connect to {ipaddr} : {e}')
        except socket.timeout as e:
            connection = False
            connection_errors.add("socket.timeout")
            logger.error(f'Unable to connect to {ipaddr} : {e}')
        except paramiko.SSHException as e:
            connection = False
            connection_errors.add("paramiko.SSHException")
            logger.error(f'Unable to connect to {ipaddr} : {e}')
        except socket.error as e:
            connection = False
            connection_errors.add("socket.error")
            logger.error(f'Unable to connect to {ipaddr} : {e}')
        except Exception as e:
            connection = False
            connection_errors.add("generic.Exception")
            logger.error(f'Unable to connect to {ipaddr} : {e}')

    if "tags" not in doc:
        doc["tags"] = {}

    doc["tags"]["connection_check"] = connection

    if len(connection_errors) > 1:
        doc["tags"]["connection_check_err"] = ' '.join(str(item) for item in connection_errors)

    try:
        res = server_pool_helper.add_node_to_server_pool(doc)
        if not res:
            result["result"] = False
            result["reason"] = f"Cannot upsert node {ipaddr} with node-connectivity checks to server pool"
            logger.error(result["reason"])
            return result
        logger.info(f"Document for node {ipaddr} with node-connectivity checks upserted to server pool successfuly")
    except Exception as e:
        result["result"] = False
        result["reason"] = f"Cannot upsert node {ipaddr} with node-connectivity checks to server pool : {e}"
        logger.error(result["reason"])
        return result

    result["result"] = True
    result["connection_check"] = doc["tags"]["connection_check"]
    if len(connection_errors) > 1:
        result["connection_check_err"] = doc["tags"]["connection_check_err"]
    return result

def check_field_consistency(doc):
    result = {}
    ipaddr = doc["ipaddr"]

    try:
        server_pool_helper = ServerPoolSDKHelper()
        logger.info(f"Connection to Server Pool successful")
    except Exception as e:
        result["result"] = False
        result["reason"] = f"Cannot connect to Server Pool using SDK : {e}"
        logger.error(result["reason"])
        return result
    
    fields_required = list(NODE_TEMPLATE.keys())
    # TODO - Remove the following line after server-pool cleanup
    fields_required.append("doc_key")

    fields_absent = []
    for field in fields_required:
        if field not in doc:
            fields_absent.append(field)

    fields_extra = []
    for field in doc:
        if field not in fields_required:
            fields_extra.append(field)

    if len(fields_absent) == 0 and len(fields_extra) == 0:
        doc["tags"]["field_consistency"] = {
            "fields_match" : True
        }
    else:
        doc["tags"]["field_consistency"] = {
            "fields_match" : False
        }
        if len(fields_absent) > 0:
            doc["tags"]["field_consistency"]["fields_absent"] = fields_absent
        if len(fields_extra) > 0:
            doc["tags"]["field_consistency"]["fields_extra"] = fields_extra

    try:
        res = server_pool_helper.add_node_to_server_pool(doc)
        if not res:
            result["result"] = False
            result["reason"] = f"Cannot upsert node {ipaddr} with field_consistency checks to server pool"
            logger.error(result["reason"])
            return result
        logger.info(f"Document for node {ipaddr} with field_consistency checks upserted to server pool successfuly")
    except Exception as e:
        result["result"] = False
        result["reason"] = f"Cannot upsert node {ipaddr} with field_consistency checks to server pool : {e}"
        logger.error(result["reason"])
        return result

    result["result"] = True
    result["field_consistency"] = doc["tags"]["field_consistency"]
    return result

def check_node_mac_addr_match(doc):
    result = {}
    ipaddr = doc["ipaddr"]

    # TODO - Remove post cleaning up of server pool
    if "tags" not in doc and "connection_check" not in doc["tags"]:
        result["result"] = False
        result["reason"] = f"OS version check was run before connection checks for {ipaddr}"
        logger.error(result["reason"])
        return result
    elif not doc["tags"]["connection_check"]:
        result["result"] = False
        result["reason"] = f"The node is unreachable, cannot perform os version checks for {ipaddr}"
        logger.error(result["reason"])
        return result

    try:
        server_pool_helper = ServerPoolSDKHelper()
        logger.info(f"Connection to Server Pool successful")
    except Exception as e:
        result["result"] = False
        result["reason"] = f"Cannot connect to Server Pool using SDK : {e}"
        logger.error(result["reason"])
        return result

    try:
        remote_connection_helper = RemoteConnectionObjectFactory.fetch_helper(ipaddr,"root","couchbase")
    except Exception as e:
        result["result"] = False
        result["reason"] = f"The node {ipaddr} is unreachable, cannot perform os version checks : {e}"
        logger.error(result["reason"])
        return result

    try:
        mac_address_in_node = remote_connection_helper.find_mac_address()
    except Exception as e:
        result["result"] = False
        result["reason"] = f"Could not find mac adddress for node {ipaddr} : {e}"
        logger.error(result["reason"])
        return result

    if "mac_address" in doc and mac_address_in_node == doc["mac_address"]:
        doc["tags"]["mac_address_node_check"] = {
            "mac_address_node_match" : True
        }
    else:
        doc["tags"]["mac_address_node_check"] = {
            "mac_address_node_match" : False,
            "mac_address_in_node" : mac_address_in_node
        }

    try:
        res = server_pool_helper.add_node_to_server_pool(doc)
        if not res:
            result["result"] = False
            result["reason"] = f"Cannot upsert node {ipaddr} with node-mac_address-consistency checks to server pool"
            logger.error(result["reason"])
            return result
        logger.info(f"Document for node {ipaddr} with node-mac_address-consistency checks upserted to server pool successfuly")
    except Exception as e:
        result["result"] = False
        result["reason"] = f"Cannot upsert node {ipaddr} with node-mac_address-consistency checks to server pool : {e}"
        logger.error(result["reason"])
        return result

    result["result"] = True
    result["os_node_match"] = doc["tags"]["mac_address_node_check"]
    return result

def check_node_memory_match(doc):
    result = {}
    ipaddr = doc["ipaddr"]

    # TODO - Remove post cleaning up of server pool
    if "tags" not in doc and "connection_check" not in doc["tags"]:
        result["result"] = False
        result["reason"] = f"OS version check was run before connection checks for {ipaddr}"
        logger.error(result["reason"])
        return result
    elif not doc["tags"]["connection_check"]:
        result["result"] = False
        result["reason"] = f"The node is unreachable, cannot perform os version checks for {ipaddr}"
        logger.error(result["reason"])
        return result

    try:
        server_pool_helper = ServerPoolSDKHelper()
        logger.info(f"Connection to Server Pool successful")
    except Exception as e:
        result["result"] = False
        result["reason"] = f"Cannot connect to Server Pool using SDK : {e}"
        logger.error(result["reason"])
        return result

    try:
        remote_connection_helper = RemoteConnectionObjectFactory.fetch_helper(ipaddr,"root","couchbase")
    except Exception as e:
        result["result"] = False
        result["reason"] = f"The node {ipaddr} is unreachable, cannot perform os version checks : {e}"
        logger.error(result["reason"])
        return result

    try:
        memory_in_node = remote_connection_helper.find_memory_total()
    except Exception as e:
        result["result"] = False
        result["reason"] = f"Could not find total memory for node {ipaddr} : {e}"
        logger.error(result["reason"])
        return result

    if "memory" in doc and memory_in_node == doc["memory"]:
        doc["tags"]["memory_node_check"] = {
            "memory_node_match" : True
        }
    else:
        doc["tags"]["memory_node_check"] = {
            "memory_node_match" : False,
            "memory_in_node" : memory_in_node
        }

    try:
        res = server_pool_helper.add_node_to_server_pool(doc)
        if not res:
            result["result"] = False
            result["reason"] = f"Cannot upsert node {ipaddr} with node-memory-consistency checks to server pool"
            logger.error(result["reason"])
            return result
        logger.info(f"Document for node {ipaddr} with node-memory-consistency checks upserted to server pool successfuly")
    except Exception as e:
        result["result"] = False
        result["reason"] = f"Cannot upsert node {ipaddr} with node-memory-consistency checks to server pool : {e}"
        logger.error(result["reason"])
        return result

    result["result"] = True
    result["os_node_match"] = doc["tags"]["memory_node_check"]
    return result

def check_node_os_match(doc):
    result = {}
    ipaddr = doc["ipaddr"]

    # TODO - Remove post cleaning up of server pool
    if "tags" not in doc and "connection_check" not in doc["tags"]:
        result["result"] = False
        result["reason"] = f"OS version check was run before connection checks for {ipaddr}"
        logger.error(result["reason"])
        return result
    elif not doc["tags"]["connection_check"]:
        result["result"] = False
        result["reason"] = f"The node is unreachable, cannot perform os version checks for {ipaddr}"
        logger.error(result["reason"])
        return result

    try:
        server_pool_helper = ServerPoolSDKHelper()
        logger.info(f"Connection to Server Pool successful")
    except Exception as e:
        result["result"] = False
        result["reason"] = f"Cannot connect to Server Pool using SDK : {e}"
        logger.error(result["reason"])
        return result

    try:
        remote_connection_helper = RemoteConnectionObjectFactory.fetch_helper(ipaddr,"root","couchbase")
    except Exception as e:
        result["result"] = False
        result["reason"] = f"The node {ipaddr} is unreachable, cannot perform os version checks : {e}"
        logger.error(result["reason"])
        return result

    try:
        os_node = remote_connection_helper.find_os_version()
    except Exception as e:
        result["result"] = False
        result["reason"] = f"Could not find os version for node {ipaddr} : {e}"
        logger.error(result["reason"])
        return result

    if "os_version" in doc and os_node == doc["os_version"]:
        doc["tags"]["os_node_check"] = {
            "os_node_match" : True
        }
    else:
        doc["tags"]["os_node_check"] = {
            "os_node_match" : False,
            "os_in_node" : os_node
        }

    try:
        res = server_pool_helper.add_node_to_server_pool(doc)
        if not res:
            result["result"] = False
            result["reason"] = f"Cannot upsert node {ipaddr} with node-os_version-consistency checks to server pool"
            logger.error(result["reason"])
            return result
        logger.info(f"Document for node {ipaddr} with node-os_version-consistency checks upserted to server pool successfuly")
    except Exception as e:
        result["result"] = False
        result["reason"] = f"Cannot upsert node {ipaddr} with node-os_version-consistency checks to server pool : {e}"
        logger.error(result["reason"])
        return result

    result["result"] = True
    result["os_node_match"] = doc["tags"]["os_node_check"]
    return result

def check_node_with_host_pool(doc):
    result = {}
    ipaddr = doc["ipaddr"]
    try:
        host_sdk_helper = HostSDKHelper()
        logger.info(f"Connection to Host Pool successful")
    except Exception as e:
        result["result"] = False
        result["reason"] = f"Cannot connect to Host Pool using SDK : {e}"
        logger.error(result["reason"])
        return result

    try:
        server_pool_helper = ServerPoolSDKHelper()
        logger.info(f"Connection to Server Pool successful")
    except Exception as e:
        result["result"] = False
        result["reason"] = f"Cannot connect to Server Pool using SDK : {e}"
        logger.error(result["reason"])
        return result

    try:
        vms = host_sdk_helper.fetch_vm(ipaddr=ipaddr)
    except Exception as e:
        result["result"] = False
        result["reason"] = f"Cannot fetch vm {ipaddr} from Host Pool using SDK : {e}"
        logger.error(result["reason"])
        return result

    try:
        vms = [vm[host_sdk_helper.vm_collection_name] for vm in vms]
    except Exception as e:
        result["result"] = False
        result["reason"] = f"Unable to parse query result from Host Pool using SDK : {e}"
        logger.error(result["reason"])
        return result

    if "tags" not in doc:
        doc["tags"] = {}

    if len(vms) == 0:
        doc["tags"]["ip_in_host_pool"] = False
    else:
        doc["tags"]["ip_in_host_pool"] = True

        vm = vms[0]
        if "origin" in doc and vm["host"] == doc["origin"]:
                doc["tags"]["origin_host_pool"] = {
                    "origin_match" : True
                }
        else:
            doc["tags"]["origin_host_pool"] = {
                "origin_match" : False,
                "origin_host_pool" : vm["host"]
            }

        if "vm_name" in doc and vm["name_label"] == doc["vm_name"]:
            doc["tags"]["vm_name_host_pool"] = {
                "vm_name_match" : True
            }
        else:
            doc["tags"]["vm_name_host_pool"] = {
                "vm_name_match" : False,
                "vm_name_host_pool" : vm["name_label"]
            }

        if "os_version" not in vm:
            vm["os_version"] = "unknown"

        if "os_version" in doc and vm["os_version"] == doc["os_version"]:
            doc["tags"]["os_version_host_pool"] = {
                "os_version_match" : True
            }
        else:
            doc["tags"]["os_version_host_pool"] = {
                "os_version_match" : False,
                "os_version_host_pool" : vm["os_version"]
            }

    try:
        res = server_pool_helper.add_node_to_server_pool(doc)
        if not res:
            result["result"] = False
            result["reason"] = f"Cannot upsert node {ipaddr} with host-pool-consistency checks to server pool"
            logger.error(result["reason"])
            return result
        logger.info(f"Document for node {ipaddr} with host-pool-consistency checks upserted to server pool successfuly")
    except Exception as e:
        result["result"] = False
        result["reason"] = f"Cannot upsert node {ipaddr} with host-pool-consistency checks to server pool : {e}"
        logger.error(result["reason"])
        return result

    result["result"] = True
    result["ip_in_host_pool"] = doc["tags"]["ip_in_host_pool"]
    if result["ip_in_host_pool"]:
        result["origin_host_pool"] = doc["tags"]["origin_host_pool"]
        result["vm_name_host_pool"] = doc["tags"]["vm_name_host_pool"]
        result["os_version_host_pool"] = doc["tags"]["os_version_host_pool"]
    return result

def fetch_docs(poolId=[]):
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
        logger.info(f'{str(task.__name__)} for {doc["ipaddr"]}')
        result[str(task.__name__)] = task(doc)
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
    all_task_dic = {
        "connectivity_check" : check_connectivity_node,
        "connectivity_check_2" : check_connectivity_node_2,
        "field_consistency_check" : check_field_consistency,
        "node_mac_address_consistency_check" : check_node_mac_addr_match,
        "node_os_consistency_check" : check_node_os_match,
        "node_memory_consistency_check" : check_node_memory_match,
        "node_host_pool_consistency_check" : check_node_with_host_pool
    }

    tasks_list = []
    if tasks == "all":
        tasks_list = all_task_dic.values()
    else:
        for task in tasks:
            if task in all_task_dic:
                tasks_list.append(all_task_dic[task])
            else:
                result["result"] = False
                result["reason"] = f"Cannot find task : {task}"
                logger.error(result["reason"])
                return result

    result = fetch_docs(poolId)
    if not result["result"]:
        return result

    return monitor_health_nodes_parallel(result["docs"], tasks_list,
                                         max_workers=750)

def parse_arguments():
    parser = argparse.ArgumentParser(description="A tool to monitors nodes")
    parser.add_argument("--poolId", type=str, help="The poolId")
    parser.add_argument("--tasks", type=str, help="The tasks")
    return parser.parse_args()

def main():
    logging_conf_path = os.path.join(script_dir, "..", "..", "logging.conf")
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
        args.tasks = "all"
    else:
        try:
            args.tasks = eval(args.tasks)
        except Exception as e:
            logger.error(f"The format of tasks is wrong : {e}")
            return

    logger.info(f"poolId {args.poolId}")
    logger.info(f"tasks {args.tasks}")

    results = fetch_and_monitor_health(poolId=args.poolId,
                                       tasks=args.tasks)

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