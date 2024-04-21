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
from constants.doc_templates import VM_TEMPLATE, HOST_TEMPLATE
import time
import copy
import concurrent
import datetime
import json
from helper.sdk_helper.testdb_helper.host_pool_helper import HostSDKHelper
from util.ssh_util.node_infra_helper.remote_connection_factory import RemoteConnectionObjectFactory
from helper.xen_orchestra_helper.xen_orchestra_factory import XenOrchestraObjectFactory

logger = logging.getLogger("tasks")

def _get_result_failure(reason, exception=None):
    result = {}
    result["result"] = False
    result["reason"] = f"{reason} : {str(exception)}" if exception else f"{reason}"
    logger.error(result["reason"])
    return result

def add_hosts_parallel(hosts_data, max_workers=None):
    if not max_workers:
        num_cores = os.cpu_count()
        max_workers = num_cores if num_cores is not None else 1000

    final_result = {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(add_host, host): host for host in hosts_data}
        for future in concurrent.futures.as_completed(futures):
            host = futures[future]
            result = future.result()
            final_result[host["label"]] = result

    return final_result

def add_host_on_testdb(host_data, host, group, xen_username, xen_password):
    result = {}
    try:
        host_pool_helper = HostSDKHelper()
        logger.info(f"Connection to Host Pool successful")
    except Exception as e:
        return _get_result_failure(reason="Cannot connect to Host Pool using SDK",
                                   exception=e)

    host_doc = copy.deepcopy(HOST_TEMPLATE)
    host_doc["name"] = host
    host_doc["hostname"] = f'{host}.sc.couchbase.com'

    host_doc["ipaddr"] = ""
    if "address" in host_data:
        host_doc["ipaddr"] = host_data["address"]

    host_doc["cpu"] = -1
    if "CPUs" in host_data:
        if "cpu_count" in host_data["CPUs"]:
            host_doc["cpu"] = host_data["CPUs"]["cpu_count"]

    host_doc["name_label"] = host_data["name_label"]
    host_doc["memory"] = -1
    if "memory" in host_data and "size" in host_data["memory"]:
        host_doc["memory"] = host_data["memory"]["size"]

    host_doc["state"] = ""
    if "power_state" in host_data:
        host_doc["state"] = host_data["power_state"]

    host_doc["poolId"] = host_data["$poolId"]
    host_doc["group"] = group
    host_doc["xen_username"] = xen_username
    host_doc["xen_password"] = xen_password
    host_doc["tags"] = {}
    if "reboot_required" in host_data:
        host_doc["tags"]["reboot_required"] = host_data["rebootRequired"]
    else:
        host_doc["tags"]["reboot_required"] = False

    try:
        res = host_pool_helper.upsert_host(doc=host_doc)
        if not res:
            return _get_result_failure(reason=f"Cannot add host {host} to host pool")
        logger.info(f"Document for host {host} added to host pool successfuly")
    except Exception as e:
        return _get_result_failure(reason=f"Cannot add host {host} to host pool",
                                   exception=e)

    result["result"] = True
    result["host_doc"] = host_doc

def add_vms_on_testdb(vms_data, host, group):
    result = {}
    try:
        host_pool_helper = HostSDKHelper()
        logger.info(f"Connection to Host Pool successful")
    except Exception as e:
        return _get_result_failure(reason="Cannot connect to Host Pool using SDK",
                                   exception=e)

    for vm in vms_data:
        vm_doc = copy.deepcopy(VM_TEMPLATE)

        vm_doc["addresses"] = {}
        if "addresses" in vm:
            vm_doc["addresses"] = vm["addresses"]

        vm_doc["cpu"] = -1
        if "CPUs" in vm and "number" in vm["CPUs"]:
            vm_doc["cpu"] = vm["CPUs"]["number"]

        vm_doc["mainIpAddress"] = ""
        if "mainIpAddress" in vm:
            vm_doc["mainIpAddress"] = vm["mainIpAddress"]

        vm_doc["memory"] = -1
        if "memory" in vm and "size" in vm["memory"]:
            vm_doc["memory"] = vm["memory"]["size"]

        vm_doc["name_label"] = vm["name_label"]

        vm_doc["os_version"] = "unknown"
        if "os_version" in vm:
            if vm["os_version"] and "name" in vm["os_version"]:
                vm_doc["os_version"] = vm["os_version"]["name"]

        vm_doc["state"] = ""
        if "power_state" in vm:
            vm_doc["state"] = vm["power_state"]

        vm_doc["poolId"] = vm["$poolId"]
        vm_doc["group"] = group
        vm_doc["host"] = host
        vm_doc["tags"] = {}

        result["name_label"] = {}
        try:
            res = host_pool_helper.upsert_vm(doc=vm_doc)
            if not res:
                result["name_label"] = _get_result_failure(reason=f"Cannot add vm {vm_doc["name_label"]} to host pool")
            logger.info(f"Document for vm {vm_doc["name_label"]} added to host pool successfuly")
        except Exception as e:
            result["name_label"] = _get_result_failure(reason=f"Cannot add vm {vm_doc["name_label"]} to host pool",
                                                       exception=e)

        result["name_label"]["result"] = True
        result["name_label"]["vm_doc"] = vm_doc

    return result

def remove_host_from_xen_orchestra(host):
    result = {}
    try:
        xen_orchestra_helper = XenOrchestraObjectFactory.fetch_helper()
        logger.info(f"Connection to xen orchestra successful")
    except Exception as e:
        return _get_result_failure(reason="Cannot connect to XenOrchestra",
                                   exception=e)

    label = host["label"]
    hostname = host["hostname"] if "hostname" in host else f'{label}.sc.couchbase.com'

    try:
        remove_result = xen_orchestra_helper.remove_host(label=label,
                                                         host=hostname)
    except Exception as e:
        return _get_result_failure(reason=f"Cannot remove host {label} from XenOrchestra",
                                   exception=e)

    result["result"] = remove_result
    if not remove_result:
        result["reason"] = f"Cannot remove host {label} from XenOrchestra"
    return result

def add_host(host):
    logger.critical(f"Host number {host['count']}")
    result = {}
    required_fields = ["username", "password", "group"]
    for field in required_fields:
        if field not in host:
            return _get_result_failure(reason=f"Field {field} not present for node {host['label']}")

    logger.info(f"All required fields present for node {host['label']}")

    try:
        HostSDKHelper()
        logger.info(f"Connection to Host Pool successful")
    except Exception as e:
        return _get_result_failure(reason="Cannot connect to Host Pool using SDK",
                                   exception=e)

    try:
        xen_orchestra_helper = XenOrchestraObjectFactory.fetch_helper()
        logger.info(f"Connection to xen orchestra successful")
    except Exception as e:
        return _get_result_failure(reason="Cannot connect to XenOrchestra",
                                   exception=e)

    label = host["label"]
    hostname = host["hostname"] if "hostname" in host else f'{label}.sc.couchbase.com'
    xen_orchestra_username = host["username"]
    xen_orchestra_password = host["password"]

    try:
        server_status = xen_orchestra_helper.get_server_status(label=label,
                                                               host=hostname)
    except Exception as e:
        if str(e) == f"Host with label {label} and host {hostname} not found":
            logger.critical(f"Host with label {label} and hostname {hostname} already exists in Xen orchestra")
            logger.critical(f"Deleting host with label {label} and hostname {hostname} from Xen orchestra")
            res = remove_host_from_xen_orchestra(host)
            if not res["result"]:
                return _get_result_failure(reason=f"Cannot delete host from XenOrchestra : {res["reason"]}")
        else:
            return _get_result_failure(reason="Cannot fetch server status from XenOrchestra",
                                       exception=e)

    try:
        xen_orchestra_helper.add_host(label=label,
                                      host=hostname,
                                      username=xen_orchestra_username,
                                      password=xen_orchestra_password)
    except Exception as e:
        return _get_result_failure(reason="Cannot add server to XenOrchestra",
                                   exception=e)

    time.sleep(10)

    try:
        server_status = xen_orchestra_helper.get_server_status(label=label,
                                                               host=hostname)
    except Exception as e:
        return _get_result_failure(reason="Cannot fetch server status from XenOrchestra",
                                   exception=e)

    if "error" in server_status:
        return _get_result_failure(reason=f"Cannot fetch server status from XenOrchestra : {server_status}")

    try:
        hosts_data = xen_orchestra_helper.fetch_list_hosts(label=label,
                                                           host=hostname)
    except Exception as e:
        return _get_result_failure(reason="Cannot fetch host list data from XenOrchestra",
                                   exception=e)
    try:
        vms_data = xen_orchestra_helper.fetch_list_vms(label=label,
                                                       host=hostname)
    except Exception as e:
        return _get_result_failure(reason="Cannot fetch vm list data from XenOrchestra",
                                   exception=e)

    res = remove_host_from_xen_orchestra(host)
    if not res["result"]:
        return _get_result_failure(reason=f"Cannot delete host from XenOrchestra : {res["reason"]}")

    if len(hosts_data) == 0:
        result["adding_host_data"] = _get_result_failure(reason=f"Cannot find host from XenOrchestra : {hosts_data}")
    elif len(hosts_data) > 0:
        result["adding_host_data"] = _get_result_failure(reason=f"Found multiple hosts from XenOrchestra : {hosts_data}")
    else:
        result["adding_host_data"] = add_host_on_testdb(hosts_data[0])

    if len(vms_data) == 0:
        result["adding_vm_data"] = _get_result_failure(reason=f"Cannot find VMs from XenOrchestra : {hosts_data}")
    elif len(vms_data) > 0:
        result["adding_vm_data"] = add_vms_on_testdb(vms_data)

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

    logger.info(f"The number of hosts to add: {len(hosts_data)}")

    i = 1
    for host in hosts_data:
        if "label" not in host:
            logger.error("Field label missing from one of the nodes")
            return
        host["count"] = i
        i+=1

    results = add_hosts_parallel(hosts_data)

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
    # main()
    logging_conf_path = os.path.join(script_dir, "..", "..", "logging.conf")
    logging.config.fileConfig(logging_conf_path)

    data = {
        # "hostname" : "",
        "label" : "xcp-s103",
        "group" : "server_pool",
        "username" : "root",
        "password" : "northscale!23",
    }

    add_host(data)
