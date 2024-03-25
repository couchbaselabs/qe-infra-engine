import sys
sys.path.append('../qe-infra-engine')
sys.path.append('../qe-infra-engine/constants')
sys.path.append('../qe-infra-engine/helper')
sys.path.append('../qe-infra-engine/helper/RemoteConnectionHelper')
sys.path.append('../qe-infra-engine/helper/SDKHelper')
sys.path.append('../qe-infra-engine/helper/SDKHelper/testdb_helper')

import argparse
import json
from constants.vm_template import VM_TEMPLATE
import copy
import os
import concurrent
from helper.SDKHelper.testdb_helper.testdb_helper import SDKHelper
from helper.RemoteConnectionHelper.remote_connection_helper import RemoteConnectionHelper

VM_TEMPLATE = {
    "ipaddr": "",
    "mac_address": "",
    "vm_name": "",
    "memory": "",
    "origin": "",
    "os": "",
    "os_version": "",
    "poolId": [],
    "prevUser": "",
    "state": "",
    "username": ""
}

def initial_install(remote_connection_helper):
    remote_connection_helper.apt_update()
    remote_connection_helper.install_wget()
    remote_connection_helper.install_curl()
    remote_connection_helper.install_libtinfo()
    remote_connection_helper.install_timesyncd()


def add_node_to_testdb(vm_data):

    ipaddr = vm_data["ipaddr"]

    sdk_helper = SDKHelper()
    remote_connection_helper = RemoteConnectionHelper(ipaddr,"root","couchbase")

    mac_address = remote_connection_helper.find_mac_address()
    memory = remote_connection_helper.find_memory()
    os, os_version = remote_connection_helper.find_os_version()

    initial_install(remote_connection_helper)

    doc = copy.deepcopy(VM_TEMPLATE)

    # Filling ipaddr field
    doc["ipaddr"] = vm_data["ipaddr"]

    # Filling mac address field
    doc["mac_address"] = mac_address

    # Filling mac address field
    doc["vm_name"] = vm_data["vm_name"]

    # Filling memory field
    doc["memory"] = memory

    # Filling origin field
    doc["origin"] = vm_data["origin"]

    # Filling os field
    doc["os"] = os
    doc["os_version"] = os_version

    # Filling poolId field
    doc["poolId"] = vm_data["poolId"]

    # Filling prevUser and currentUser fields with empty string
    doc["prevUser"] = ""
    doc["username"] = ""

    # Filling state info with available
    doc["state"] = "available"

    try:
        res = sdk_helper.add_node_to_server_pool(doc)
        if not res:
            print(f"Could not add VM {ipaddr}")
            print(res)
    except Exception as e:
        print(f"Could not add VM {ipaddr}")
        print(e)



def parse_arguments():
    parser = argparse.ArgumentParser(description="A tool to add VMs to pool")
    parser.add_argument("--data", type=str, help="The data of all VMs")
    return parser.parse_args()

def main():
    args = parse_arguments()
    if not args.data:
        print("No data found")
        return
    try:
        vms_data = eval(args.data)
    except:
        print("Data passed was wrong")
        return

    print(len(vms_data))

    num_cores = os.cpu_count()
    max_workers = num_cores if num_cores is not None else 10

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submitting each dictionary to the executor to run func() concurrently
        futures = [executor.submit(add_node_to_testdb, vm_data) for vm_data in vms_data]
        # Waiting for all tasks to complete
        for future in concurrent.futures.as_completed(futures):
            future.result()

if __name__ == "__main__":
    main()