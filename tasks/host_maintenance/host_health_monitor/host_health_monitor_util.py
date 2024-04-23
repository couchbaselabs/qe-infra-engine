from helper.sdk_helper.testdb_helper.host_pool_helper import HostSDKHelper
from tasks.host_maintenance.host_operations.add_hosts import add_host
import logging

logger = logging.getLogger("tasks")

def _get_result_failure(reason, exception=None):
    result = {}
    result["result"] = False
    result["reason"] = f"{reason} : {str(exception)}" if exception else f"{reason}"
    logger.error(result["reason"])
    return result

def check_for_vms_state(doc : dict, vm_docs : list):
    result = {}
    host = doc["name"]

    try:
        host_pool_helper = HostSDKHelper()
        logger.info(f"Connection to Host Pool successful")
    except Exception as e:
        return _get_result_failure(reason=f"Cannot connect to Host Pool using SDK",
                                   exception=e)
    doc["tags"]["vm_states"] = {}
    for vm in vm_docs:
        if vm["state"] not in doc["tags"]["vm_states"]:
            doc["tags"]["vm_states"][vm["state"]] = 0
        doc["tags"]["vm_states"][vm["state"]] += 1

    try:
        res = host_pool_helper.upsert_host(doc)
        if not res:
            return _get_result_failure(reason=f"Cannot upsert host {host} with halted-vm checks to host pool")
        logger.info(f"Document for host {host} with halted-vm checks upserted to host pool successfuly")
    except Exception as e:
        return _get_result_failure(reason=f"Cannot upsert host {host} with halted-vm checks to host pool",
                                   exception=e)

    if "vm_states" in doc:
        result["result"] = True
        result["vm_states"] = doc["tags"]["vm_states"]
    else:
        result["result"] = False
    return result

def check_for_cpu_usage(doc : dict, vm_docs : list):
    result = {}
    host = doc["name"]

    try:
        host_pool_helper = HostSDKHelper()
        logger.info(f"Connection to Host Pool successful")
    except Exception as e:
        return _get_result_failure(reason=f"Cannot connect to Host Pool using SDK",
                                   exception=e)

    cpu = 0
    for vm in vm_docs:
        if vm["state"] == "Running":
            cpu += int(vm["cpu"])

    if int(doc["cpu"]) != 0:
        doc["tags"]["allocated_cpu_utilization"] = cpu / int(doc["cpu"]) * 100
    else:
        doc["tags"]["allocated_cpu_utilization"] = 0

    try:
        res = host_pool_helper.upsert_host(doc)
        if not res:
            return _get_result_failure(reason=f"Cannot upsert host {host} with cpu-utilization checks to host pool")
        logger.info(f"Document for host {host} with cpu-utilization checks upserted to host pool successfuly")
    except Exception as e:
        return _get_result_failure(reason=f"Cannot upsert host {host} with cpu-utilization checks to host pool",
                                   exception=e)

    result["result"] = True
    result["allocated_cpu_utilization"] = doc["tags"]["allocated_cpu_utilization"]
    return result

def check_for_mem_usage(doc : dict, vm_docs : list):
    result = {}
    host = doc["name"]

    try:
        host_pool_helper = HostSDKHelper()
        logger.info(f"Connection to Host Pool successful")
    except Exception as e:
        return _get_result_failure(reason=f"Cannot connect to Host Pool using SDK",
                                   exception=e)

    memory = 0
    for vm in vm_docs:
        if vm["state"] == "Running":
            memory += int(vm["memory"])
    
    if int(doc["memory"]) == 0:
         doc["tags"]["allocated_memory_utilization"] = 0
    else:
        doc["tags"]["allocated_memory_utilization"] = memory / int(doc["memory"]) * 100


    try:
        res = host_pool_helper.upsert_host(doc)
        if not res:
            return _get_result_failure(reason=f"Cannot upsert host {host} with memory-utilization checks to host pool")
        logger.info(f"Document for host {host} with memory-utilization checks upserted to host pool successfuly")
    except Exception as e:
        return _get_result_failure(reason=f"Cannot upsert host {host} with memory-utilization checks to host pool",
                                   exception=e)

    result["result"] = True
    result["allocated_memory_utilization"] = doc["tags"]["allocated_memory_utilization"]
    return result

ALL_TASKS_DIC = {
    "allocated memory utilization check" : check_for_mem_usage,
    "allocated cpu utilization check" : check_for_cpu_usage,
    "check vms state" : check_for_vms_state
}