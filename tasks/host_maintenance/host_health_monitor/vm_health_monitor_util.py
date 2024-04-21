from helper.sdk_helper.testdb_helper.host_pool_helper import HostSDKHelper
from helper.sdk_helper.testdb_helper.server_pool_helper import ServerPoolSDKHelper
from constants.doc_templates import VM_TEMPLATE
import logging

logger = logging.getLogger("tasks")

def _get_result_failure(reason, exception=None):
    result = {}
    result["result"] = False
    result["reason"] = f"{reason} : {str(exception)}" if exception else f"{reason}"
    logger.error(result["reason"])
    return result

def check_vm_network(vm_doc):
    result = {}
    try:
        host_sdk_helper = HostSDKHelper()
        logger.info(f"Connection to Host Pool successful")
    except Exception as e:
        return _get_result_failure(reason="Cannot connect to Host Pool using SDK",
                                   exception=e)

    if "addresses" not in vm_doc:
        vm_doc["tags"]["addresses_available"] = False
    else:
        present = False
        for key in vm_doc["addresses"]:
            if "ipv4" in key:
                present = True
        vm_doc["tags"]["addresses_available"] = present

    if "mainIpAddress" not in vm_doc:
        vm_doc["tags"]["mainIpAddress_available"] = False
    else:
        if len(vm_doc["mainIpAddress"].split(".")) != 4:
            vm_doc["tags"]["mainIpAddress_available"] = False
        else:
            vm_doc["tags"]["mainIpAddress_available"] = True

    try:
        res = host_sdk_helper.upsert_vm(vm_doc)
        if not res:
            return _get_result_failure(reason=f"Cannot upsert vm {vm_doc["name_label"]} with network-consistency checks to host pool")
        logger.info(f"Document for vm {vm_doc["name_label"]} with network-consistency checks upserted to host pool successfuly")
    except Exception as e:
        return _get_result_failure(reason=f"Cannot upsert vm {vm_doc["name_label"]} with network-consistency checks to host pool",
                                   exception=e)

    result["result"] = True
    result["addresses_available"] = vm_doc["tags"]["addresses_available"]
    result["mainIpAddress_available"] = vm_doc["tags"]["mainIpAddress_available"]
    return result

def check_vm_os_version(vm_doc):
    result = {}
    try:
        host_sdk_helper = HostSDKHelper()
        logger.info(f"Connection to Host Pool successful")
    except Exception as e:
        return _get_result_failure(reason="Cannot connect to Host Pool using SDK",
                                   exception=e)

    if "os_version" not in vm_doc:
        vm_doc["tags"]["os_version_available"] = False
    elif vm_doc["os_version"] == "":
        vm_doc["tags"]["os_version_available"] = False
    else:
        vm_doc["tags"]["os_version_available"] = True

    try:
        res = host_sdk_helper.upsert_vm(vm_doc)
        if not res:
            return _get_result_failure(reason=f"Cannot upsert vm {vm_doc["name_label"]} with os-version-consistency checks to host pool")
        logger.info(f"Document for vm {vm_doc["name_label"]} with os-version-consistency checks upserted to host pool successfuly")
    except Exception as e:
        return _get_result_failure(reason=f"Cannot upsert vm {vm_doc["name_label"]} with os-version-consistency checks to host pool",
                                   exception=e)

    result["result"] = True
    result["os_version_available"] = vm_doc["tags"]["os_version_available"]
    return result

def check_vms_in_server_pool(vm_doc):
    result = {}
    try:
        host_sdk_helper = HostSDKHelper()
        logger.info(f"Connection to Host Pool successful")
    except Exception as e:
        return _get_result_failure(reason="Cannot connect to Host Pool using SDK",
                                   exception=e)

    try:
        server_pool_helper = ServerPoolSDKHelper()
        logger.info(f"Connection to Server Pool successful")
    except Exception as e:
        return _get_result_failure(reason="Cannot connect to Server Pool using SDK",
                                   exception=e)

    addresses = set(vm_doc["addresses"].values())
    addresses.add(vm_doc["mainIpAddress"])

    query_result = server_pool_helper.fetch_all_nodes()
    nodes = []
    for row in query_result:
        nodes.append(row["_default"])

    ip_present = False
    for node in nodes:
        if node["ipaddr"] in addresses:
            ip_present = True
            break

    vm_doc["tags"]["vm_in_server_pool"] = ip_present

    try:
        res = host_sdk_helper.upsert_vm(vm_doc)
        if not res:
            return _get_result_failure(reason=f"Cannot upsert vm {vm_doc["name_label"]} with server-pool-consistency checks to host pool")
        logger.info(f"Document for vm {vm_doc["name_label"]} with server-pool-consistency checks upserted to host pool successfuly")
    except Exception as e:
        return _get_result_failure(reason=f"Cannot upsert vm {vm_doc["name_label"]} with server-pool-consistency checks to host pool",
                                   exception=e)

    result["result"] = True
    result["vm_in_server_pool"] = vm_doc["tags"]["vm_in_server_pool"]
    return result

def check_vm_field_consistency(vm_doc):

    result = {}
    try:
        host_sdk_helper = HostSDKHelper()
        logger.info(f"Connection to Host Pool successful")
    except Exception as e:
        return _get_result_failure(reason="Cannot connect to Host Pool using SDK",
                                   exception=e)

    
    fields_required = list(VM_TEMPLATE.keys())

    fields_absent = []
    for field in fields_required:
        if field not in vm_doc:
            fields_absent.append(field)

    fields_extra = []
    for field in vm_doc:
        if field not in fields_required:
            fields_extra.append(field)

    if len(fields_absent) == 0 and len(fields_extra) == 0:
        vm_doc["tags"]["field_consistency"] = {
            "fields_match" : True
        }
    else:
        vm_doc["tags"]["field_consistency"] = {
            "fields_match" : False
        }
        if len(fields_absent) > 0:
            vm_doc["tags"]["field_consistency"]["fields_absent"] = fields_absent
        if len(fields_extra) > 0:
            vm_doc["tags"]["field_consistency"]["fields_extra"] = fields_extra

    try:
        res = host_sdk_helper.upsert_vm(vm_doc)
        if not res:
            return _get_result_failure(reason=f"Cannot upsert vm {vm_doc["name_label"]} with field-consistency checks to host pool")
        logger.info(f"Document for vm {vm_doc["name_label"]} with field-consistency checks upserted to host pool successfuly")
    except Exception as e:
        return _get_result_failure(reason=f"Cannot upsert vm {vm_doc["name_label"]} with field-consistency checks to host pool",
                                   exception=e)

    result["result"] = True
    result["field_consistency"] = vm_doc["tags"]["field_consistency"]
    return result

ALL_TASKS_DIC = {
    "check vm os version" : check_vm_os_version,
    "check vm network" : check_vm_network,
    "vm server pool consistency check" : check_vms_in_server_pool,
    "vm field consistency check" : check_vm_field_consistency,
}
