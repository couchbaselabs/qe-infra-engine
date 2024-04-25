from helper.sdk_helper.testdb_helper.host_pool_helper import HostSDKHelper
from helper.sdk_helper.testdb_helper.server_pool_helper import ServerPoolSDKHelper
from util.ssh_util.node_infra_helper.remote_connection_factory import RemoteConnectionObjectFactory
from constants.doc_templates import NODE_TEMPLATE
import paramiko, paramiko.ssh_exception
import logging
import socket

logger = logging.getLogger("tasks")

def _get_result_failure(reason, exception=None):
    result = {}
    result["result"] = False
    result["reason"] = f"{reason} : {str(exception)}" if exception else f"{reason}"
    logger.error(result["reason"])
    return result

def check_connectivity_node(doc):
    result = {}
    ipaddr = doc["ipaddr"]

    try:
        server_pool_helper = ServerPoolSDKHelper()
        logger.info(f"Connection to Server Pool successful")
    except Exception as e:
        return _get_result_failure(reason=f"Cannot connect to Server Pool using SDK",
                                  exception=e)

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
        res = server_pool_helper.upsert_node_to_server_pool(doc)
        if not res:
            return _get_result_failure(reason=f"Cannot upsert node {ipaddr} with node-connectivity checks to server pool")

        logger.info(f"Document for node {ipaddr} with node-connectivity checks upserted to server pool successfuly")
    except Exception as e:
        return _get_result_failure(reason=f"Cannot upsert node {ipaddr} with node-connectivity checks to server pool",
                                  exception=e)

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
        return _get_result_failure(reason=f"Cannot connect to Server Pool using SDK",
                                   exception=e)

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
        res = server_pool_helper.upsert_node_to_server_pool(doc)
        if not res:
            return _get_result_failure(reason=f"Cannot upsert node {ipaddr} with node-connectivity checks to server pool")
        logger.info(f"Document for node {ipaddr} with node-connectivity checks upserted to server pool successfuly")
    except Exception as e:
        return _get_result_failure(reason=f"Cannot upsert node {ipaddr} with node-connectivity checks to server pool",
                                   exception=e)

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
        return _get_result_failure(reason="Cannot connect to Server Pool using SDK",
                                   exception=e)

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
        res = server_pool_helper.upsert_node_to_server_pool(doc)
        if not res:
            return _get_result_failure(reason=f"Cannot upsert node {ipaddr} with field_consistency checks to server pool")
        logger.info(f"Document for node {ipaddr} with field_consistency checks upserted to server pool successfuly")
    except Exception as e:
        return _get_result_failure(reason=f"Cannot upsert node {ipaddr} with field_consistency checks to server pool",
                                   exception=e)

    result["result"] = True
    result["field_consistency"] = doc["tags"]["field_consistency"]
    return result

def check_node_stats_match(doc):
    result = {}
    ipaddr = doc["ipaddr"]

    # TODO - Remove post cleaning up of server pool
    if "tags" not in doc and "connection_check" not in doc["tags"]:
        return _get_result_failure(reason=f"OS version check was run before connection checks for {ipaddr}")
    elif not doc["tags"]["connection_check"]:
        return _get_result_failure(reason=f"The node is unreachable, cannot perform os version checks for {ipaddr}")

    try:
        server_pool_helper = ServerPoolSDKHelper()
        logger.info(f"Connection to Server Pool successful")
    except Exception as e:
        return _get_result_failure(reason="Cannot connect to Server Pool using SDK",
                                   exception=e)

    try:
        remote_connection_helper = RemoteConnectionObjectFactory.fetch_helper(ipaddr,"root","couchbase")
    except Exception as e:
        return _get_result_failure(reason=f"The node {ipaddr} is unreachable, cannot perform os version checks",
                                   exception=e)

    try:
        mac_address_in_node = remote_connection_helper.find_mac_address()
    except Exception as e:
        return _get_result_failure(reason=f"Could not find mac adddress for node {ipaddr}",
                                   exception=e)

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
        memory_in_node = remote_connection_helper.find_memory_total()
    except Exception as e:
        return _get_result_failure(reason=f"Could not find total memory for node {ipaddr}",
                                   exception=e)

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
        os_node = remote_connection_helper.find_os_version()
    except Exception as e:
        return _get_result_failure(reason=f"Could not find os version for node {ipaddr}",
                                   exception=e)

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
        res = server_pool_helper.upsert_node_to_server_pool(doc)
        if not res:
            return _get_result_failure(reason=f"Cannot upsert node {ipaddr} with node-stats-consistency checks to server pool")
        logger.info(f"Document for node {ipaddr} with node-stats-consistency checks upserted to server pool successfuly")
    except Exception as e:
        return _get_result_failure(reason=f"Cannot upsert node {ipaddr} with node-stats-consistency checks to server pool",
                                   exception=e)

    result["result"] = True
    result["mac_address_node_match"] = doc["tags"]["mac_address_node_check"]
    result["memory_node_match"] = doc["tags"]["memory_node_check"]
    result["os_node_match"] = doc["tags"]["os_node_check"]
    return result

def check_node_with_host_pool(doc):
    result = {}
    ipaddr = doc["ipaddr"]
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

    try:
        vms = host_sdk_helper.fetch_vm(ipaddr=ipaddr)
    except Exception as e:
        return _get_result_failure(reason=f"Cannot fetch vm {ipaddr} from Host Pool using SDK",
                                   exception=e)

    try:
        vms = [vm[host_sdk_helper.vm_collection_name] for vm in vms]
    except Exception as e:
        return _get_result_failure(reason="Unable to parse query result from Host Pool using SDK",
                                   exception=e)

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
        res = server_pool_helper.upsert_node_to_server_pool(doc)
        if not res:
            return _get_result_failure(reason=f"Cannot upsert node {ipaddr} with host-pool-consistency checks to server pool")
        logger.info(f"Document for node {ipaddr} with host-pool-consistency checks upserted to server pool successfuly")
    except Exception as e:
        return _get_result_failure(reason=f"Cannot upsert node {ipaddr} with host-pool-consistency checks to server pool",
                                   exception=e)

    result["result"] = True
    result["ip_in_host_pool"] = doc["tags"]["ip_in_host_pool"]
    if result["ip_in_host_pool"]:
        result["origin_host_pool"] = doc["tags"]["origin_host_pool"]
        result["vm_name_host_pool"] = doc["tags"]["vm_name_host_pool"]
        result["os_version_host_pool"] = doc["tags"]["os_version_host_pool"]
    return result

ALL_TASKS_DIC = {
    "connectivity check" : check_connectivity_node,
    "connectivity check detail" : check_connectivity_node_2,
    "field consistency check" : check_field_consistency,
    "node stats consistency check" : check_node_stats_match,
    "node host pool consistency check" : check_node_with_host_pool
}

# TODO tasks
'''
1. Checking state and move from booked to available if its in booked state for more than 48hrs
2. Checking status of ntp
3. Checking status of directories and permissions on the nodes
4. Checking status of reserved nodes
'''