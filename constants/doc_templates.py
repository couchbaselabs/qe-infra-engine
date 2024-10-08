HOST_TEMPLATE = {
    "name" : "",
    "hostname" : "",
    "ipaddr" : "",
    "cpu" : 0,
    "name_label" : "",
    "memory" : "",
    "state" : "",
    "poolId" : "",
    "group" : "",
    "xen_username" : "",
    "xen_password" : "",
    "tags" : {
        "list" : [],
        "details" : {}
    }
}

NODE_TEMPLATE = {
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
    "username": "",
    "tags" : {
        "list" : [],
        "details" : {}
    }
}

VM_TEMPLATE = {
    "addresses" : {},
    "cpu" : "",
    "mainIpAddress" : "",
    "memory" : 0,
    "name_label" : "",
    "os_version" : "",
    "state" : "",
    "poolId" : "",
    "group" : "",
    "host" : "",
    "tags" : {
        "list" : [],
        "details" : {}
    }
}

SLAVE_TEMPLATE = {
    "ipaddr" : "",
    "name" : "",
    "description" : "",
    "labels" : [],
    "os" : "",
    "os_version" : "",
    "state" : "",
    "memory" : "",
    "tags" : {},
    "mac_address": "",
    "origin": "",
    "name_label" : "",
    "jenkins_host" : "",
    "num_executors" : 0,
    "remote_fs": "",
    "usage_mode" : "",
    "tags" : {
        "list" : [],
        "details" : {}
    }
}

TASK_TEMPLATE = {
    "task_id" : "",
    "task_name": "",
    "start_time": "",
    "end_time": "",
    "state": "",
    "result": False,
}

TASK_RESULT_TEMPLATE = {
    "task_id" : "",
    "result": {}
}