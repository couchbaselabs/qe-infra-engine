from constants.jenkins import JENKINS_URLS
from tasks.task import Task
from tasks.task_result import TaskResult
from helper.sdk_helper.testdb_helper.slave_pool_helper import SlavePoolSDKHelper
from helper.jenkins_helper.jenkins_helper_factory import JenkinsHelperFactory

class ChangeSlavesTask(Task):

    def change_slave_in_slave_pool(self, task_result: TaskResult, params: dict) -> None:
        if "slave" not in params:
            self.set_subtask_exception(ValueError("Invalid arguments passed"))
        slave = params["slave"]
        if "old_ipaddr" not in slave:
            self.set_subtask_exception(ValueError(f"old_ipaddr is not found in {slave}"))
        old_ipaddr = slave["old_ipaddr"]
        if "new_ipaddr" not in slave:
            self.set_subtask_exception(ValueError(f"new_ipaddr is not found in {slave}"))
        new_ipaddr = slave["new_ipaddr"]

        try:
            slave_pool_helper = SlavePoolSDKHelper()
            self.logger.info(f"Connection to Slave Pool successful")
        except Exception as e:
            exception = f"Cannot connect to Slave Pool using SDK : {e}"
            self.set_subtask_exception(exception)

        if old_ipaddr == new_ipaddr:
            try:
                slave_doc = slave_pool_helper.get_slave(old_ipaddr)
                self.logger.info(f"Doc with old ipaddr {old_ipaddr} successfully found in slave-pool")
            except Exception as e:
                exception = f"Cannot find old ipaddr {old_ipaddr} doc in slave-pool : {e}"
                self.set_subtask_exception(exception)

            # Can only change labels, name, description, state, origin, name_label, jenkins_host
            changeable_fields = ['labels', 'name', 'description', 'state', 'origin', 'name_label', 'jenkins_host']
            for field in changeable_fields:
                if field in slave:
                    slave_doc[field] = slave[field]

            try:
                res = slave_pool_helper.upsert_slave_to_slave_pool(slave_doc)
                if not res:
                    exception = f"Cannot update slave {slave['ipaddr']} doc in slave pool"
                    self.set_subtask_exception(exception)
                self.logger.info(f"Document for slave {slave['ipaddr']} updated in slave pool successfuly")
            except Exception as e:
                exception = f"Cannot update slave {slave['ipaddr']} doc in slave pool : {e}"
                self.set_subtask_exception(exception)

            task_result.result_json = {}
            task_result.result_json["new_doc"] = slave_doc

        else:
            try:
                res = slave_pool_helper.delete_slave(old_ipaddr)
                if res:
                    self.logger.info(f"Doc with old ipaddr {old_ipaddr} successfully deleted")
                else:
                    exception = f"Cannot delete old ipaddr {old_ipaddr} doc in slave-pool : {e}"
                    self.set_subtask_exception(exception)
            except Exception as e:
                exception = f"Cannot find old ipaddr {old_ipaddr} doc in slave-pool : {e}"
                self.set_subtask_exception(exception)

            slave["ipaddr"] = new_ipaddr
            self.add_slave_task_params.append(slave)

            task_result.result_json = {}
            task_result.result_json["delete_old_ipaddr"] = True

    def change_slave_properties(self, task_result: TaskResult, params: dict) -> None:
        if "slave" not in params:
            self.set_subtask_exception(ValueError("Invalid arguments passed"))
        slave = params["slave"]
        if "name" not in slave:
            self.set_subtask_exception(ValueError(f"name key is missing for the slave {slave}"))
        one_required_fields = ["description", "num_executors", "remote_fs", "labels",
                               "usage_mode"]
        field_present = False
        for field in one_required_fields:
            if field in slave:
                field_present = True
        if field_present:
            exception = f"None of the fields in {one_required_fields} present for slave {slave['name']}"
            self.set_subtask_exception(exception)

        slave_name = slave["name"]
        description = slave["description"] if "description" in slave else None
        num_executors = slave["num_executors"] if "num_executors" in slave else None
        remote_fs = slave["remote_fs"] if "remote_fs" in slave else None
        labels = slave["remote_fs"] if "remote_fs" in slave else None
        usage_mode = slave["usage_mode"] if "usage_mode" in slave else None

        if usage_mode:
            usage_modes_allowed = ['EXCLUSIVE', 'NORMAL']
            if usage_mode not in usage_modes_allowed:
                exception = ValueError(f"usage_mode {usage_mode} consists of invalid usage mode, allowed : {usage_modes_allowed}")
                self.set_subtask_exception(exception)

        if labels:
            if not isinstance(labels, list):
                raise ValueError(f"{labels} labels is not a list")

            for label in labels:
                if " " in label:
                    raise ValueError(f"label {label} consists of a space, not valid")

        try:
            slave_pool_helper = SlavePoolSDKHelper()
            self.logger.info(f"Connection to Slave Pool successful")
        except Exception as e:
            exception = f"Cannot connect to Slave Pool using SDK : {e}"
            self.set_subtask_exception(exception)

        try:
            slave_doc = slave_pool_helper.get_slave(slave_name)
            self.logger.info(f"Doc with slave {slave_name} successfully found in slave-pool")
        except Exception as e:
            exception = f"Cannot find slave {slave_name} doc in slave-pool : {e}"
            self.set_subtask_exception(exception)

        jenkins_url = slave_doc["jenkins_host"]

        if jenkins_url not in JENKINS_URLS:
            exception = ValueError(f"The value of jenkins host is invalid : {slave['jenkins_host']}")
            self.set_subtask_exception(exception)

        try:
            jenkins_helper = JenkinsHelperFactory.fetch_helper(JENKINS_URLS[slave["jenkins_host"]])
            self.logger.info(f"Connection to Jenkins successful")
        except Exception as e:
            exception = f"Cannot connect to Jenkins : {e}"
            self.set_subtask_exception(exception)

        try:
            status, response = jenkins_helper.update_slave_properties(slave_name=slave_name,
                                                                      description=description,
                                                                      labels=labels,
                                                                      num_executors=num_executors,
                                                                      remote_fs=remote_fs,
                                                                      usage_mode=usage_mode)
            if status != 200:
                exception = f"Cannot change slave {slave['name']} in Jenkins : {status}, {response}"
                self.set_subtask_exception(exception)
        except Exception as e:
            exception = f"Cannot change slave {slave['name']} in Jenkins : {e}"
            self.set_subtask_exception(exception)

        task_result.result_json = {}
        task_result.result_json["change_in_jenkins"] = str(response)