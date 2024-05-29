from flask_restx import Namespace
from tasks.task_manager import TaskManager

tasks_api = Namespace('tasks', description='tasks related operations')

task_manager = TaskManager(5)

from qe_infra_rest_client.app.tasks import api, add_nodes_task_api