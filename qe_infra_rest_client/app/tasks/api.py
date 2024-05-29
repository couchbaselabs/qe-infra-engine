import logging
from flask_restx import Resource, fields, abort
from qe_infra_rest_client.app.tasks import tasks_api, task_manager
from tasks.node_maintenance.node_operations.add_nodes import AddNodesTask

logger = logging.getLogger("rest")

parser = tasks_api.parser()
parser.add_argument('task_id', type=str, required=True, help='The task ID')

@tasks_api.route('/get_status')
@tasks_api.expect(parser)
class TasksStatusAPI(Resource):

    @tasks_api.doc('post_query_data', responses={
        500: 'Internal Server Error',
        200: 'Success'
    })
    @tasks_api.response(200, 'Success', tasks_api.model('TaskStatusResponseModel', {
    'status': fields.String(description='Status of the task submitted')
}))
    @tasks_api.produces(['application/json'])

    def get(self):
        """
        GET method for starting the add nodes task with the given values
        Adds nodes into the QE-server-pool after proper initialization
        """
        args = parser.parse_args()
        task_id = args["task_id"]
        return task_manager.get_current_task_status(task_id)
    
@tasks_api.route('/get_task_result')
@tasks_api.expect(parser)
class TasksResultAPI(Resource):

    @tasks_api.doc('post_query_data', responses={
        500: 'Internal Server Error',
        200: 'Success'
    })
    @tasks_api.response(200, 'Success', tasks_api.model('TaskStatusResponseModel', {
    'status': fields.String(description='Status of the task submitted')
}))
    @tasks_api.produces(['application/json'])

    def get(self):
        """
        Post method for starting the add nodes task with the given values
        Adds nodes into the QE-server-pool after proper initialization
        """
        args = parser.parse_args()
        task_id = args["task_id"]
        task, result =  task_manager.get_task_result(task_id)
        return task.generate_json_result()
        



