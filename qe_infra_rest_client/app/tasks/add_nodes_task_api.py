import logging
from flask_restx import Resource, fields, abort
from qe_infra_rest_client.app.tasks import tasks_api, task_manager
from tasks.node_maintenance.node_operations.add_nodes import AddNodesTask

logger = logging.getLogger("rest")

vm_model = tasks_api.model('VM', {
    'poolId': fields.List(fields.String, description='List of pool IDs'),
    'ipaddr': fields.String(description='IP address'),
    'ssh_username': fields.String(description='SSH username'),
    'ssh_password': fields.String(description='SSH password'),
    'origin': fields.String(description='Origin'),
    'vm_name': fields.String(description='VM name')
})

vm_list_model = tasks_api.model('VMList', {
    'vms': fields.List(fields.Nested(vm_model))
})



@tasks_api.route('/add_nodes_task')
class AddNodesAPI(Resource):

    @tasks_api.doc('post_query_data', responses={
        500: 'Internal Server Error',
        200: 'Success'
    })
    @tasks_api.response(200, 'Success', tasks_api.model('AddNodesTaskResponseModel', {
    'task_id': fields.String(description='Task Id submitted')
}))
    @tasks_api.expect(vm_list_model, validate=True)
    @tasks_api.produces(['application/json'])


    def post(self):
        """
        Post method for starting the add nodes task with the given values
        Adds nodes into the QE-server-pool after proper initialization
        """
        args = tasks_api.payload
        vms = args["vms"]
        params = {
            "data" : vms
        }
        try:
            task = AddNodesTask(params)
        except Exception as e:
            message = f"Cannot created task : {e}"
            logger.error(message)
            abort(500, f"Internal Server error : {message}")
        try:
            task_manager.add_task(task)
        except Exception as e:
            message = f"Cannot add task to task manager : {e}"
            logger.error(message)
            abort(500, f"Internal Server error : {message}")

        return {"task_id" : str(task.id)}



