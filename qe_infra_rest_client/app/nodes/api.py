from flask_restx import Resource, abort, fields
import logging
from qe_infra_rest_client.app.nodes import nodes_api

logger = logging.getLogger("rest")

@nodes_api.route('/')
class NodeAPI(Resource):

    @nodes_api.doc('get', responses={
        501: 'Not Implemented error'
    })
    def get(self):
        abort(501, message="Not Implemented")