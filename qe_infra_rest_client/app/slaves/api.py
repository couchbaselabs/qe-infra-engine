from flask_restx import Resource, abort, fields
import logging
from qe_infra_rest_client.app.slaves import slaves_api

logger = logging.getLogger("rest")

@slaves_api.route('/')
class SlaveAPI(Resource):

    @slaves_api.doc('get', responses={
        501: 'Not Implemented error'
    })
    def get(self):
        abort(501, message="Not Implemented")