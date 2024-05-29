from flask_restx import Api
from flask import Flask
from qe_infra_rest_client.app.nodes import nodes_api, nodes_query_api, nodes_data_api, nodes_aggregate_api
from qe_infra_rest_client.app.tasks import tasks_api

api = Api(
    title='QE-Infra-REST-Client',
    version='1.0',
    description='A rest client for qe-infra-engine',
)
app = Flask(__name__)

def fetch_app():
    api.add_namespace(nodes_api)
    api.add_namespace(nodes_query_api)
    api.add_namespace(nodes_data_api)
    api.add_namespace(nodes_aggregate_api)
    api.add_namespace(tasks_api)
    api.init_app(app)
    return app