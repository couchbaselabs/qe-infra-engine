from flask_restx import Namespace

nodes_api = Namespace('nodes', description='nodes related operations')
nodes_query_api = Namespace('nodes/query', description="nodes and query related operation")
nodes_data_api = Namespace('nodes/data', description="nodes and data related operation")
nodes_aggregate_api = Namespace('nodes/aggregate', description="nodes and aggregate related operation")

from qe_infra_rest_client.app.nodes import api, query, data, aggregate