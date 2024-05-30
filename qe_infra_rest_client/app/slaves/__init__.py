from flask_restx import Namespace

slaves_api = Namespace('slaves', description='slaves related operations')
slaves_query_api = Namespace('slaves/query', description="slaves and query related operation")
slaves_data_api = Namespace('slaves/data', description="slaves and data related operation")
slaves_aggregate_api = Namespace('slaves/aggregate', description="slaves and aggregate related operation")

from qe_infra_rest_client.app.slaves import api, query, data, aggregate