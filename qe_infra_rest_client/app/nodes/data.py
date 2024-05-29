import logging
import math
from flask_restx import Resource, abort, fields
from qe_infra_rest_client.app.nodes import nodes_data_api
from constants.rest_client_constants import NODE_FILTERS
from helper.sdk_helper.testdb_helper.server_pool_helper import ServerPoolSDKHelper

logger = logging.getLogger("rest")

post_request_model = nodes_data_api.model('PostRequestData', {
    'filters': fields.Nested(nodes_data_api.model('Filters', {
        'filter1': fields.List(fields.String(description='filter1 and the list of values for the filter')),
        'filter2': fields.List(fields.String(description='filter2 and the list of values for the filter'))
    })),
    'fields': fields.List(fields.String(description='List of fields for which the data has to be fetched')),
    'per_page' : fields.Integer(description='Number of node documents per page'),
    'page' : fields.Integer(description='The current page'),
})


@nodes_data_api.route('/fetchData')
class DataAPI(Resource):

    @nodes_data_api.doc('post_query_data', responses={
        500: 'Internal Server Error',
        200: 'Success'
    })
    @nodes_data_api.response(200, 'Success', nodes_data_api.model('ResponseModelData', {
    'total_nodes': fields.Integer(description='Total number of nodes'),
    'total_pages': fields.Float(description='Total number of pages'),
    'page': fields.Integer(description='Current page number'),
    'data': fields.Raw(description='Data entries')
}))
    @nodes_data_api.expect(post_request_model, validate=True)
    @nodes_data_api.produces(['application/json'])


    def post(self):
        """
        Post method for fetching all data with the possible filters
        Fetches data in a paginated manner with filters and the fields needed
        """
        args = nodes_data_api.payload
        filters = args['filters']
        fields = args['fields']
        per_page = args['per_page']
        page = args["page"]
        try:
            server_pool_helper = ServerPoolSDKHelper()
        except Exception as e:
            message = f"Cannot connect to server-pool : {e}"
            logger.error(message)
            abort(500, f"Internal Server error : {message}")

        try:
            query_result = server_pool_helper.fetch_count_nodes_by_filters(filters=filters)
            field_elements = []
            num_docs = 0
            for row in query_result:
                num_docs = row["count"]
        except Exception as e:
            message = f"Cannot query to server-pool : {e}"
            logger.error(message)
            abort(500, f"Internal Server error : {message}")

        offset = page * per_page

        try:
            query_result = server_pool_helper.fetch_nodes_by_filters(filters=filters, fields=fields,
                                                                     page=per_page, offset=offset)
            field_elements = []
            for row in query_result:
                field_elements.append(row)
        except Exception as e:
            message = f"Cannot query to server-pool : {e}"
            logger.error(message)
            abort(500, f"Internal Server error : {message}")

        res = {
            "total_nodes" : int(num_docs),
            "total_pages" : math.ceil(num_docs/per_page),
            "page" : page,
            "data" : field_elements
        }
        return res

