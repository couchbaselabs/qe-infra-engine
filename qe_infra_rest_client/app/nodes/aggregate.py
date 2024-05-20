import logging
from flask_restx import Resource, abort, fields
from qe_infra_rest_client.app.nodes import nodes_aggregate_api
from helper.sdk_helper.testdb_helper.server_pool_helper import ServerPoolSDKHelper

logger = logging.getLogger("rest")

post_request_model = nodes_aggregate_api.model('PostRequestAggregate', {
    'filters': fields.Nested(nodes_aggregate_api.model('Filters', {
        'filter1': fields.List(fields.String(description='filter1 and the list of values for the filter')),
        'filter2': fields.List(fields.String(description='filter2 and the list of values for the filter'))
    })),
    'pivot': fields.String(description='Pivot for which the aggregate has to be calculated'),
})


@nodes_aggregate_api.route('/fetchPivotAggregates')
class AggregateAPI(Resource):

    @nodes_aggregate_api.doc('post_query_aggregate', responses={
        500: 'Internal Server Error',
        200: 'Success'
    })
    @nodes_aggregate_api.response(200, 'Success', nodes_aggregate_api.model('ResponseModelAggregate', {
    'field_value1': fields.Integer(description='Total number of nodes with that field value1'),
    'field_value2': fields.Float(description='Total number of nodes with that field value12'),
}))
    @nodes_aggregate_api.expect(post_request_model, validate=True)
    @nodes_aggregate_api.produces(['application/json'])


    def post(self):
        """
        Post method for fetching all aggregates of a pivot
        Fetches the count of distinct values of a pivot.
        """
        args = nodes_aggregate_api.payload
        filters = args['filters']
        pivot = args['pivot']

        try:
            server_pool_helper = ServerPoolSDKHelper()
        except Exception as e:
            message = f"Cannot connect to server-pool : {e}"
            logger.error(message)
            abort(500, f"Internal Server error : {message}")

        try:
            if pivot == "state":
                query_result = server_pool_helper.fetch_aggregate_state(filters=filters)
                field_elements = {}
                for row in query_result:
                    field_elements[row['state']] = row['count']
                return field_elements
            else:
                abort(501, f'Not Implemented for other pivots')
        except Exception as e:
            message = f"Cannot query to server-pool : {e}"
            logger.error(message)
            abort(500, f"Internal Server error : {message}")


