import logging
from flask_restx import Resource, abort, fields
from qe_infra_rest_client.app.slaves import slaves_aggregate_api
from helper.sdk_helper.testdb_helper.slave_pool_helper import SlavePoolSDKHelper

logger = logging.getLogger("rest")

post_request_model = slaves_aggregate_api.model('SlavePostRequestAggregate', {
    'filters': fields.Nested(slaves_aggregate_api.model('Filters', {
        'filter1': fields.List(fields.String(description='filter1 and the list of values for the filter')),
        'filter2': fields.List(fields.String(description='filter2 and the list of values for the filter'))
    })),
    'pivot': fields.String(description='Pivot for which the aggregate has to be calculated'),
})


@slaves_aggregate_api.route('/fetchPivotAggregates')
class AggregateAPI(Resource):

    @slaves_aggregate_api.doc('slave_post_query_aggregate', responses={
        500: 'Internal Server Error',
        200: 'Success'
    })
    @slaves_aggregate_api.response(200, 'Success', slaves_aggregate_api.model('SlaveResponseModelAggregate', {
    'field_value1': fields.Integer(description='Total number of slavex with that field value1'),
    'field_value2': fields.Float(description='Total number of slaves with that field value12'),
}))
    @slaves_aggregate_api.expect(post_request_model, validate=True)
    @slaves_aggregate_api.produces(['application/json'])


    def post(self):
        """
        Post method for fetching all aggregates of a pivot
        Fetches the count of distinct values of a pivot.
        """
        args = slaves_aggregate_api.payload
        filters = args['filters']
        pivot = args['pivot']

        try:
            slave_pool_helper = SlavePoolSDKHelper()
        except Exception as e:
            message = f"Cannot connect to slave-pool : {e}"
            logger.error(message)
            abort(500, f"Internal Server error : {message}")

        try:
            if pivot == "state":
                query_result = slave_pool_helper.fetch_aggregate_state(filters=filters)
                field_elements = {}
                for row in query_result:
                    field_elements[row['state']] = row['count']
                return field_elements
            else:
                query_result = slave_pool_helper.fetch_aggregate_tags(filters=filters)
                field_elements = {}
                for row in query_result:
                    field_elements = row
                return field_elements
        except Exception as e:
            message = f"Cannot query to slave-pool : {e}"
            logger.error(message)
            abort(500, f"Internal Server error : {message}")


