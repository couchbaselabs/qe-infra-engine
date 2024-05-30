import logging
from flask_restx import Resource, abort, fields
from qe_infra_rest_client.app.slaves import slaves_query_api
from constants.rest_client_constants import SLAVE_FILTERS
from helper.sdk_helper.testdb_helper.slave_pool_helper import SlavePoolSDKHelper

logger = logging.getLogger("rest")

@slaves_query_api.route('/fetchFilters')
class QueryAPI(Resource):

    @slaves_query_api.doc('get', responses={
        500: 'Internal Server Error',
        200: 'Success'
    })
    @slaves_query_api.response(200, 'Success', slaves_query_api.model('SlaveQueryResponse', {
    'field1': fields.List(fields.String("value1", description='List of distinct values for field1')),
    'field2': fields.List(fields.String("value2", description='List of distinct values for field2')),
    }))
    def get(self):
        """
        Get method for fetching filters possible for the node
        Returns a json of filter field as key and list of distinct values possible for the field as a list
        """
        try:
            slave_pool_helper = SlavePoolSDKHelper()
        except Exception as e:
            message = f"Cannot connect to slave-pool : {e}"
            logger.error(message)
            abort(500, f"Internal Server error : {message}")

        res = {}
        for field in SLAVE_FILTERS:
            try:
                query_result = slave_pool_helper.fetch_distinct_values_array(field=field)
                field_elements = []
                for row in query_result:
                    field_elements.append(row["unnested_element"])
            except Exception as e:
                message = f"Cannot complete query to slave-pool : {e}"
                logger.error(message)
                abort(500, f"Internal Server error : {message}")

            if len(field_elements) == 0:
                try:
                    query_result = slave_pool_helper.fetch_distinct_values(field=field)
                    for row in query_result:
                        if field in row:
                            field_elements.append(row[field])
                except Exception as e:
                    message = f"Cannot complete query to slave-pool : {e}"
                    logger.error(message)
                    abort(500, f"Internal Server error : {message}")

            res[field] = field_elements
        return res

