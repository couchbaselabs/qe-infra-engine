import base64
import logging
import requests
from enum import Enum


class RestMethods(Enum):
    DELETE = "DELETE"
    GET = "GET"
    POST = "POST"
    PUT = "PUT"


class RestClient:

    def __init__(self, base_url, username, password):
        self.session = requests.Session()
        self.base_url = base_url
        self.username = username
        self.password = password
        self.logger = logging.getLogger("rest_api")

    def _create_header(self, content_type='application/json', header_params=None):
        authorization = base64.b64encode(f'{self.username}:{self.password}'.encode()).decode()
        header =  {'Content-Type': content_type,
                   'Authorization': f'Basic {authorization}',
                   'Connection': 'close',
                   'Accept': '*/*'}
        if header_params:
            for field in header_params:
                header[field] = header_params[field]
        return header

    def request(self, endpoint, method='GET', params=None, content_type=None,
                verify=False, retries=5, header_params=None):

        if method not in RestMethods.__members__ and not isinstance(method, RestMethods) :
            error_msg = "The method passed is illegal"
            self.logger.error(error_msg)
            raise ValueError(error_msg)

        if isinstance(method, RestMethods):
            method = method.name

        if content_type:
            header = self._create_header(content_type=content_type,
                                         header_params=header_params)
        else:
            header = self._create_header(header_params=header_params)

        url = self.base_url + endpoint

        for retry in range(1, retries+1):
            try:
                response = self.session.request(method=method,
                                                url=url,
                                                data=params,
                                                headers=header,
                                                verify=verify)
                response.raise_for_status()
                try:
                    content = response.json()
                    return response.status_code, content
                except ValueError as e:
                    self.logger.error(f"Parsing content to json failed {e}")
                    return response.status_code, response.content
            except Exception as e:
                self.logger.error(f"Error trying to connect to {url} : {e}")
                if retry == retries:
                        raise e





