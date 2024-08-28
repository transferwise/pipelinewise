import base64
import io
import backoff
import jsonlines
import requests
from requests.exceptions import ConnectionError
from singer import metrics
import singer

LOGGER = singer.get_logger()

BACKOFF_MAX_TRIES_REQUEST = 7


class ReadTimeoutError(Exception):
    pass


class Server5xxError(Exception):
    pass


class Server429Error(Exception):
    pass


class MixpanelError(Exception):
    pass


class MixpanelBadRequestError(MixpanelError):
    pass


class MixpanelUnauthorizedError(MixpanelError):
    pass


class MixpanelRequestFailedError(MixpanelError):
    pass


class MixpanelNotFoundError(MixpanelError):
    pass


class MixpanelForbiddenError(MixpanelError):
    pass


class MixpanelInternalServiceError(MixpanelError):
    pass


ERROR_CODE_EXCEPTION_MAPPING = {
    400: MixpanelBadRequestError,
    401: MixpanelUnauthorizedError,
    402: MixpanelRequestFailedError,
    403: MixpanelForbiddenError,
    404: MixpanelNotFoundError,
    500: MixpanelInternalServiceError}


def get_exception_for_error_code(error_code):
    return ERROR_CODE_EXCEPTION_MAPPING.get(error_code, MixpanelError)

def raise_for_error(response):
    LOGGER.error('ERROR {}: {}, REASON: {}'.format(response.status_code,\
        response.text, response.reason))
    try:
        response.raise_for_status()
    except (requests.HTTPError, requests.ConnectionError) as error:
        try:
            content_length = len(response.content)
            if content_length == 0:
                # There is nothing we can do here since Mixpanel has neither sent
                # us a 2xx response nor a response content.
                return
            response = response.json()
            if ('error' in response) or ('errorCode' in response):
                message = '%s: %s' % (response.get('error', str(error)),
                                      response.get('message', 'Unknown Error'))
                error_code = response.get('status')
                ex = get_exception_for_error_code(error_code)
                raise ex(message)
            else:
                raise MixpanelError(error)
        except (ValueError, TypeError):
            raise MixpanelError(error)


class MixpanelClient(object):
    def __init__(self,
                 api_secret,
                 user_agent=None):
        self.__api_secret = api_secret
        self.__user_agent = user_agent
        self.__session = requests.Session()
        self.__verified = False
        self.disable_engage_endpoint = False

    def __enter__(self):
        self.__verified = self.check_access()
        return self

    def __exit__(self, exception_type, exception_value, traceback):
        self.__session.close()


    @backoff.on_exception(backoff.expo,
                          (Server5xxError, Server429Error, ReadTimeoutError, ConnectionError),
                          max_tries=5,
                          factor=2)
    def check_access(self):
        if self.__api_secret is None:
            raise Exception('Error: Missing api_secret in tap config.json.')
        headers = {}
        # Endpoint: simple API call to return a single record (org settings) to test access
        url = 'https://mixpanel.com/api/2.0/engage'
        LOGGER.info('Checking access by calling {}'.format(url))
        if self.__user_agent:
            headers['User-Agent'] = self.__user_agent
        headers['Accept'] = 'application/json'
        headers['Authorization'] = 'Basic {}'.format(
            str(base64.urlsafe_b64encode(self.__api_secret.encode("utf-8")), "utf-8"))

        try:
            response = self.__session.get(
                url=url,
                headers=headers)
        except requests.exceptions.Timeout as err:
            LOGGER.error('TIMEOUT ERROR: {}'.format(err))
            raise ReadTimeoutError

        if response.status_code == 402:
            # 402 Payment Requirement does not indicate a permissions or authentication error
            self.disable_engage_endpoint = True
            LOGGER.warning('Mixpanel returned a 402 from the Engage API. Engage stream will be skipped.')
            return True
        elif response.status_code != 200:
            LOGGER.error('Error status_code = {}'.format(response.status_code))
            raise_for_error(response)
        else:
            return True


    @backoff.on_exception(
        backoff.expo,
        (Server5xxError, Server429Error, ReadTimeoutError, ConnectionError),
        max_tries=BACKOFF_MAX_TRIES_REQUEST,
        factor=3, 
        logger=LOGGER)
    def perform_request(self,
                        method,
                        url=None,
                        params=None,
                        json=None,
                        stream=False,
                        **kwargs):
        try:
            response = self.__session.request(method=method,
                                          url=url,
                                          params=params,
                                          json=json,
                                          stream=stream,
                                          **kwargs)

            if response.status_code >= 500:
                raise Server5xxError()

            if response.status_code != 200:
                raise_for_error(response)
            return response
        except requests.exceptions.Timeout as err:
            LOGGER.error('TIMEOUT ERROR: {}'.format(err))
            raise ReadTimeoutError(err)


    def request(self, method, url=None, path=None, params=None, json=None, **kwargs):
        if not self.__verified:
            self.__verified = self.check_access()

        if url and path:
            url = '{}/{}'.format(url, path)
        elif path and not url:
            url = 'https://mixpanel.com/api/2.0/{}'.format(path)

        if 'endpoint' in kwargs:
            endpoint = kwargs['endpoint']
            del kwargs['endpoint']
        else:
            endpoint = None

        if 'headers' not in kwargs:
            kwargs['headers'] = {}

        kwargs['headers']['Accept'] = 'application/json'

        if self.__user_agent:
            kwargs['headers']['User-Agent'] = self.__user_agent

        if method == 'POST':
            kwargs['headers']['Content-Type'] = 'application/json'

        kwargs['headers']['Authorization'] = 'Basic {}'.format(
            str(base64.urlsafe_b64encode(self.__api_secret.encode("utf-8")), "utf-8"))
        with metrics.http_request_timer(endpoint) as timer:
            response = self.perform_request(method=method,
                                            url=url,
                                            params=params,
                                            json=json,
                                            **kwargs)

            timer.tags[metrics.Tag.http_status_code] = response.status_code

        response_json = response.json()
        return response_json


    def request_export(self, method, url=None, path=None, params=None, json=None, **kwargs):
        if not self.__verified:
            self.__verified = self.check_access()

        if url and path:
            url = '{}/{}'.format(url, path)
        elif path and not url:
            url = 'https://data.mixpanel.com/api/2.0/{}'.format(path)

        if 'endpoint' in kwargs:
            endpoint = kwargs['endpoint']
            del kwargs['endpoint']
        else:
            endpoint = 'export'

        if 'headers' not in kwargs:
            kwargs['headers'] = {}

        kwargs['headers']['Accept'] = 'application/json'

        if self.__user_agent:
            kwargs['headers']['User-Agent'] = self.__user_agent

        if method == 'POST':
            kwargs['headers']['Content-Type'] = 'application/json'

        kwargs['headers']['Authorization'] = 'Basic {}'.format(
            str(base64.urlsafe_b64encode(self.__api_secret.encode("utf-8")), "utf-8"))
        with metrics.http_request_timer(endpoint) as timer:
            response = self.perform_request(method=method,
                                        url=url,
                                        params=params,
                                        json=json,
                                        stream=True,
                                        **kwargs)
            timer.tags[metrics.Tag.http_status_code] = response.status_code

            # export endpoint returns jsonl results;
            #  other endpoints return json with array of results
            #  jsonlines reference: https://jsonlines.readthedocs.io/en/latest/
            reader = jsonlines.Reader(response.iter_lines())
            for record in reader.iter(allow_none=True, skip_empty=True):
                yield record
