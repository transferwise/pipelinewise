import backoff
import requests
import singer
from singer import metrics

LOGGER = singer.get_logger()

API_URL = 'https://api.twilio.com'
API_VERSION = '2010-04-01'


class Server5xxError(Exception):
    pass


class Server429Error(Exception):
    pass


class TwilioError(Exception):
    pass


class TwilioBadRequestError(TwilioError):
    pass


class TwilioUnauthorizedError(TwilioError):
    pass


class TwilioForbiddenError(TwilioError):
    pass


class TwilioNotFoundError(TwilioError):
    pass


class TwilioMethodNotAllowedError(TwilioError):
    pass


# Errors: https://www.twilio.com/docs/usage/twilios-response#response-formats-exceptions
STATUS_EXCEPTION_MAPPING = {
    400: TwilioBadRequestError,
    401: TwilioUnauthorizedError,
    403: TwilioForbiddenError,
    404: TwilioNotFoundError,
    405: TwilioMethodNotAllowedError}


def get_exception_for_status(status):
    return STATUS_EXCEPTION_MAPPING.get(status, TwilioError)


# Error message (example):
# {
#   "status": 400,
#   "message": "No to number is specified",
#   "code": 21201,
#   "more_info": "http:\/\/www.twilio.com\/docs\/errors\/21201"
# }
def raise_for_error(response):
    LOGGER.error('ERROR %s: %s, REASON: %s', response.status_code, response.text, response.reason)

    try:
        response.raise_for_status()
    except (requests.HTTPError, requests.ConnectionError) as error:
        try:
            content_length = len(response.content)
            if content_length == 0:
                # There is nothing we can do here since Twilio has neither sent
                # us a 2xx response nor a response content.
                return
            response = response.json()
            if ('status' in response) and ('message' in response):
                status = response.get('status')
                message = response.get('message')
                error_code = response.get('code', 'N/A')
                more_info = response.get('more_info', 'N/A')
                error_message = '{}: {}, error code: {}, more info: {}, ERROR: {}'.format(
                    status, message, error_code, more_info, error)
                ex = get_exception_for_status(status)
                raise ex(error_message) from ex
            raise TwilioError(error) from error
        except (ValueError, TypeError) as ex:
            raise TwilioError(ex) from ex


class TwilioClient:
    def __init__(self,
                 account_sid,
                 auth_token,
                 user_agent=None):
        self.__account_sid = account_sid
        self.__auth_token = auth_token
        base_url = "{}/{}".format(API_URL, API_VERSION)
        self.base_url = base_url
        self.__user_agent = user_agent
        self.__session = requests.Session()
        self.__verified = False

    def __enter__(self):
        self.__verified = self.check_access()
        return self

    def __exit__(self, exception_type, exception_value, traceback):
        self.__session.close()

    # pylint: disable=inconsistent-return-statements
    @backoff.on_exception(backoff.expo,
                          Server5xxError,
                          max_tries=7,
                          factor=3)
    def check_access(self):
        if self.__account_sid is None or self.__auth_token is None:
            raise Exception('Error: Missing account_sid or auth_token in config.json.')
        if self.__account_sid is None:
            raise Exception('Error: Missing account_sid in config.json.')
        headers = {}
        # Endpoint: simple API call to return a single record (CompanyInformation) to test access
        # https://developer.Twilio.com/default/documentation/Rest-Adv-v8#operations-Company_Information-GetCompanyInfo
        endpoint = 'Accounts'
        url = '{}/{}.json'.format(self.base_url, endpoint)
        if self.__user_agent:
            headers['User-Agent'] = self.__user_agent
        headers['Accept'] = 'application/json'
        response = self.__session.get(
            url=url,
            headers=headers,
            # Basic Authentication
            auth=(self.__account_sid, self.__auth_token))
        if response.status_code != 200:
            LOGGER.error('Error status_code = %s', response.status_code)
            raise_for_error(response)

        return True

    @backoff.on_exception(backoff.expo,
                          (Server5xxError, ConnectionError, Server429Error),
                          max_tries=7,
                          factor=3)
    # Rate limit?
    # @utils.ratelimit(1000, 3600)
    def request(self, method, path=None, url=None, json=None, version=None, **kwargs):
        if not self.__verified:
            self.__verified = self.check_access()

        if not version:
            version = 'v2'

        if not url and path:
            url = '{}/{}.json'.format(self.base_url, path)

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

        with metrics.http_request_timer(endpoint) as timer:
            response = self.__session.request(
                method=method,
                url=url,
                # Basic Authentication
                auth=(self.__account_sid, self.__auth_token),
                json=json,
                **kwargs)
            timer.tags[metrics.Tag.http_status_code] = response.status_code

        if response.status_code >= 500:
            raise Server5xxError()

        if response.status_code != 200:
            raise_for_error(response)

        return response.json()

    def get(self, path, **kwargs):
        return self.request('GET', path=path, **kwargs)

    def post(self, path, **kwargs):
        return self.request('POST', path=path, **kwargs)
