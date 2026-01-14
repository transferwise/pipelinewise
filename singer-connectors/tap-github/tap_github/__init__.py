import collections
import datetime
import fnmatch
import json
import os
import time

import requests
import singer
from singer import bookmarks, metadata, metrics

session = requests.Session()
logger = singer.get_logger()

# repository is deprecated. Use instead:
# organization and repos_exclude
# or repos_include instead
REQUIRED_CONFIG_KEYS = ['start_date', 'access_token']

MAX_RATE_LIMIT_WAIT_SECONDS = 600
RATE_THROTTLING_EXTRA_WAITING_TIME = 60
MAX_RATE_LIMIT_RETRIES_PER_REQUEST = 3

KEY_PROPERTIES = {
    'commits': ['sha'],
    'comments': ['id'],
    'issues': ['id'],
    'assignees': ['id'],
    'collaborators': ['id'],
    'pull_requests':['id'],
    'stargazers': ['user_id'],
    'releases': ['id'],
    'reviews': ['id'],
    'review_comments': ['id'],
    'pr_commits': ['id'],
    'events': ['id'],
    'issue_events': ['id'],
    'issue_labels': ['id'],
    'issue_milestones': ['id'],
    'commit_comments': ['id'],
    'projects': ['id'],
    'project_columns': ['id'],
    'project_cards': ['id'],
    'repos': ['id'],
    'teams': ['id'],
    'team_members': ['id'],
    'team_memberships': ['url']
}

class GithubException(Exception):
    pass

class BadCredentialsException(GithubException):
    pass

class InvalidParametersException(Exception):
    pass

class AuthException(GithubException):
    pass

class NotFoundException(GithubException):
    pass

class BadRequestException(GithubException):
    pass

class InternalServerError(GithubException):
    pass

class UnprocessableError(GithubException):
    pass

class NotModifiedError(GithubException):
    pass

class MovedPermanentlyError(GithubException):
    pass

class ConflictError(GithubException):
    pass

class RateLimitExceeded(GithubException):
    pass

ERROR_CODE_EXCEPTION_MAPPING = {
    301: {
        "raise_exception": MovedPermanentlyError,
        "message": "The resource you are looking for is moved to another URL."
    },
    304: {
        "raise_exception": NotModifiedError,
        "message": "The requested resource has not been modified since the last time you accessed it."
    },
    400:{
        "raise_exception": BadRequestException,
        "message": "The request is missing or has a bad parameter."
    },
    401: {
        "raise_exception": BadCredentialsException,
        "message": "Invalid authorization credentials."
    },
    403: {
        "raise_exception": AuthException,
        "message": "User doesn't have permission to access the resource."
    },
    404: {
        "raise_exception": NotFoundException,
        "message": "The resource you have specified cannot be found"
    },
    409: {
        "raise_exception": ConflictError,
        "message": "The request could not be completed due to a conflict with the current state of the server."
    },
    422: {
        "raise_exception": UnprocessableError,
        "message": "The request was not able to process right now."
    },
    500: {
        "raise_exception": InternalServerError,
        "message": "An error has occurred at Github's end."
    }
}

def translate_state(state, catalog, repositories):
    '''
    This tap used to only support a single repository, in which case the
    state took the shape of:
    {
      "bookmarks": {
        "commits": {
          "since": "2018-11-14T13:21:20.700360Z"
        }
      }
    }
    The tap now supports multiple repos, so this function should be called
    at the beginning of each run to ensure the state is translate to the
    new format:
    {
      "bookmarks": {
        "singer-io/tap-adwords": {
          "commits": {
            "since": "2018-11-14T13:21:20.700360Z"
          }
        }
        "singer-io/tap-salesforce": {
          "commits": {
            "since": "2018-11-14T13:21:20.700360Z"
          }
        }
      }
    }
    '''
    nested_dict = lambda: collections.defaultdict(nested_dict)
    new_state = nested_dict()

    for stream in catalog['streams']:
        stream_name = stream['tap_stream_id']
        for repo in repositories:
            if bookmarks.get_bookmark(state, repo, stream_name):
                return state
            if bookmarks.get_bookmark(state, stream_name, 'since'):
                new_state['bookmarks'][repo][stream_name]['since'] = bookmarks.get_bookmark(state, stream_name, 'since')

    return new_state


def get_bookmark(state, repo, stream_name, bookmark_key, start_date):
    repo_stream_dict = bookmarks.get_bookmark(state, repo, stream_name)
    if repo_stream_dict:
        return repo_stream_dict.get(bookmark_key)
    if start_date:
        return start_date
    return None

def raise_for_error(resp, source):

    content_length = len(resp.content)
    if content_length == 0:
        # There is nothing we can do here since Github has neither sent
        # us a 2xx response nor a response content.
        return

    error_code = resp.status_code
    try:
        response_json = resp.json()
    except Exception:
        response_json = {}

    if error_code == 404:
        details = ERROR_CODE_EXCEPTION_MAPPING.get(error_code).get("message")
        if source == "teams":
            details += ' or it is a personal account repository'
        message = "HTTP-error-code: 404, Error: {}. Please refer \'{}\' for more details.".format(details, response_json.get("documentation_url"))
    else:
        message = "HTTP-error-code: {}, Error: {}".format(
            error_code, ERROR_CODE_EXCEPTION_MAPPING.get(error_code, {}).get("message", "Unknown Error") if response_json == {} else response_json)

    exc = ERROR_CODE_EXCEPTION_MAPPING.get(error_code, {}).get("raise_exception", GithubException)
    raise exc(message) from None

def calculate_seconds(epoch):
    current = time.time()
    return int(round((epoch - current), 0))

def rate_throttling(response):
    if 'Retry-After' in response.headers:
        retry_after = int(response.headers['Retry-After'])
        if retry_after > 0:
            seconds_to_sleep = retry_after + RATE_THROTTLING_EXTRA_WAITING_TIME
            if seconds_to_sleep > MAX_RATE_LIMIT_WAIT_SECONDS:
                message = f"API secondary rate limit exceeded. Retry-After: {seconds_to_sleep} seconds exceeds max wait time of {MAX_RATE_LIMIT_WAIT_SECONDS} seconds."
                raise RateLimitExceeded(message) from None

            logger.warning(f"API secondary rate limit exceeded. Tap will retry data collection after {seconds_to_sleep} seconds.")
            time.sleep(seconds_to_sleep)
            return True

    if 'X-RateLimit-Remaining' in response.headers:
        remaining_requests_per_hour = int(response.headers['X-RateLimit-Remaining'])
        if remaining_requests_per_hour == 0:
            rate_limit_reset_timestamp = int(response.headers.get('X-RateLimit-Reset', 0))
            seconds_to_sleep = calculate_seconds(rate_limit_reset_timestamp) + RATE_THROTTLING_EXTRA_WAITING_TIME

            if seconds_to_sleep > MAX_RATE_LIMIT_WAIT_SECONDS:
                rate_limit_user = response.headers.get('X-RateLimit-Limit', 0)
                rate_limit_reset_time = datetime.datetime.fromtimestamp(rate_limit_reset_timestamp)
                message = f"API rate limit exceeded: " \
                          f"User limit per hour: {rate_limit_user}, " \
                          f"Remaining requests: {remaining_requests_per_hour}, " \
                          f"Time to reset {rate_limit_reset_time.isoformat()}"
                raise RateLimitExceeded(message) from None

            rate_limit_reset_time = datetime.datetime.fromtimestamp(rate_limit_reset_timestamp)
            rate_limit_user = response.headers.get('X-RateLimit-Limit', 0)
            logger.warning(f"API rate limit exceeded. User limit per hour: {rate_limit_user}, Remaining requests: {remaining_requests_per_hour}")
            logger.warning(f"Time to reset rate limit: {rate_limit_reset_time.isoformat()}")
            logger.warning(f"Tap will retry data collection after {str(datetime.timedelta(seconds=seconds_to_sleep))}")
            time.sleep(seconds_to_sleep)
            return True

    return False

# pylint: disable=dangerous-default-value
def authed_get(source, url, headers={}, do_rate_throttling=True, rate_limit_retry_count=0):
    with metrics.http_request_timer(source) as timer:
        session.headers.update(headers)
        resp = session.request(method='get', url=url)
        if resp.status_code != 200:
            if do_rate_throttling and resp.status_code == 403:
                # Check retry limit before attempting rate throttling to prevent infinite recursion
                # Each request gets MAX_RATE_LIMIT_RETRIES_PER_REQUEST attempts to handle transient rate limits
                if rate_limit_retry_count < MAX_RATE_LIMIT_RETRIES_PER_REQUEST:
                    if rate_throttling(resp):
                        return authed_get(source, url, headers, do_rate_throttling, rate_limit_retry_count + 1)
                else:
                    logger.error(f"Max rate limit retries ({MAX_RATE_LIMIT_RETRIES_PER_REQUEST}) exceeded for {url}")
            raise_for_error(resp, source)
            return None
        else:
            timer.tags[metrics.Tag.http_status_code] = resp.status_code
            if do_rate_throttling:
                rate_throttling(resp)
            return resp

def authed_get_all_pages(source, url, headers={}):
    while True:
        r = authed_get(source, url, headers)
        yield r
        if 'next' in r.links:
            url = r.links['next']['url']
        else:
            break

def get_abs_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)


def generate_pr_commit_schema(commit_schema):
    pr_commit_schema = commit_schema.copy()
    pr_commit_schema['properties']['pr_number'] = {
        "type":  ["null", "integer"]
    }
    pr_commit_schema['properties']['pr_id'] = {
        "type": ["null", "string"]
    }
    pr_commit_schema['properties']['id'] = {
        "type": ["null", "string"]
    }

    return pr_commit_schema

def load_schemas():
    schemas = {}

    for filename in os.listdir(get_abs_path('schemas')):
        path = get_abs_path('schemas') + '/' + filename
        file_raw = filename.replace('.json', '')
        with open(path, encoding='utf-8') as file:
            schemas[file_raw] = json.load(file)

    schemas['pr_commits'] = generate_pr_commit_schema(schemas['commits'])
    return schemas

class DependencyException(Exception):
    pass

def validate_dependencies(selected_stream_ids):
    errs = []
    msg_tmpl = ("Unable to extract '{0}' data, "
                "to receive '{0}' data, you also need to select '{1}'.")

    for main_stream, sub_streams in SUB_STREAMS.items():
        if main_stream not in selected_stream_ids:
            for sub_stream in sub_streams:
                if sub_stream in selected_stream_ids:
                    errs.append(msg_tmpl.format(sub_stream, main_stream))

    if errs:
        raise DependencyException(" ".join(errs))


def write_metadata(mdata, values, breadcrumb):
    mdata.append(
        {
            'metadata': values,
            'breadcrumb': breadcrumb
        }
    )

def populate_metadata(schema_name, schema):
    mdata = metadata.new()
    #mdata = metadata.write(mdata, (), 'forced-replication-method', KEY_PROPERTIES[schema_name])
    mdata = metadata.write(mdata, (), 'table-key-properties', KEY_PROPERTIES[schema_name])

    for field_name in schema['properties'].keys():
        if field_name in KEY_PROPERTIES[schema_name]:
            mdata = metadata.write(mdata, ('properties', field_name), 'inclusion', 'automatic')
        else:
            mdata = metadata.write(mdata, ('properties', field_name), 'inclusion', 'available')

    return mdata

def get_catalog():
    raw_schemas = load_schemas()
    streams = []

    for schema_name, schema in raw_schemas.items():

        # get metadata for each field
        mdata = populate_metadata(schema_name, schema)

        # create and add catalog entry
        catalog_entry = {
            'stream': schema_name,
            'tap_stream_id': schema_name,
            'schema': schema,
            'metadata' : metadata.to_list(mdata),
            'key_properties': KEY_PROPERTIES[schema_name],
        }
        streams.append(catalog_entry)

    return {'streams': streams}

def verify_repo_access(url_for_repo, repo):
    try:
        authed_get("verifying repository access", url_for_repo, do_rate_throttling=False)
    except NotFoundException:
        # throwing user-friendly error message as it checks token access
        message = "HTTP-error-code: 404, Error: Please check the repository name \'{}\' or you do not have sufficient permissions to access this repository.".format(repo)
        raise NotFoundException(message) from None

def verify_access_for_repo(config):

    access_token = config['access_token']
    session.headers.update({'authorization': 'token ' + access_token, 'per_page': '1', 'page': '1'})

    repositories = get_repos_from_config(config)

    for repo in repositories:
        logger.info("Verifying access of repository: %s", repo)

        url_for_repo = "https://api.github.com/repos/{}/commits".format(repo)

        # Verifying for Repo access
        verify_repo_access(url_for_repo, repo)

def do_discover(config):
    verify_access_for_repo(config)
    catalog = get_catalog()
    # dump catalog
    print(json.dumps(catalog, indent=2))

def get_all_teams(schemas, repo_path, state, mdata, _start_date):
    org = repo_path.split('/')[0]
    with metrics.record_counter('teams') as counter:
        for response in authed_get_all_pages(
                'teams',
                'https://api.github.com/orgs/{}/teams?sort=created_at&direction=desc'.format(org)
        ):
            teams = response.json()
            extraction_time = singer.utils.now()

            for r in teams:
                team_slug = r.get('slug')
                r['_sdc_repository'] = repo_path

                # transform and write release record
                with singer.Transformer() as transformer:
                    rec = transformer.transform(r, schemas, metadata=metadata.to_map(mdata))
                singer.write_record('teams', rec, time_extracted=extraction_time)
                singer.write_bookmark(state, repo_path, 'teams', {'since': singer.utils.strftime(extraction_time)})
                counter.increment()

                if schemas.get('team_members'):
                    for team_members_rec in get_all_team_members(team_slug, schemas['team_members'], repo_path, state, mdata):
                        singer.write_record('team_members', team_members_rec, time_extracted=extraction_time)
                        singer.write_bookmark(state, repo_path, 'team_members', {'since': singer.utils.strftime(extraction_time)})

                if schemas.get('team_memberships'):
                    for team_memberships_rec in get_all_team_memberships(team_slug, schemas['team_memberships'], repo_path, state, mdata):
                        singer.write_record('team_memberships', team_memberships_rec, time_extracted=extraction_time)

    return state

def get_all_team_members(team_slug, schemas, repo_path, state, mdata):
    org = repo_path.split('/')[0]
    with metrics.record_counter('team_members') as counter:
        for response in authed_get_all_pages(
                'team_members',
                'https://api.github.com/orgs/{}/teams/{}/members?sort=created_at&direction=desc'.format(org, team_slug)
        ):
            team_members = response.json()
            for r in team_members:
                r['_sdc_repository'] = repo_path

                # transform and write release record
                with singer.Transformer() as transformer:
                    rec = transformer.transform(r, schemas, metadata=metadata.to_map(mdata))
                counter.increment()

                yield rec

    return state

def get_all_team_memberships(team_slug, schemas, repo_path, state, mdata):
    org = repo_path.split('/')[0]
    for response in authed_get_all_pages(
            'team_members',
            'https://api.github.com/orgs/{}/teams/{}/members?sort=created_at&direction=desc'.format(org, team_slug)
        ):
        team_members = response.json()
        with metrics.record_counter('team_memberships') as counter:
            for r in team_members:
                username = r['login']
                for res in authed_get_all_pages(
                        'memberships',
                        'https://api.github.com/orgs/{}/teams/{}/memberships/{}'.format(org, team_slug, username)
                ):
                    team_membership = res.json()
                    team_membership['_sdc_repository'] = repo_path
                    with singer.Transformer() as transformer:
                        rec = transformer.transform(team_membership, schemas, metadata=metadata.to_map(mdata))
                    counter.increment()
                    yield rec
    return state


def get_all_issue_events(schemas, repo_path, state, mdata, start_date):
    bookmark_value = get_bookmark(state, repo_path, "issue_events", "since", start_date)
    if bookmark_value:
        bookmark_time = singer.utils.strptime_to_utc(bookmark_value)
    else:
        bookmark_time = 0


    with metrics.record_counter('issue_events') as counter:
        for response in authed_get_all_pages(
                'issue_events',
                'https://api.github.com/repos/{}/issues/events?sort=created_at&direction=desc'.format(repo_path)
        ):
            events = response.json()
            extraction_time = singer.utils.now()
            for event in events:
                event['_sdc_repository'] = repo_path
                # skip records that haven't been updated since the last run
                # the GitHub API doesn't currently allow a ?since param for pulls
                # once we find the first piece of old data we can return, thanks to
                # the sorting
                updated_at = event.get('created_at') if event.get('updated_at') is None else event.get('updated_at')
                if bookmark_time and singer.utils.strptime_to_utc(updated_at) < bookmark_time:
                    return state

                # transform and write release record
                with singer.Transformer() as transformer:
                    rec = transformer.transform(event, schemas, metadata=metadata.to_map(mdata))
                singer.write_record('issue_events', rec, time_extracted=extraction_time)
                singer.write_bookmark(state, repo_path, 'issue_events', {'since': singer.utils.strftime(extraction_time)})
                counter.increment()

    return state


def get_all_events(schemas, repo_path, state, mdata, start_date):
    # Incremental sync off `created_at`
    # https://developer.github.com/v3/issues/events/#list-events-for-a-repository
    # 'https://api.github.com/repos/{}/issues/events?sort=created_at&direction=desc'.format(repo_path)

    bookmark_value = get_bookmark(state, repo_path, "events", "since", start_date)
    if bookmark_value:
        bookmark_time = singer.utils.strptime_to_utc(bookmark_value)
    else:
        bookmark_time = 0

    with metrics.record_counter('events') as counter:
        for response in authed_get_all_pages(
                'events',
                'https://api.github.com/repos/{}/events?sort=created_at&direction=desc'.format(repo_path)
        ):
            events = response.json()
            extraction_time = singer.utils.now()
            for r in events:
                r['_sdc_repository'] = repo_path

                # skip records that haven't been updated since the last run
                # the GitHub API doesn't currently allow a ?since param for pulls
                # once we find the first piece of old data we can return, thanks to
                # the sorting
                updated_at = r.get('created_at') if r.get('updated_at') is None else r.get('updated_at')
                if bookmark_time and singer.utils.strptime_to_utc(updated_at) < bookmark_time:
                    return state

                # transform and write release record
                with singer.Transformer() as transformer:
                    rec = transformer.transform(r, schemas, metadata=metadata.to_map(mdata))
                singer.write_record('events', rec, time_extracted=extraction_time)
                singer.write_bookmark(state, repo_path, 'events', {'since': singer.utils.strftime(extraction_time)})
                counter.increment()

    return state

def get_all_issue_milestones(schemas, repo_path, state, mdata, start_date):
    # Incremental sync off `due on` ??? confirm.
    # https://developer.github.com/v3/issues/milestones/#list-milestones-for-a-repository
    # 'https://api.github.com/repos/{}/milestones?sort=created_at&direction=desc'.format(repo_path)
    bookmark_value = get_bookmark(state, repo_path, "issue_milestones", "since", start_date)
    if bookmark_value:
        bookmark_time = singer.utils.strptime_to_utc(bookmark_value)
    else:
        bookmark_time = 0

    with metrics.record_counter('issue_milestones') as counter:
        for response in authed_get_all_pages(
                'milestones',
                'https://api.github.com/repos/{}/milestones?direction=desc'.format(repo_path)
        ):
            milestones = response.json()
            extraction_time = singer.utils.now()
            for r in milestones:
                r['_sdc_repository'] = repo_path

                # skip records that haven't been updated since the last run
                # the GitHub API doesn't currently allow a ?since param for pulls
                # once we find the first piece of old data we can return, thanks to
                # the sorting
                if bookmark_time and r.get("due_on") and singer.utils.strptime_to_utc(r.get("due_on")) < bookmark_time:
                    continue

                # transform and write release record
                with singer.Transformer() as transformer:
                    rec = transformer.transform(r, schemas, metadata=metadata.to_map(mdata))
                singer.write_record('issue_milestones', rec, time_extracted=extraction_time)
                singer.write_bookmark(state, repo_path, 'issue_milestones', {'since': singer.utils.strftime(extraction_time)})
                counter.increment()

    return state

def get_all_issue_labels(schemas, repo_path, state, mdata, _start_date):
    # https://developer.github.com/v3/issues/labels/
    # not sure if incremental key
    # 'https://api.github.com/repos/{}/labels?sort=created_at&direction=desc'.format(repo_path)

    with metrics.record_counter('issue_labels') as counter:
        for response in authed_get_all_pages(
                'issue_labels',
                'https://api.github.com/repos/{}/labels'.format(repo_path)
        ):
            issue_labels = response.json()
            extraction_time = singer.utils.now()
            for r in issue_labels:
                r['_sdc_repository'] = repo_path

                # transform and write release record
                with singer.Transformer() as transformer:
                    rec = transformer.transform(r, schemas, metadata=metadata.to_map(mdata))
                singer.write_record('issue_labels', rec, time_extracted=extraction_time)
                singer.write_bookmark(state, repo_path, 'issue_labels', {'since': singer.utils.strftime(extraction_time)})
                counter.increment()

    return state

def get_all_commit_comments(schemas, repo_path, state, mdata, start_date):
    # https://developer.github.com/v3/repos/comments/
    # updated_at? incremental
    # 'https://api.github.com/repos/{}/comments?sort=created_at&direction=desc'.format(repo_path)
    bookmark_value = get_bookmark(state, repo_path, "commit_comments", "since", start_date)
    if bookmark_value:
        bookmark_time = singer.utils.strptime_to_utc(bookmark_value)
    else:
        bookmark_time = 0

    with metrics.record_counter('commit_comments') as counter:
        for response in authed_get_all_pages(
                'commit_comments',
                'https://api.github.com/repos/{}/comments?sort=created_at&direction=desc'.format(repo_path)
        ):
            commit_comments = response.json()
            extraction_time = singer.utils.now()
            for r in commit_comments:
                r['_sdc_repository'] = repo_path

                # skip records that haven't been updated since the last run
                # the GitHub API doesn't currently allow a ?since param for pulls
                # once we find the first piece of old data we can return, thanks to
                # the sorting
                if bookmark_time and singer.utils.strptime_to_utc(r.get('updated_at')) < bookmark_time:
                    return state

                # transform and write release record
                with singer.Transformer() as transformer:
                    rec = transformer.transform(r, schemas, metadata=metadata.to_map(mdata))
                singer.write_record('commit_comments', rec, time_extracted=extraction_time)
                singer.write_bookmark(state, repo_path, 'commit_comments', {'since': singer.utils.strftime(extraction_time)})
                counter.increment()

    return state

def get_all_projects(schemas, repo_path, state, mdata, start_date):
    bookmark_value = get_bookmark(state, repo_path, "projects", "since", start_date)
    if bookmark_value:
        bookmark_time = singer.utils.strptime_to_utc(bookmark_value)
    else:
        bookmark_time = 0

    with metrics.record_counter('projects') as counter:
        #pylint: disable=too-many-nested-blocks
        for response in authed_get_all_pages(
                'projects',
                'https://api.github.com/repos/{}/projects?sort=created_at&direction=desc'.format(repo_path),
                { 'Accept': 'application/vnd.github.inertia-preview+json' }
        ):
            projects = response.json()
            extraction_time = singer.utils.now()
            for r in projects:
                r['_sdc_repository'] = repo_path

                # skip records that haven't been updated since the last run
                # the GitHub API doesn't currently allow a ?since param for pulls
                # once we find the first piece of old data we can return, thanks to
                # the sorting
                if bookmark_time and singer.utils.strptime_to_utc(r.get('updated_at')) < bookmark_time:
                    return state

                # transform and write release record
                with singer.Transformer() as transformer:
                    rec = transformer.transform(r, schemas, metadata=metadata.to_map(mdata))
                singer.write_record('projects', rec, time_extracted=extraction_time)
                singer.write_bookmark(state, repo_path, 'projects', {'since': singer.utils.strftime(extraction_time)})
                counter.increment()

                project_id = r.get('id')

                # sync project_columns if that schema is present (only there if selected)
                if schemas.get('project_columns'):
                    for project_column_rec in get_all_project_columns(project_id, schemas['project_columns'], repo_path, state, mdata, start_date):
                        singer.write_record('project_columns', project_column_rec, time_extracted=extraction_time)
                        singer.write_bookmark(state, repo_path, 'project_columns', {'since': singer.utils.strftime(extraction_time)})

                        # sync project_cards if that schema is present (only there if selected)
                        if schemas.get('project_cards'):
                            column_id = project_column_rec['id']
                            for project_card_rec in get_all_project_cards(column_id, schemas['project_cards'], repo_path, state, mdata, start_date):
                                singer.write_record('project_cards', project_card_rec, time_extracted=extraction_time)
                                singer.write_bookmark(state, repo_path, 'project_cards', {'since': singer.utils.strftime(extraction_time)})
    return state


def get_all_project_cards(column_id, schemas, repo_path, state, mdata, start_date):
    bookmark_value = get_bookmark(state, repo_path, "project_cards", "since", start_date)
    if bookmark_value:
        bookmark_time = singer.utils.strptime_to_utc(bookmark_value)
    else:
        bookmark_time = 0

    with metrics.record_counter('project_cards') as counter:
        for response in authed_get_all_pages(
                'project_cards',
                'https://api.github.com/projects/columns/{}/cards?sort=created_at&direction=desc'.format(column_id)
        ):
            project_cards = response.json()
            for r in project_cards:
                r['_sdc_repository'] = repo_path

                # skip records that haven't been updated since the last run
                # the GitHub API doesn't currently allow a ?since param for pulls
                # once we find the first piece of old data we can return, thanks to
                # the sorting
                if bookmark_time and singer.utils.strptime_to_utc(r.get('updated_at')) < bookmark_time:
                    return state

                # transform and write release record
                with singer.Transformer() as transformer:
                    rec = transformer.transform(r, schemas, metadata=metadata.to_map(mdata))
                counter.increment()
                yield rec

    return state

def get_all_project_columns(project_id, schemas, repo_path, state, mdata, start_date):
    bookmark_value = get_bookmark(state, repo_path, "project_columns", "since", start_date)
    if bookmark_value:
        bookmark_time = singer.utils.strptime_to_utc(bookmark_value)
    else:
        bookmark_time = 0

    with metrics.record_counter('project_columns') as counter:
        for response in authed_get_all_pages(
                'project_columns',
                'https://api.github.com/projects/{}/columns?sort=created_at&direction=desc'.format(project_id)
        ):
            project_columns = response.json()
            for r in project_columns:
                r['_sdc_repository'] = repo_path

                # skip records that haven't been updated since the last run
                # the GitHub API doesn't currently allow a ?since param for pulls
                # once we find the first piece of old data we can return, thanks to
                # the sorting
                if bookmark_time and singer.utils.strptime_to_utc(r.get('updated_at')) < bookmark_time:
                    return state

                # transform and write release record
                with singer.Transformer() as transformer:
                    rec = transformer.transform(r, schemas, metadata=metadata.to_map(mdata))
                counter.increment()
                yield rec

    return state

def get_all_releases(schemas, repo_path, state, mdata, _start_date):
    # Releases doesn't seem to have an `updated_at` property, yet can be edited.
    # For this reason and since the volume of release can safely be considered low,
    #    bookmarks were ignored for releases.

    with metrics.record_counter('releases') as counter:
        for response in authed_get_all_pages(
                'releases',
                'https://api.github.com/repos/{}/releases?sort=created_at&direction=desc'.format(repo_path)
        ):
            releases = response.json()
            extraction_time = singer.utils.now()
            for r in releases:
                r['_sdc_repository'] = repo_path

                # transform and write release record
                with singer.Transformer() as transformer:
                    rec = transformer.transform(r, schemas, metadata=metadata.to_map(mdata))
                singer.write_record('releases', rec, time_extracted=extraction_time)
                singer.write_bookmark(state, repo_path, 'releases', {'since': singer.utils.strftime(extraction_time)})
                counter.increment()

    return state

def get_all_pull_requests(schemas, repo_path, state, mdata, start_date):
    '''
    https://developer.github.com/v3/pulls/#list-pull-requests
    '''

    bookmark_value = get_bookmark(state, repo_path, "pull_requests", "since", start_date)
    if bookmark_value:
        bookmark_time = singer.utils.strptime_to_utc(bookmark_value)
    else:
        bookmark_time = 0

    with metrics.record_counter('pull_requests') as counter:
        with metrics.record_counter('reviews') as reviews_counter:
            for response in authed_get_all_pages(
                    'pull_requests',
                    'https://api.github.com/repos/{}/pulls?state=all&sort=updated&direction=desc'.format(repo_path)
            ):
                pull_requests = response.json()
                extraction_time = singer.utils.now()
                for pr in pull_requests:


                    # skip records that haven't been updated since the last run
                    # the GitHub API doesn't currently allow a ?since param for pulls
                    # once we find the first piece of old data we can return, thanks to
                    # the sorting
                    if bookmark_time and singer.utils.strptime_to_utc(pr.get('updated_at')) < bookmark_time:
                        return state

                    pr_num = pr.get('number')
                    pr_id = pr.get('id')
                    pr['_sdc_repository'] = repo_path

                    # transform and write pull_request record
                    with singer.Transformer() as transformer:
                        rec = transformer.transform(pr, schemas['pull_requests'], metadata=metadata.to_map(mdata))
                    singer.write_record('pull_requests', rec, time_extracted=extraction_time)
                    singer.write_bookmark(state, repo_path, 'pull_requests', {'since': singer.utils.strftime(extraction_time)})
                    counter.increment()

                    # sync reviews if that schema is present (only there if selected)
                    if schemas.get('reviews'):
                        for review_rec in get_reviews_for_pr(pr_num, schemas['reviews'], repo_path, state, mdata):
                            singer.write_record('reviews', review_rec, time_extracted=extraction_time)
                            singer.write_bookmark(state, repo_path, 'reviews', {'since': singer.utils.strftime(extraction_time)})

                            reviews_counter.increment()

                    # sync review comments if that schema is present (only there if selected)
                    if schemas.get('review_comments'):
                        for review_comment_rec in get_review_comments_for_pr(pr_num, schemas['review_comments'], repo_path, state, mdata):
                            singer.write_record('review_comments', review_comment_rec, time_extracted=extraction_time)
                            singer.write_bookmark(state, repo_path, 'review_comments', {'since': singer.utils.strftime(extraction_time)})

                    if schemas.get('pr_commits'):
                        for pr_commit in get_commits_for_pr(
                                pr_num,
                                pr_id,
                                schemas['pr_commits'],
                                repo_path,
                                state,
                                mdata
                        ):
                            singer.write_record('pr_commits', pr_commit, time_extracted=extraction_time)
                            singer.write_bookmark(state, repo_path, 'pr_commits', {'since': singer.utils.strftime(extraction_time)})

    return state

def get_reviews_for_pr(pr_number, schema, repo_path, state, mdata):
    for response in authed_get_all_pages(
            'reviews',
            'https://api.github.com/repos/{}/pulls/{}/reviews'.format(repo_path,pr_number)
    ):
        reviews = response.json()
        for review in reviews:
            review['_sdc_repository'] = repo_path
            with singer.Transformer() as transformer:
                rec = transformer.transform(review, schema, metadata=metadata.to_map(mdata))
            yield rec
    return state

def get_review_comments_for_pr(pr_number, schema, repo_path, state, mdata):
    for response in authed_get_all_pages(
            'comments',
            'https://api.github.com/repos/{}/pulls/{}/comments'.format(repo_path,pr_number)
    ):
        review_comments = response.json()
        for comment in review_comments:
            comment['_sdc_repository'] = repo_path
            with singer.Transformer() as transformer:
                rec = transformer.transform(comment, schema, metadata=metadata.to_map(mdata))
            yield rec
    return state

def get_commits_for_pr(pr_number, pr_id, schema, repo_path, state, mdata):
    for response in authed_get_all_pages(
            'pr_commits',
            'https://api.github.com/repos/{}/pulls/{}/commits'.format(repo_path,pr_number)
    ):

        commit_data = response.json()
        for commit in commit_data:
            commit['_sdc_repository'] = repo_path
            commit['pr_number'] = pr_number
            commit['pr_id'] = pr_id
            commit['id'] = '{}-{}'.format(pr_id, commit['sha'])
            with singer.Transformer() as transformer:
                rec = transformer.transform(commit, schema, metadata=metadata.to_map(mdata))
            yield rec

    return state


def get_all_assignees(schema, repo_path, state, mdata, _start_date):
    '''
    https://developer.github.com/v3/issues/assignees/#list-assignees
    '''
    with metrics.record_counter('assignees') as counter:
        for response in authed_get_all_pages(
                'assignees',
                'https://api.github.com/repos/{}/assignees'.format(repo_path)
        ):
            assignees = response.json()
            extraction_time = singer.utils.now()
            for assignee in assignees:
                assignee['_sdc_repository'] = repo_path
                with singer.Transformer() as transformer:
                    rec = transformer.transform(assignee, schema, metadata=metadata.to_map(mdata))
                singer.write_record('assignees', rec, time_extracted=extraction_time)
                singer.write_bookmark(state, repo_path, 'assignees', {'since': singer.utils.strftime(extraction_time)})
                counter.increment()

    return state

def get_all_collaborators(schema, repo_path, state, mdata, _start_date):
    '''
    https://developer.github.com/v3/repos/collaborators/#list-collaborators
    '''
    with metrics.record_counter('collaborators') as counter:
        for response in authed_get_all_pages(
                'collaborators',
                'https://api.github.com/repos/{}/collaborators'.format(repo_path)
        ):
            collaborators = response.json()
            extraction_time = singer.utils.now()
            for collaborator in collaborators:
                collaborator['_sdc_repository'] = repo_path
                with singer.Transformer() as transformer:
                    rec = transformer.transform(collaborator, schema, metadata=metadata.to_map(mdata))
                singer.write_record('collaborators', rec, time_extracted=extraction_time)
                singer.write_bookmark(state, repo_path, 'collaborator', {'since': singer.utils.strftime(extraction_time)})
                counter.increment()

    return state

def get_all_commits(schema, repo_path,  state, mdata, start_date):
    '''
    https://developer.github.com/v3/repos/commits/#list-commits-on-a-repository
    '''
    bookmark = get_bookmark(state, repo_path, "commits", "since", start_date)
    if bookmark:
        query_string = '?since={}'.format(bookmark)
    else:
        query_string = ''

    with metrics.record_counter('commits') as counter:
        for response in authed_get_all_pages(
                'commits',
                'https://api.github.com/repos/{}/commits{}'.format(repo_path, query_string)
        ):
            commits = response.json()
            extraction_time = singer.utils.now()
            for commit in commits:
                commit['_sdc_repository'] = repo_path
                with singer.Transformer() as transformer:
                    rec = transformer.transform(commit, schema, metadata=metadata.to_map(mdata))
                singer.write_record('commits', rec, time_extracted=extraction_time)
                singer.write_bookmark(state, repo_path, 'commits', {'since': singer.utils.strftime(extraction_time)})
                counter.increment()

    return state

def get_all_issues(schema, repo_path,  state, mdata, start_date):
    '''
    https://developer.github.com/v3/issues/#list-issues-for-a-repository
    '''

    bookmark = get_bookmark(state, repo_path, "issues", "since", start_date)
    if bookmark:
        query_string = '&since={}'.format(bookmark)
    else:
        query_string = ''

    with metrics.record_counter('issues') as counter:
        for response in authed_get_all_pages(
                'issues',
                'https://api.github.com/repos/{}/issues?state=all&sort=updated&direction=asc{}'.format(repo_path, query_string)
        ):
            issues = response.json()
            extraction_time = singer.utils.now()
            for issue in issues:
                issue['_sdc_repository'] = repo_path
                with singer.Transformer() as transformer:
                    rec = transformer.transform(issue, schema, metadata=metadata.to_map(mdata))
                singer.write_record('issues', rec, time_extracted=extraction_time)
                singer.write_bookmark(state, repo_path, 'issues', {'since': singer.utils.strftime(extraction_time)})
                counter.increment()
    return state

def get_all_comments(schema, repo_path, state, mdata, start_date):
    '''
    https://developer.github.com/v3/issues/comments/#list-comments-in-a-repository
    '''

    bookmark = get_bookmark(state, repo_path, "comments", "since", start_date)
    if bookmark:
        query_string = '&since={}'.format(bookmark)
    else:
        query_string = ''

    with metrics.record_counter('comments') as counter:
        for response in authed_get_all_pages(
                'comments',
                'https://api.github.com/repos/{}/issues/comments?sort=updated&direction=asc{}'.format(repo_path, query_string)
        ):
            comments = response.json()
            extraction_time = singer.utils.now()
            for comment in comments:
                comment['_sdc_repository'] = repo_path
                with singer.Transformer() as transformer:
                    rec = transformer.transform(comment, schema, metadata=metadata.to_map(mdata))
                singer.write_record('comments', rec, time_extracted=extraction_time)
                singer.write_bookmark(state, repo_path, 'comments', {'since': singer.utils.strftime(extraction_time)})
                counter.increment()
    return state

def get_all_stargazers(schema, repo_path, state, mdata, _start_date):
    '''
    https://developer.github.com/v3/activity/starring/#list-stargazers
    '''

    stargazers_headers = {'Accept': 'application/vnd.github.v3.star+json'}

    with metrics.record_counter('stargazers') as counter:
        for response in authed_get_all_pages(
                'stargazers',
                'https://api.github.com/repos/{}/stargazers'.format(repo_path), stargazers_headers
        ):
            stargazers = response.json()
            extraction_time = singer.utils.now()
            for stargazer in stargazers:
                user_id = stargazer['user']['id']
                stargazer['_sdc_repository'] = repo_path
                with singer.Transformer() as transformer:
                    rec = transformer.transform(stargazer, schema, metadata=metadata.to_map(mdata))
                rec['user_id'] = user_id
                singer.write_record('stargazers', rec, time_extracted=extraction_time)
                singer.write_bookmark(state, repo_path, 'stargazers', {'since': singer.utils.strftime(extraction_time)})
                counter.increment()

    return state


def get_all_repositories(
    org: str,
    excludes=None,
    includes=None,
    include_archived=False,
    include_disabled=False
) -> dict:
    '''
    https://docs.github.com/en/rest/reference/repos
    '''

    if includes is None:
        includes = []
    if excludes is None:
        excludes = []

    headers = {'Accept': 'application/vnd.github.v3+json'}
    per_page = 100
    repos = []
    with metrics.record_counter('repositories') as counter:
        for response in authed_get_all_pages(
            'repositories',
            f"https://api.github.com/orgs/{org}/repos?per_page={per_page}",
            headers
        ):
            repositories = response.json()
            for repository in repositories:

                if should_skip_repo(repository, includes, excludes, include_archived, include_disabled):
                    continue

                repo = {
                    'id': repository['id'],
                    'name': repository.get('name'),
                    'full_name': repository['full_name'],
                    'size': repository['size'],
                    'updated_at': repository['updated_at']
                }
                repos.append(repo)
                counter.increment()

    return repos


def should_skip_repo(
    repository,
    includes,
    excludes,
    include_archived: bool,
    include_disabled: bool,
):
    name = repository.get('name')
    size = repository.get('size')
    is_archived = repository.get('archived', False)
    is_disabled = repository.get('disabled', False)
    match_excludes = next(filter(lambda exclude: fnmatch.fnmatch(name, exclude), excludes), None)
    match_includes = next(filter(lambda include: fnmatch.fnmatch(name, include), includes), None)

    if size <= 0:
        return True

    if is_archived and not include_archived:
        return True

    if is_disabled and not include_disabled:
        return True

    if excludes and match_excludes:
        return True

    if includes and not match_includes:
        return True

    return False



def get_selected_streams(catalog):
    '''
    Gets selected streams.  Checks schema's 'selected'
    first -- and then checks metadata, looking for an empty
    breadcrumb and mdata with a 'selected' entry
    '''
    selected_streams = []
    for stream in catalog['streams']:
        stream_metadata = stream['metadata']
        if stream['schema'].get('selected', False):
            selected_streams.append(stream['tap_stream_id'])
        else:
            for entry in stream_metadata:
                # stream metadata will have empty breadcrumb
                if not entry['breadcrumb'] and entry['metadata'].get('selected',None):
                    selected_streams.append(stream['tap_stream_id'])

    return selected_streams

def get_stream_from_catalog(stream_id, catalog):
    for stream in catalog['streams']:
        if stream['tap_stream_id'] == stream_id:
            return stream
    return None

SYNC_FUNCTIONS = {
    'commits': get_all_commits,
    'comments': get_all_comments,
    'issues': get_all_issues,
    'assignees': get_all_assignees,
    'collaborators': get_all_collaborators,
    'pull_requests': get_all_pull_requests,
    'releases': get_all_releases,
    'stargazers': get_all_stargazers,
    'events': get_all_events,
    'issue_events': get_all_issue_events,
    'issue_milestones': get_all_issue_milestones,
    'issue_labels': get_all_issue_labels,
    'projects': get_all_projects,
    'commit_comments': get_all_commit_comments,
    'teams': get_all_teams
}

SUB_STREAMS = {
    'pull_requests': ['reviews', 'review_comments', 'pr_commits'],
    'projects': ['project_cards', 'project_columns'],
    'teams': ['team_members', 'team_memberships']
}

def do_sync(config, state, catalog):
    access_token = config['access_token']
    session.headers.update({'authorization': 'token ' + access_token})

    start_date = config['start_date'] if 'start_date' in config else None
    # get selected streams, make sure stream dependencies are met
    selected_stream_ids = get_selected_streams(catalog)
    validate_dependencies(selected_stream_ids)

    repositories = get_repos_from_config(config)

    state = translate_state(state, catalog, repositories)
    singer.write_state(state)

    #pylint: disable=too-many-nested-blocks
    for repo in repositories:
        logger.info("Starting sync of repository: %s", repo)
        for stream in catalog['streams']:
            stream_id = stream['tap_stream_id']
            stream_schema = stream['schema']
            mdata = stream['metadata']

            # if it is a "sub_stream", it will be sync'd by its parent
            if not SYNC_FUNCTIONS.get(stream_id):
                continue

            # if stream is selected, write schema and sync
            if stream_id in selected_stream_ids:
                singer.write_schema(stream_id, stream_schema, stream['key_properties'])

                # get sync function and any sub streams
                sync_func = SYNC_FUNCTIONS[stream_id]
                sub_stream_ids = SUB_STREAMS.get(stream_id, None)

                # sync stream
                if not sub_stream_ids:
                    state = sync_func(stream_schema, repo, state, mdata, start_date)

                # handle streams with sub streams
                else:
                    stream_schemas = {stream_id: stream_schema}

                    # get and write selected sub stream schemas
                    for sub_stream_id in sub_stream_ids:
                        if sub_stream_id in selected_stream_ids:
                            sub_stream = get_stream_from_catalog(sub_stream_id, catalog)
                            stream_schemas[sub_stream_id] = sub_stream['schema']
                            singer.write_schema(sub_stream_id, sub_stream['schema'],
                                                sub_stream['key_properties'])

                    # sync stream and it's sub streams
                    state = sync_func(stream_schemas, repo, state, mdata, start_date)

                singer.write_state(state)


def _validate_repo_config(organization, repos_include, repos_exclude):

    if not organization:
        if [repo for repo in repos_include if '*' in repo]:
            raise InvalidParametersException("organization is required when repos_include/repository contains wildcard matchers")

        if [repo for repo in repos_exclude if '*' in repo]:
            raise InvalidParametersException("organization is required when repos_exclude contains wildcard matchers")

        if not repos_include:
            raise InvalidParametersException("organization is required when repos_include/repository isn't present")

        if repos_exclude:
            raise InvalidParametersException("repos_exclude requires organization")

    if organization and [repo for repo in repos_include if '/' in repo]:
        raise InvalidParametersException("org prefix not allowed in repos_include/repository when organization is present")

    if [repo for repo in repos_exclude if '/' in repo]:
        raise InvalidParametersException("org prefix not allowed in repos_exclude")

    if '*' in repos_exclude:
        raise InvalidParametersException("exclude all '*' cannot be used with repos_exclude")


def get_repos_from_config(config) -> list:
    organization = config.get('organization')

    repos_include = [repo for repo in config.get('repos_include', '').split(' ') if repo] \
        + [repo for repo in config.get('repository', '').split(' ') if repo]

    repos_exclude = [repo for repo in config.get('repos_exclude', '').split(' ') if repo]

    _validate_repo_config(organization, repos_include, repos_exclude)

    if not organization and repos_include:
        return [_get_repository_full_name(organization, r) for r in repos_include]
    else:
        include_archived = config.get('include_archived', False)
        include_disabled = config.get('include_disabled', False)
        repos = get_all_repositories(
            org=organization,
            excludes=repos_exclude,
            includes=repos_include,
            include_archived=include_archived,
            include_disabled=include_disabled
        )

        return [r['full_name'] for r in repos]


def _get_repository_full_name(org, repo_name):
    if '/' in repo_name:
        return repo_name
    else:
        return f"{org}/{repo_name}"

@singer.utils.handle_top_exception(logger)
def main():
    args = singer.utils.parse_args(REQUIRED_CONFIG_KEYS)

    global MAX_RATE_LIMIT_WAIT_SECONDS

    if "max_rate_limit_wait_seconds" in args.config:
        MAX_RATE_LIMIT_WAIT_SECONDS = args.config["max_rate_limit_wait_seconds"]

    if args.discover:
        do_discover(args.config)
    else:
        catalog = args.properties if args.properties else get_catalog()
        do_sync(args.config, args.state, catalog)

if __name__ == '__main__':
    main()
