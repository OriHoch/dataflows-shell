import os
import requests
import datetime
from dataflows import Flow


SCHEMAS = {
    'groups': {'fields': [{'name': n, 'type': t} for n, t in {
        'users': 'array',
        'display_name': 'string',
        'description': 'string',
        'image_display_url': 'string',
        'type': 'boolean',
        'created': 'datetime',
        'name': 'string',
        'is_organization': 'boolean',
        'state': 'string',
        'extras': 'array',
        'image_url': 'string',
        'title': 'string',
        'revision_id': 'string',
        'num_followers': 'integer',
        'id': 'string'
    }.items()]},
    'users': {'fields': [{'name': n, 'type': t} for n, t in {
        'display_name': 'string',
        'name': 'string',
        'created': 'datetime',
        'id': 'string',
        'sysadmin': 'boolean',
        'state': 'string',
        'fullname': 'string',
        'email': 'string',
        'number_created_packages': 'integer',
        'number_of_edits': 'integer'
    }.items()]}
}

PAGINATION_LIMITS = {
    'groups': 500
}

INITIAL_REQUEST_DATA = {
    'groups': {'all_fields': True, 'include_dataset_count': False, 'include_extras': True, 'include_users': True},
    'users': {'all_fields': True}
}

ACTION_URL_PATH = {
    'groups': '/api/3/action/group_list',
    'users': '/api/3/action/user_list',
}


def get_requests_session(CKAN_API_KEY):
    CKAN_AUTH_HEADERS = {'Authorization': CKAN_API_KEY}
    session = requests.session()
    session.headers.update(CKAN_AUTH_HEADERS)
    return session


def parse_ckan_row(row, schema):
    output_row = {}
    for field in schema['fields']:
        value = row[field['name']]
        value = {
            'string': str,
            'integer': int,
            'datetime': lambda v: datetime.datetime.strptime(v, '%Y-%m-%dT%H:%M:%S.%f'),
            'boolean': bool,
            'array': lambda v: v
        }[field['type']](value)
        output_row[field['name']] = value
    return output_row


def get(session, url, obj_type):
    schema = SCHEMAS[obj_type]
    limit = PAGINATION_LIMITS.get(obj_type)
    request_data = INITIAL_REQUEST_DATA[obj_type]
    url_path = ACTION_URL_PATH[obj_type]

    def __get_objs_page():
        groups_res = session.post(f'{url}{url_path}', data=request_data).json()
        assert groups_res and groups_res['success']
        return groups_res['result']

    def __get():
        if limit:
            offset = 0
            while True:
                request_data.update(limit=limit, offset=offset)
                objs = __get_objs_page()
                num_objs = 0
                for num_objs, obj in enumerate(objs, 1):
                    yield parse_ckan_row(obj, schema)
                if num_objs == 0:
                    break
                offset += num_objs
        else:
            for obj in __get_objs_page():
                yield parse_ckan_row(obj, schema)

    def _get(package):
        package.pkg.add_resource({'name': f'ckan_{obj_type}', 'path': f'ckan_{obj_type}.csv', 'schema': schema})
        yield package.pkg
        yield from package
        yield __get()

    return _get


def _ckan(action, obj_type, session, url):
    if action == 'get':
        if obj_type in ['group', 'groups']:
            return get(session, url, 'groups')
        elif obj_type in ['user', 'users']:
            return get(session, url, 'users')
    raise NotImplementedError


def ckan(action, obj_type, CKAN_API_KEY=None, CKAN_URL=None):
    if not CKAN_API_KEY:
        CKAN_API_KEY = os.environ.get('CKAN_API_KEY')
    if not CKAN_URL:
        CKAN_URL = os.environ.get('CKAN_URL')
    assert CKAN_API_KEY and CKAN_URL
    return _ckan(action, obj_type, get_requests_session(CKAN_API_KEY), CKAN_URL)
