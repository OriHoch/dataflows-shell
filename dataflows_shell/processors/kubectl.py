from dataflows import Flow
import subprocess
import json


def _kubectl(action, obj_type):
    res = subprocess.check_output(f'kubectl {action} {obj_type} -o json', shell=True)
    res = json.loads(res)
    assert res['kind'] == 'List'
    for row in res['items']:
        metadata = row.pop('metadata')
        spec = row.pop('spec')
        row.update(metadata)
        row.update(spec)
        yield row


def kubectl(action, obj_type):

    return Flow(
        _kubectl(action, obj_type)
    )
