import os
import subprocess
import sys


def exec(exec_field='exec', returncode_field=None):

    def exec_rows(rows):
        for row in rows:
            if exec_field in row:
                print(f'exec: {row[exec_field]}')
                p = subprocess.Popen(row[exec_field], shell=True, stderr=sys.stderr, stdout=sys.stderr)
                returncode = p.wait()
                if returncode_field is not None:
                    row[returncode_field] = returncode
            yield row

    def _exec(package):
        if returncode_field is not None:
            for resource in package.pkg.descriptor['resources']:
                if returncode_field not in [f['name'] for f in resource['schema']['fields']]:
                    resource['schema']['fields'].append({'name': returncode_field,
                                                         'type': 'integer'})
        yield package.pkg
        for rows in package:
            yield exec_rows(rows)

    return _exec
