import sys, os
import json
from dataflows_shell import dfs_features
from os.path import expanduser
import datetime
from dataflows_shell.dfs_runner import DfsRunner

def is_string_dfs_kwarg(key):
    fields = ['load', 'dump', 'args', 'kwargs', 'print-fields', 'print-format', 'fields']
    if dfs_features.LAMBDA_FILTER:
        fields.append('lambda-filter')
    return key in fields


def infer_processor_arg_types(args):
    for arg in args:
        try:
            arg = int(arg)
        except ValueError:
            pass
        yield arg


def infer_processor_kwarg_types(kwargs):
    for k, v in kwargs.items():
        try:
            v = int(v)
        except ValueError:
            pass
        kwargs[k] = v


def get_short_arg_key_value(arg):
    for short, long in {'d': 'dump',
                        'l': 'load',
                        'a': 'args',
                        'k': 'kwargs',
                        'f': 'print-fields',
                        'o': 'print-format',}.items():
        if arg.startswith(f'-{short}='):
            key = long
            value = '='.join(arg.split('=')[1:])
            return key, value
    return None, None


def parse_dfs_args(args):
    processor_args, processor_kwargs, dfs_kwargs = [], {}, {}
    for arg in args:
        if arg in ['--clear', '-c']:
            dfs_kwargs['clear'] = True
        elif arg in ['--quiet', '-q']:
            dfs_kwargs['quiet'] = True
        elif arg in ['--print', '-p']:
            dfs_kwargs['print'] = True
        elif arg == '--async':
            dfs_kwargs['dry_run'] = True
        else:
            key, val = get_short_arg_key_value(arg)
            if key:
                dfs_kwargs[key] = val
            elif arg.startswith('--'):
                arg = arg[2:].split('=')
                key = arg[0]
                value = '='.join(arg[1:])
                if is_string_dfs_kwarg(key):
                    if key in dfs_kwargs:
                        if isinstance(key, list):
                            dfs_kwargs[key].append(value)
                        else:
                            dfs_kwargs[key] = [dfs_kwargs[key], value]
                    else:
                        dfs_kwargs[key] = value
                else:
                    processor_kwargs[key] = value
            else:
                processor_args.append(arg)
    processor_args = list(infer_processor_arg_types(processor_args))
    infer_processor_kwarg_types(processor_kwargs)
    if 'args' in dfs_kwargs:
        processor_args += json.loads(dfs_kwargs['args'])
    if 'kwargs' in dfs_kwargs:
        processor_kwargs.update(json.loads(dfs_kwargs['kwargs']))
    return processor_args, processor_kwargs, dfs_kwargs


def dfs():
    args = sys.argv[1:]
    if dfs_features.HISTORY:
        history_file_path = os.environ.get('DFS_HISTORY_FILE_PATH', '~/.dataflows_shell/history')
        if '~/.dataflows_shell/' in history_file_path:
            os.makedirs(expanduser('~/.dataflows_shell'), exist_ok=True)
        with open(expanduser(history_file_path), 'a') as f:
            f.write('# ' + datetime.datetime.now().strftime('%Y-%m-%d %H:%m:%S') + "\n")
            f.write(' '.join(args) + "\n")
    action, kwargs = None, {}
    if len(args) == 0 or args[0].endswith('.dfs'):
        action = 'shell'
    else:
        processor_spec = args.pop(0).strip()
        if processor_spec == 'import':
            action = 'import'
        elif processor_spec == 'get-checkpoint-path':
            action = 'get-checkpoint-path'
        elif processor_spec in ['-h', '--help', 'help']:
            action = 'help'
        elif processor_spec in ['--clear', '-c']:
            action = 'clear'
        elif processor_spec in ['--version', 'version', '-v']:
            action = 'version'
        elif processor_spec in ['--list-checkpoints', '-ls']:
            action = 'list-checkpoints'
        else:
            action = 'processor'
            processor_args, processor_kwargs, dfs_kwargs = parse_dfs_args(args)
            kwargs.update(processor_spec=processor_spec, **dfs_kwargs, **processor_kwargs)
            args = processor_args
    assert DfsRunner(*args, **kwargs).run(action)
    exit(0)


if __name__ == '__main__':
    dfs()
