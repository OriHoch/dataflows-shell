import json


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


def infer_processor_arg_types(args):
    for arg in args:
        try:
            arg = int(arg)
        except ValueError:
            pass
        yield arg


def is_string_dfs_kwarg(key):
    return key in ['load', 'dump', 'args', 'kwargs', 'print-fields', 'print-format', 'fields']


def parse_dfs_args(args):
    processor_args, processor_kwargs, dfs_kwargs = [], {}, {}
    for arg in args:
        if arg in ['--clear', '-c']:
            dfs_kwargs['clear'] = True
        elif arg in ['--quiet', '-q']:
            dfs_kwargs['quiet'] = True
        elif arg.startswith('--print'):
            dfs_kwargs['print'] = 1 if len(arg) == 7 else int(arg[8:])
        elif arg.startswith('-p'):
            dfs_kwargs['print'] = 1 if len(arg) == 2 else int(arg[2:])
        elif arg.startswith('--debugger'):
            if len(arg) > 11:
                dfs_kwargs['debugger'] = json.loads(arg[11:])
            else:
                dfs_kwargs['debugger'] = True
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
