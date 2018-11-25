import sys, os
from dataflows import Flow
import dataflows_shell.processors
import dataflows.processors
from contextlib import redirect_stdout
import json, yaml
import subprocess
import tempfile
from glob import iglob
from importlib import import_module
import secrets


def is_valid_built_in_processor_spec(processor_spec):
    for c in ['_', '/', '.']:
        if processor_spec.startswith(c):
            return False
    if ' ' in processor_spec:
        return False
    return True


def parse_processor_spec(processor_spec):
    processor_func = None
    if processor_spec.startswith('lambda row: '):
        def _lambda_func(*args, **kwargs):
            return eval(processor_spec)
        processor_func = _lambda_func
    elif is_valid_built_in_processor_spec(processor_spec):
        if hasattr(dataflows_shell.processors, processor_spec):
            processor_func = getattr(dataflows_shell.processors, processor_spec)
        elif hasattr(dataflows.processors, processor_spec):
            processor_func = getattr(dataflows.processors, processor_spec)
    if not processor_func:
        module_spec, processor_spec = processor_spec.split(':')
        flow_module = import_module(module_spec)
        processor_func = getattr(flow_module, processor_spec)
    return processor_func


def is_string_dfs_kwarg(key):
    return key in ['load', 'dump', 'args', 'kwargs', 'print-fields', 'print-format', 'fields']


def get_text_file_stdin_processor(name):

    schema = {'fields': [{'name': 'line_length', 'type': 'integer'},
                         {'name': 'line', 'type': 'string'},]}

    def text_file_processor(package):
        package.pkg.add_resource({'name': name, 'path': f'{name}.csv', 'schema': schema})
        yield package.pkg
        yield from package
        yield ({'line_length': len(line.rstrip()), 'line': line.rstrip()}
               for i, line in enumerate(sys.stdin.readlines()))

    return text_file_processor


def get_single_load_source_steps(load_source, stats, clear):
    if load_source == 'null':
        return ()
    elif load_source.startswith('stdin'):
        if load_source.startswith('stdin:'):
            resource_name = load_source.replace('stdin:', '')
        else:
            resource_name = 'stdin-' + secrets.token_hex(5)
        return (dataflows_shell.processors.checkpoint(load_last_checkpoint=True, clear=clear, dfs_stats=stats),
                get_text_file_stdin_processor(resource_name),
                dataflows_shell.processors.add_field('line', type='string'),
                dataflows_shell.processors.add_field('line_length', type='integer'))
    elif load_source == 'checkpoint':
        return dataflows_shell.processors.checkpoint(load_last_checkpoint=True, clear=clear, dfs_stats=stats),
    elif load_source.startswith('checkpoint:'):
        checkpoint_source = load_source.replace('checkpoint:', '')
        try:
            return dataflows_shell.processors.checkpoint(int(checkpoint_source), force_load=True, clear=clear,
                                                         dfs_stats=stats),
        except ValueError:
            return dataflows_shell.processors.checkpoint(checkpoint_source, force_load=True, clear=clear,
                                                         dfs_stats=stats),
    else:
        return dataflows.processors.load(f'{load_source}/datapackage.json'),


def get_dfs_load_steps(dfs_kwargs, stats):
    clear = dfs_kwargs.get('clear', False)
    if 'load' in dfs_kwargs:
        load_source = dfs_kwargs['load']
        if isinstance(load_source, list):
            steps = []
            for source in load_source:
                steps += list(get_single_load_source_steps(source, {}, clear))
            print(steps, file=sys.stderr)
            return tuple(steps)
        else:
            return get_single_load_source_steps(load_source, stats, clear)
    else:
        return dataflows_shell.processors.checkpoint(load_last_checkpoint=True, clear=clear, dfs_stats=stats),


def get_dfs_fields_steps(dfs_kwargs):
    steps = []
    if 'fields' in dfs_kwargs:
        delete_fields = set()
        for field in dfs_kwargs['fields'].split(','):
            field = field.strip()
            if field.startswith('+'):
                if ':' in field:
                    tmpfield = field[1:].split(':')
                    field_name = tmpfield[0]
                    field_type = ':'.join(tmpfield[1:])
                    if field_type in ['string', 'integer', 'date', 'number', 'boolean']:
                        steps.append(dataflows_shell.processors.add_field(field_name, field_type))
                        continue
                field = field[1:].split('=')
                field_name = field[0]
                field_value = '='.join(field[1:]) if len(field) > 1 else ''
                steps.append(dataflows_shell.processors.add_field(field_name, 'string', field_value))
            elif field.startswith('-'):
                delete_fields.add(field[1:])
            else:
                field = field.split(':')
                field_name = field[0]
                field_type = field[1] if len(field) > 1 else 'string'
                steps.append(dataflows.processors.set_type(field_name, type=field_type))
        if len(delete_fields) > 0:
            steps.append(dataflows.processors.delete_fields(list(delete_fields)))
    return tuple(steps)


def get_dfs_dump_steps(dfs_kwargs, stats):
    if 'dump' in dfs_kwargs:
        dump_spec = dfs_kwargs['dump']
        if dump_spec == 'null':
            return ()
        if dump_spec in ['', 'checkpoint']:
            return dataflows_shell.processors.checkpoint(force_dump=True, dfs_stats=stats),
        elif dump_spec.startswith('checkpoint:'):
            checkpoint_spec = dump_spec.replace('checkpoint:', '')
            try:
                return dataflows_shell.processors.checkpoint(int(checkpoint_spec), force_dump=True, dfs_stats=stats),
            except ValueError:
                return dataflows_shell.processors.checkpoint(checkpoint_spec, force_dump=True, dfs_stats=stats),
        else:
            stats['dump_path'] = dump_spec
            return dataflows.processors.dump_to_path(dump_spec),
    else:
        return dataflows_shell.processors.checkpoint(dfs_stats=stats),


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


def print_file(name):
    with open(os.path.join(os.path.dirname(__file__), '..', name)) as f:
        for line in f.readlines():
            print(line.rstrip())


def help():
    print_file('README.md')


def version():
    print_file('VERSION.txt')


def dfs_shell(args):
    if len(args) > 0:
        dfs_script = args[0]
        dfs_script_args = args[1:]
    else:
        dfs_script, dfs_script_args = None, None
    if not dfs_script:
        print('\nDataFlows Shell\n')
        print("press <CTRL + C> to exit the shell")
        print("press <Enter> to switch between DataFlows shell and system shell")
        print("type '--help' for the DataFlows Shell reference")
        print()
    stats = {}
    dataflows_shell.processors.clear_autonumbered_checkpoints(stats)
    if stats["cleared_dfs_checkpoints"] > 0:
        print(f'Cleared {stats["cleared_dfs_checkpoints"]} checkpoints', file=sys.stderr)
    last_checkpoint = None
    is_system_shell = False
    last_pwd = None
    dfs_script_file = open(dfs_script) if dfs_script else None
    try:
        while True:
            args_env = None
            if dfs_script:
                cmd = dfs_script_file.readline()
                if len(cmd) == 0:
                    exit(0)
                if cmd.strip().startswith('#'):
                    continue
                if cmd.strip() == '':
                    continue
                cmd = cmd.rstrip()
                args_env = dict(**os.environ)
                args_env.update(**{f'DFS_ARG{i+1}': str(a) for i, a in enumerate(dfs_script_args)})
                print()
                print(f'dfs > {cmd}', file=sys.stderr)
            elif is_system_shell:
                cmd = input('shell > ')
            else:
                cmd = input('dfs > ')
            if not dfs_script and cmd == '':
                is_system_shell = not is_system_shell
            elif not dfs_script and cmd == '--help':
                help()
            else:
                if cmd.endswith('\\'):
                    while cmd.endswith('\\'):
                        cmd = cmd.rstrip('\\')
                        if dfs_script:
                            cmd += dfs_script_file.readline()
                        else:
                            cmd += ' ' + input(' ')
                _, temp_filename = tempfile.mkstemp()
                if not dfs_script and is_system_shell:
                    if last_pwd is not None:
                        cmd = f'cd "{last_pwd}"; {cmd}'
                    cmd = f'{cmd}; pwd > {temp_filename}'
                    os.system(cmd)
                else:
                    cmd = f'dfs {cmd}'
                    cmd = f'{cmd}; pwd > {temp_filename}'
                    p = subprocess.Popen(cmd, shell=True, stderr=sys.stderr, stdout=subprocess.PIPE,
                                         cwd=last_pwd, env=args_env)
                    last_checkpoint, _ = p.communicate(input=last_checkpoint)
                    if p.returncode != 0:
                        print(f'cmd={cmd}, env={args_env}, cwd={last_pwd}', file=sys.stderr)
                        raise Exception()
                with open(temp_filename) as f:
                    last_pwd = f.read().strip()
                os.unlink(temp_filename)
    except (EOFError, KeyboardInterrupt):
        print()
        exit(0)


def dfs_import(args):
    for arg in args:
        print(f'alias {arg}="dfs {arg}"')
    exit(0)


def dfs():
    args = sys.argv[1:]
    if len(args) == 0 or args[0].endswith('.dfs'):
        dfs_shell(args)
        return
    processor_spec = args.pop(0).strip()
    if processor_spec == 'import':
        dfs_import(args)
        return
    if processor_spec == 'get-checkpoint-path':
        checkpoint = args[0]
        assert checkpoint.startswith('checkpoint:')
        checkpoint = checkpoint.replace('checkpoint:', '')
        try:
            checkpoint = int(checkpoint)
            print(f'.dfs-checkpoints/__{checkpoint}')
        except Exception:
            print(f'.dfs-checkpoints/{checkpoint}')
        exit(0)
    if processor_spec in ['-h', '--help', 'help']:
        help()
        exit(0)
    if processor_spec in ['--clear', '-c']:
        stats = {}
        dataflows_shell.processors.clear_autonumbered_checkpoints(stats)
        print(f'Cleared {stats["cleared_dfs_checkpoints"]} checkpoints', file=sys.stderr)
        exit(0)
    if processor_spec in ['--version', 'version', '-v']:
        version()
        exit(0)
    if processor_spec in ['--list-checkpoints', '-ls']:
        num_autonumbered = 0
        for file in iglob('.dfs-checkpoints/*'):
            file = file.replace('.dfs-checkpoints/', '')
            if file.startswith(dataflows_shell.processors.AUTONUMBER_CHECKPOINT_NAME_PREFIX):
                num_autonumbered += 1
            else:
                print(f'checkpoint:{file}', file=sys.stderr)
        print(f'{num_autonumbered} autonumbered checkpoints')
        exit(0)
    if processor_spec.startswith('-'):
        processor_args, processor_kwargs, dfs_kwargs = parse_dfs_args([processor_spec] + args)
        assert len(processor_args) + len(processor_kwargs) == 0, 'no processor - cannot have processor args'
        processor_steps = ()
    else:
        processor_func = parse_processor_spec(processor_spec)
        processor_args, processor_kwargs, dfs_kwargs = parse_dfs_args(args)
        processor_steps = processor_func(*processor_args, **processor_kwargs),
    print_format = dfs_kwargs.get('print-format', 'text')
    print_fields = dfs_kwargs.get('print-fields')
    if print_fields:
        print_fields = print_fields.split(',')
    if dfs_kwargs.get('print') and print_format == 'text':
        print_steps = dataflows.processors.printer(num_rows=1, fields=print_fields),
    else:
        print_steps = ()
    if 'load' not in dfs_kwargs and not sys.stdin.isatty():
        dfs_kwargs['load'] = sys.stdin.read().strip()
    load_stats = {}
    dump_stats = {}
    if dfs_kwargs.get('quiet') or print_format != 'text':
        f = open(os.devnull, 'w')
    else:
        f = sys.stderr
    with redirect_stdout(f):
        flow = Flow(
            *get_dfs_load_steps(dfs_kwargs, load_stats),
            *processor_steps,
            *get_dfs_fields_steps(dfs_kwargs),
            *print_steps,
            *get_dfs_dump_steps(dfs_kwargs, dump_stats),
        )
        if print_format == 'text':
            flow_dp, flow_stats = flow.process()
            print(flow_stats, file=sys.stderr)
            output_res = None
        else:
            data, pkg, stats = flow.results()
            output_res = [data, pkg.descriptor, stats]
    if print_format == 'text':
        if 'checkpoint_num' in dump_stats:
            print(f'checkpoint:{dump_stats["checkpoint_num"]}')
        elif 'checkpoint_name' in dump_stats:
            print(f'checkpoint:{dump_stats["checkpoint_name"]}')
        elif 'dump_path' in dump_stats:
            print(dump_stats['dump_path'])
        else:
            raise NotImplementedError()
    elif print_format == 'json':
        json.dump(output_res, sys.stdout)
        print('')
    elif print_format == 'yaml':
        yaml.dump(output_res, sys.stdout, default_flow_style=False)
    else:
        raise Exception(f'unsupported print format: {print_format}')
    exit(0)


if __name__ == '__main__':
    dfs()
