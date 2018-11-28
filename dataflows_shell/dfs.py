import sys
import os
import json
import yaml
from contextlib import redirect_stdout
from glob import iglob
from dataflows import Flow, printer
from .processors import AUTONUMBER_CHECKPOINT_NAME_PREFIX, clear_autonumbered_checkpoints
from .processor_spec import parse_processor_spec
from .dfs_args import parse_dfs_args
from .dfs_load_steps import get_dfs_load_steps
from .dfs_fields_steps import get_dfs_fields_steps
from .dfs_dump_steps import get_dfs_dump_steps
from .dfs_shell import dfs_shell
from .dfs_debugger import DfsDebuggerFlow


def file_get_contents(name):
    with open(os.path.join(os.path.dirname(__file__), '..', name)) as f:
        for line in f.readlines():
            print(line.rstrip())


def dfs_import(args):
    for arg in args:
        print(f'alias {arg}="dfs {arg}"')
    exit(0)


def dfs(args):
    if len(args) == 0 or args[0].endswith('.dfs'):
        dfs_shell(args)
        return True
    processor_spec = args.pop(0).strip()
    if processor_spec == 'import':
        for arg in args:
            print(f'alias {arg}="dfs {arg}"')
        return True
    if processor_spec == 'get-checkpoint-path':
        checkpoint = args[0]
        assert checkpoint.startswith('checkpoint:')
        checkpoint = checkpoint.replace('checkpoint:', '')
        try:
            checkpoint = int(checkpoint)
            print(f'.dfs-checkpoints/__{checkpoint}')
        except Exception:
            print(f'.dfs-checkpoints/{checkpoint}')
        return True
    if processor_spec in ['-h', '--help', 'help']:
        print(file_get_contents('README.md'))
        return True
    if processor_spec in ['--clear', '-c']:
        stats = {}
        clear_autonumbered_checkpoints(stats)
        print(f'Cleared {stats["cleared_dfs_checkpoints"]} checkpoints', file=sys.stderr)
        return True
    if processor_spec in ['--version', 'version', '-v']:
        print(file_get_contents('VERSION.txt'))
        return True
    if processor_spec in ['--list-checkpoints', '-ls']:
        num_autonumbered = 0
        for file in iglob('.dfs-checkpoints/*'):
            file = file.replace('.dfs-checkpoints/', '')
            if file.startswith(AUTONUMBER_CHECKPOINT_NAME_PREFIX):
                num_autonumbered += 1
            else:
                print(f'checkpoint:{file}', file=sys.stderr)
        print(f'{num_autonumbered} autonumbered checkpoints')
        return True
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
        print_steps = printer(num_rows=dfs_kwargs['print'], fields=print_fields),
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
        chain = (
            *get_dfs_load_steps(dfs_kwargs, load_stats),
            *processor_steps,
            *get_dfs_fields_steps(dfs_kwargs),
            *print_steps,
            *get_dfs_dump_steps(dfs_kwargs, dump_stats),
        )
        if dfs_kwargs.get('debugger'):
            flow = DfsDebuggerFlow(chain, context={} if dfs_kwargs['debugger'] is True else dfs_kwargs['debugger'])
        else:
            flow = Flow(*chain)
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
    return True
