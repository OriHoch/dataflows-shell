from dataflows import Flow
import dataflows_shell.processors
import dataflows.processors
import secrets
from dataflows_shell import dfs_features
from os.path import expanduser
import os
import sys
from importlib import import_module
from contextlib import redirect_stdout
import json
import yaml


def get_dfs_checkpoints_path():
    if dfs_features.USE_HOME_DIR_FOR_CHECKPOINTS:
        checkpoints_path = expanduser('~/.dataflows_shell/checkpoints')
        if '~/.dataflows_shell/' in checkpoints_path:
            os.makedirs(expanduser('~/.dataflows_shell'), exist_ok=True)
    else:
        checkpoints_path = '.dfs-checkpoints'
    return checkpoints_path


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


def is_async_checkpoint():



def get_single_load_source_steps(load_source, stats, clear, checkpoints_path, dry_run):
    if load_source == 'null':
        if dry_run:
            return 'null'
        else:
            return ()
    elif load_source.startswith('stdin'):
        assert not dry_run, 'dry run is not supported for loading resources from stdin'
        if load_source.startswith('stdin:'):
            resource_name = load_source.replace('stdin:', '')
        else:
            resource_name = 'stdin-' + secrets.token_hex(5)
        return (dataflows_shell.processors.checkpoint(load_last_checkpoint=True, clear=clear, dfs_stats=stats, checkpoint_path=checkpoints_path),
                get_text_file_stdin_processor(resource_name),
                dataflows_shell.processors.add_field('line', type='string'),
                dataflows_shell.processors.add_field('line_length', type='integer'))
    elif load_source == 'checkpoint':
        checkpoint_num = dataflows_shell.processors.get_last_checkpoint_num(checkpoints_path) + 1
        checkpoint_path = f'{checkpoints_path}/__{checkpoint_num}'
        if dry_run:
            if os.path.exists(f'{checkpoint_path}/datapackage.json') or os.path.exists(f'{checkpoint_path}/async.json'):
                return f'checkpoint:{checkpoint_num}'
            else:
                return 'null'
        else:
            if os.path.exists(f'{checkpoint_path}/async.json') and not os.path.exists(f'checkpoint_path}/datapackage.json'):
                with open(f'{checkpoint_path}/async.json') as f:
                    async_load = json.load(f)
            return dataflows_shell.processors.checkpoint(load_last_checkpoint=True, clear=clear, dfs_stats=stats, checkpoint_path=checkpoints_path),
    elif load_source.startswith('checkpoint:'):
        checkpoint_name = load_source.replace('checkpoint:', '')
        if dry_run:
            return f'checkpoint:{checkpoint_name}'
        else:
            return dataflows_shell.processors.checkpoint(checkpoint_name, force_load=True, clear=clear,
                                                         dfs_stats=stats, checkpoint_path=checkpoints_path),
    elif dry_run:
        return load_source
    else:
        return dataflows.processors.load(f'{load_source}/datapackage.json'),


def get_dfs_load_steps(clear, load, stats, checkpoints_path, dry_run=False):
    if load:
        load_source = load
    elif not sys.stdin.isatty():
        load_source = sys.stdin.read().strip()
    else:
        load_source = ''
    if load_source:
        if isinstance(load_source, list):
            steps = []
            if dry_run:
                return [get_single_load_source_steps(src, stats, clear, checkpoints_path, True)
                        for src in load_source]
            else:
                for source in load_source:
                    steps += list(get_single_load_source_steps(source, {}, clear, checkpoints_path, False))
                return tuple(steps)
        elif dry_run:
            return [get_single_load_source_steps(load_source, stats, clear, checkpoints_path, True)]
        else:
            return get_single_load_source_steps(load_source, stats, clear, checkpoints_path, False),
    elif dry_run:
        return [get_single_load_source_steps('checkpoint', stats, clear, checkpoints_path, True)]
    else:
        return get_single_load_source_steps('checkpoint', stats, clear, checkpoints_path, True),


def get_dfs_processor_steps(processor_spec, *processor_args, dry_run=False, **processor_kwargs):
    if not processor_spec:
        assert len(processor_args) + len(processor_kwargs) == 0, 'no processor - cannot have processor args'
        return None if dry_run else ()
    elif dry_run:
        return processor_spec, processor_args, processor_kwargs
    else:
        processor_func = parse_processor_spec(processor_spec)
        return processor_func(*processor_args, **processor_kwargs),


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


def get_dfs_fields_steps(fields):
    steps = []
    if fields:
        delete_fields = set()
        for field in fields.split(','):
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


def get_dfs_filter_steps(lambda_filter):
    if lambda_filter:
        add_lambda_filter = eval(f'lambda row: row.update(_lambda_filter={lambda_filter})')
        return (add_lambda_filter,
                dataflows_shell.processors.add_field('_lambda_filter', 'boolean'),
                dataflows.processors.filter_rows([{"_lambda_filter": True}]))
    else:
        return ()


def get_dfs_printer_steps(_print, print_format, print_fields):
    if _print and print_format == 'text':
        return dataflows.processors.printer(num_rows=1, fields=print_fields),
    else:
        return ()


def get_dfs_dump_steps(dump, stats, checkpoints_path, dry_run=False):
    dump_args, dump_kwargs, dump_processor = [], {}, None
    if dump:
        dump_spec = dump
        if dump_spec == 'null':
            return None if dry_run else ()
        if dump_spec in ['', 'checkpoint']:
            if dry_run:
                checkpoint_num = dataflows_shell.processors.get_last_checkpoint_num(checkpoints_path) + 1
                stats["checkpoint_num"] = checkpoint_num
                return f'{checkpoints_path}/__{checkpoint_num}'
            else:
                dump_kwargs = dict(force_dump=True, dfs_stats=stats, checkpoint_path=checkpoints_path)
                dump_processor = dataflows_shell.processors.checkpoint
        elif dump_spec.startswith('checkpoint:'):
            checkpoint_name = dump_spec.replace('checkpoint:', '')
            if dry_run:
                try:
                    checkpoint_num = int(checkpoint_name)
                    stats["checkpoint_num"] = checkpoint_num
                    return f'{checkpoints_path}/__{checkpoint_num}'
                except Exception:
                    stats["checkpoint_name"] = checkpoint_name
                    return f'{checkpoints_path}/{checkpoint_name}'
            else:
                dump_args = [checkpoint_name]
                dump_kwargs = dict(force_dump=True, dfs_stats=stats, checkpoint_path=checkpoints_path)
                dump_processor = dataflows_shell.processors.checkpoint
        else:
            stats['dump_path'] = dump_spec
            if dry_run:
                return stats['dump_path']
            else:
                dump_args = [dump_spec]
                dump_processor = dataflows.processors.dump_to_path
    else:
        if dry_run:
            checkpoint_num = dataflows_shell.processors.get_last_checkpoint_num(checkpoints_path) + 1
            stats["checkpoint_num"] = checkpoint_num
            return f'{checkpoints_path}/__{checkpoint_num}'
        else:
            dump_kwargs = dict(dfs_stats=stats, checkpoint_path=checkpoints_path)
            dump_processor = dataflows_shell.processors.checkpoint
    return dump_processor(*dump_args, **dump_kwargs),



def get_dfs_dump_path(dump, stats, checkpoints_path, dry_run=False):
    dump_args, dump_kwargs, dump_processor = [], {}, None
    if dump:
        dump_spec = dump
        if dump_spec == 'null':
            return None if dry_run else ()
        if dump_spec in ['', 'checkpoint']:
            if dry_run:
                checkpoint_num = dataflows_shell.processors.get_last_checkpoint_num(checkpoints_path) + 1
                stats["checkpoint_num"] = checkpoint_num
                return f'{checkpoints_path}/__{checkpoint_num}'
            else:
                dump_kwargs = dict(force_dump=True, dfs_stats=stats, checkpoint_path=checkpoints_path)
                dump_processor = dataflows_shell.processors.checkpoint
        elif dump_spec.startswith('checkpoint:'):
            checkpoint_name = dump_spec.replace('checkpoint:', '')
            if dry_run:
                try:
                    checkpoint_num = int(checkpoint_name)
                    stats["checkpoint_num"] = checkpoint_num
                    return f'{checkpoints_path}/__{checkpoint_num}'
                except Exception:
                    stats["checkpoint_name"] = checkpoint_name
                    return f'{checkpoints_path}/{checkpoint_name}'
            else:
                dump_args = [checkpoint_name]
                dump_kwargs = dict(force_dump=True, dfs_stats=stats, checkpoint_path=checkpoints_path)
                dump_processor = dataflows_shell.processors.checkpoint
        else:
            stats['dump_path'] = dump_spec
            if dry_run:
                return stats['dump_path']
            else:
                dump_args = [dump_spec]
                dump_processor = dataflows.processors.dump_to_path
    else:
        if dry_run:
            checkpoint_num = dataflows_shell.processors.get_last_checkpoint_num(checkpoints_path) + 1
            stats["checkpoint_num"] = checkpoint_num
            return f'{checkpoints_path}/__{checkpoint_num}'
        else:
            dump_kwargs = dict(dfs_stats=stats, checkpoint_path=checkpoints_path)
            dump_processor = dataflows_shell.processors.checkpoint
    return dump_processor(*dump_args, **dump_kwargs),


def get_dfs_checkpoint_spec(dfs_dump_stats):
    if 'checkpoint_num' in dfs_dump_stats:
        return f'checkpoint:{dfs_dump_stats["checkpoint_num"]}'
    elif 'checkpoint_name' in dfs_dump_stats:
        return f'checkpoint:{dfs_dump_stats["checkpoint_name"]}'
    elif 'dump_path' in dfs_dump_stats:
        return dfs_dump_stats['dump_path']
    else:
        raise NotImplementedError()


def get_dfs_checkpoint_path(checkpoint_spec, checkpoints_path):
    if checkpoint_spec == 'null':
        return None
    elif checkpoint_spec.startswith('stdin'):
        raise NotImplementedError('cannot get checkpoint path for stdin resources')
    elif checkpoint_spec == 'checkpoint':
        raise NotImplementedError('cannot get checkpoint path for loading from last autonumbered checkpoint')
    elif checkpoint_spec.startswith('checkpoint:'):
        checkpoint_name = checkpoint_spec.replace('checkpoint:', '')
        try:
            checkpoint_num = int(checkpoint_name)
            return f'{checkpoints_path}/__{checkpoint_num}'
        except Exception:
            return f'{checkpoints_path}/{checkpoint_name}'
    else:
        return checkpoint_spec


def get_dfs_steps(checkpoints_path, dfs_load_stats, dfs_dump_stats, print_format, _print, print_fields, dry_run,
                  clear, load, dump, processor_spec, processor_args, processor_kwargs, fields, lambda_filter):
    if dry_run:
        load_sources = get_dfs_load_steps(clear, load, dfs_load_stats, checkpoints_path, dry_run=True)
        dump_args_path = get_dfs_dump_steps(dump, dfs_dump_stats, checkpoints_path, dry_run=True)
        return {'load': {'sources': load_sources, 'clear': clear},
                'processor': {'spec': processor_spec, 'args': processor_args, 'kwargs': processor_kwargs},
                'fields': {'specs': fields},
                'filter': {'lambda_filter': lambda_filter},
                'printer': {'print': _print, 'format': print_format, 'fields': print_fields},
                'dump': {'path': dump_args_path},}
    else:
        return (
            *get_dfs_load_steps(clear, load, dfs_load_stats, checkpoints_path),
            *get_dfs_processor_steps(processor_spec, *processor_args, **processor_kwargs),
            *get_dfs_fields_steps(fields),
            *get_dfs_filter_steps(lambda_filter),
            *get_dfs_printer_steps(_print, print_format, print_fields),
            *get_dfs_dump_steps(dump, dfs_dump_stats, checkpoints_path),
        )


_print_func = print


class dfs(Flow):

    def __init__(
        self,
        *processor_args,
        processor_spec=None,
        clear=False,
        load=None,
        fields=None,
        lambda_filter=None,
        dump=None,
        quiet=None,
        print=None,
        print_format=None,
        print_fields=None,
        dry_run=None,
        dfs_runner=None,
        **processor_kwargs
    ):
        self.dfs_checkpoints_path = get_dfs_checkpoints_path()
        self.dfs_load_stats = {}
        self.dfs_dump_stats = {}
        self.quiet = quiet
        self.print_format = print_format or 'text'
        if print_fields:
            self.print_fields = print_fields.split(',')
        else:
            self.print_fields = []
        self.print = print
        print = _print_func
        self.dry_run = dry_run
        self.clear = clear
        self.dfs_runner = dfs_runner
        if self.dry_run and self.clear:
            assert self.dfs_runner.run('clear')
        dfs_steps = get_dfs_steps(self.dfs_checkpoints_path, self.dfs_load_stats, self.dfs_dump_stats, self.print_format,
                                  self.print, self.print_fields, self.dry_run, self.clear, load, dump, processor_spec,
                                  processor_args, processor_kwargs, fields, lambda_filter)
        if self.dry_run:
            self.dry_run_steps_res = dfs_steps
            dfs_steps = ()
        super().__init__(*dfs_steps)

    def run(self):
        if self.dry_run:
            print(get_dfs_checkpoint_spec(self.dfs_dump_stats))
            async_dump_path = self.dry_run_steps_res['dump']['path']
            if os.path.exists(f'{async_dump_path}/datapackage.json'):
                os.unlink(f'{async_dump_path}/datapackage.json')
            else:
                os.makedirs(async_dump_path, exist_ok=True)
            with open(f'{async_dump_path}/async.json', 'w') as f:
                json.dump(self.dry_run_steps_res, f, indent=2)
            return True
        else:
            if self.quiet or self.print_format != 'text':
                f = open(os.devnull, 'w')
            else:
                f = sys.stderr
            with redirect_stdout(f):
                if self.print_format == 'text':
                    flow_dp, flow_stats = self.process()
                    print(flow_stats, file=sys.stderr)
                    output_res = None
                else:
                    data, pkg, stats = self.results()
                    output_res = [data, pkg.descriptor, stats]
            if self.print_format == 'text':
                print(get_dfs_checkpoint_spec(self.dfs_dump_stats))
            elif self.print_format == 'json':
                json.dump(output_res, sys.stdout)
                print('')
            elif self.print_format == 'yaml':
                yaml.dump(output_res, sys.stdout, default_flow_style=False)
            elif dfs_features.OUTPUT_FORMAT_FIRST_VALUE and self.print_format == 'firstvalue':
                row = output_res[0][0][0]
                if self.print_fields:
                    print(row[self.print_fields[0]])
                else:
                    print(row[list(row.keys())[0]])
            else:
                raise Exception(f'unsupported print format: {self.print_format}')
            return True
