import sys
import secrets
from dataflows import load
from .processors import checkpoint, add_field


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
        return (checkpoint(load_last_checkpoint=True, clear=clear, dfs_stats=stats),
                get_text_file_stdin_processor(resource_name),
                add_field('line', type='string'),
                add_field('line_length', type='integer'))
    elif load_source == 'checkpoint':
        return checkpoint(load_last_checkpoint=True, clear=clear, dfs_stats=stats),
    elif load_source.startswith('checkpoint:'):
        checkpoint_source = load_source.replace('checkpoint:', '')
        try:
            return checkpoint(int(checkpoint_source), force_load=True, clear=clear,
                                                         dfs_stats=stats),
        except ValueError:
            return checkpoint(checkpoint_source, force_load=True, clear=clear,
                                                         dfs_stats=stats),
    else:
        return load(f'{load_source}/datapackage.json'),


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
        return checkpoint(load_last_checkpoint=True, clear=clear, dfs_stats=stats),
