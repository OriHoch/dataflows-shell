from dataflows import dump_to_path
from .processors import checkpoint


def get_dfs_dump_steps(dfs_kwargs, stats):
    if 'dump' in dfs_kwargs:
        dump_spec = dfs_kwargs['dump']
        if dump_spec == 'null':
            return ()
        if dump_spec in ['', 'checkpoint']:
            return checkpoint(force_dump=True, dfs_stats=stats),
        elif dump_spec.startswith('checkpoint:'):
            checkpoint_spec = dump_spec.replace('checkpoint:', '')
            try:
                return checkpoint(int(checkpoint_spec), force_dump=True, dfs_stats=stats),
            except ValueError:
                return checkpoint(checkpoint_spec, force_dump=True, dfs_stats=stats),
        else:
            stats['dump_path'] = dump_spec
            return dump_to_path(dump_spec),
    else:
        return checkpoint(dfs_stats=stats),
