from dataflows import Flow, set_type
from glob import iglob
import os


def get_path_row(path):
    path_type = 'dir' if os.path.isdir(path) else 'file'
    file_size = 0
    if path_type == 'file':
        try:
            file_size = os.path.getsize(path)
        except Exception:
            path_type = '?'
    return {'path': path, 'path_type': path_type, 'file_size': file_size}


def ls(glob_pattern='*', recursive=False):
    iglob_generator = iglob(glob_pattern, recursive=recursive)
    try:
        first_path = next(iglob_generator)
    except StopIteration:
        first_path = None

    def _ls():
        if first_path:
            yield get_path_row(first_path)
        for path in iglob_generator:
            yield get_path_row(path)

    type_steps = (set_type('path', type='string'),
                  set_type('path_type', type='string'),
                  set_type('file_size', type='integer')) if first_path else ()

    return Flow(_ls(), *type_steps)
