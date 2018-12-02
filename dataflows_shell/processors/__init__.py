from dataflows_shell.dfs_features import HTML_REQUESTS

# core processors

from .dfs import dfs
from .ls import ls
from .checkpoint import checkpoint, clear_autonumbered_checkpoints, AUTONUMBER_CHECKPOINT_NAME_PREFIX, get_last_checkpoint_num
from .exec import exec
from .add_field import add_field

# third party processors

from .kubectl import kubectl
from .ckan import ckan

if HTML_REQUESTS:
    from .html_requests import html_requests
