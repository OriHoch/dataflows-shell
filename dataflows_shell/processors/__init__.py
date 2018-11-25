# core processors

from .ls import ls
from .checkpoint import checkpoint, clear_autonumbered_checkpoints, AUTONUMBER_CHECKPOINT_NAME_PREFIX
from .exec import exec
from .add_field import add_field

# third party processors

from .kubectl import kubectl
from .ckan import ckan
