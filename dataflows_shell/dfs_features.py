import os
import sys
from functools import lru_cache


@lru_cache(maxsize=1)
def _get_feature(name):
    if os.environ.get('DFS_FEATURE_ENABLE_ALL'):
        res = True
    else:
        res = bool(os.environ.get(f'DFS_FEATURE_{name}', ''))
    if res:
        print(f'DFS_FEATURE_{name}: enabled', file=sys.stderr)
    return res

HISTORY = _get_feature('HISTORY')
USE_HOME_DIR_FOR_CHECKPOINTS = _get_feature('USE_HOME_DIR_FOR_CHECKPOINTS')
LAMBDA_FILTER = _get_feature('LAMBDA_FILTER')
OUTPUT_FORMAT_FIRST_VALUE = _get_feature('OUTPUT_FORMAT_FIRST_VALUE')
HTML_REQUESTS = _get_feature('HTML_REQUESTS')
