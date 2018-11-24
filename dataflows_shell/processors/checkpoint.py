from dataflows import checkpoint as dataflows_checkpoint
from glob import iglob
import os
import shutil


AUTONUMBER_CHECKPOINT_NAME_PREFIX='__'


def clear_autonumbered_checkpoints(dfs_stats, checkpoint_path='.dfs-checkpoints'):
    dfs_stats['cleared_dfs_checkpoints'] = 0
    for path in iglob(f'{checkpoint_path}/*'):
        name = path[len(checkpoint_path) + 1:]
        if name.startswith(AUTONUMBER_CHECKPOINT_NAME_PREFIX):
            shutil.rmtree(path)
            dfs_stats['cleared_dfs_checkpoints'] += 1


class checkpoint(dataflows_checkpoint):

    def __init__(self, checkpoint_name=None, checkpoint_path='.dfs-checkpoints',
                 force_dump=False, force_load=False, load_last_checkpoint=False,
                 clear=False, dfs_stats=None, **kwargs):
        if dfs_stats is not None:
            self.dfs_stats = dfs_stats
        else:
            self.dfs_stats = dict()
        if clear:
            clear_autonumbered_checkpoints(dfs_stats, checkpoint_path)
        self.force_dump = force_dump
        self.force_load = force_load
        self.load_last_checkpoint = load_last_checkpoint
        if not checkpoint_name:
            last_checkpoint_num = 0
            for path in iglob(f'{checkpoint_path}/*'):
                name = path[len(checkpoint_path)+1:]
                if name.startswith(AUTONUMBER_CHECKPOINT_NAME_PREFIX):
                    num = int(name.replace(AUTONUMBER_CHECKPOINT_NAME_PREFIX, ''))
                    if num > last_checkpoint_num:
                        last_checkpoint_num = num
            if self.load_last_checkpoint:
                checkpoint_num = last_checkpoint_num
            else:
                checkpoint_num = last_checkpoint_num + 1
            self.dfs_stats['checkpoint_num'] = checkpoint_num
            checkpoint_name = f'{AUTONUMBER_CHECKPOINT_NAME_PREFIX}{checkpoint_num}'
        elif isinstance(checkpoint_name, int):
            checkpoint_name = f'{AUTONUMBER_CHECKPOINT_NAME_PREFIX}{checkpoint_name}'
        self.dfs_stats['checkpoint_name'] = checkpoint_name
        super().__init__(checkpoint_name, checkpoint_path, **kwargs)

    def _preprocess_chain(self):
        if self.force_dump and os.path.exists(self.checkpoint_path):
            shutil.rmtree(self.checkpoint_path)
        checkpoint_package_json_path = os.path.join(self.checkpoint_path, 'datapackage.json')
        if self.force_load and not os.path.exists(checkpoint_package_json_path):
            raise Exception(f'Missing checkpoint: {checkpoint_package_json_path}')
        if self.load_last_checkpoint and not os.path.exists(checkpoint_package_json_path):
            return self.chain
        else:
            return super()._preprocess_chain()
