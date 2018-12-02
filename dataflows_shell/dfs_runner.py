import os
import sys
import tempfile
import subprocess
import json
from os.path import expanduser
from glob import iglob
from dataflows_shell import dfs_features
import dataflows_shell.processors


def dfs_shell(runner):
    if len(runner.args) > 0:
        dfs_script = runner.args[0]
        dfs_script_args = runner.args[1:]
    else:
        dfs_script, dfs_script_args = None, None
    if not dfs_script:
        print('\nDataFlows Shell\n')
        print("press <CTRL + C> to exit the shell")
        print("press <Enter> to switch between DataFlows shell and system shell")
        print("type '--help' for the DataFlows Shell reference")
        print()
    stats = {}
    dataflows_shell.processors.clear_autonumbered_checkpoints(stats, checkpoint_path=runner.checkpoints_path)
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
        return True


def dfs_import(runner):
    for arg in runner.args:
        print(f'alias {arg}="dfs {arg}"')
    return True


def dfs_get_checkpoint_path(runner):
    checkpoint = runner.args[0]
    assert checkpoint.startswith('checkpoint:')
    checkpoint = checkpoint.replace('checkpoint:', '')
    try:
        checkpoint = int(checkpoint)
        print(f'{runner.checkpoints_path}/__{checkpoint}')
    except Exception:
        print(f'{runner.checkpoints_path}/{checkpoint}')
    return True


def print_file(name):
    with open(os.path.join(os.path.dirname(__file__), '..', name)) as f:
        for line in f.readlines():
            print(line.rstrip())
    return True


def dfs_help():
    return print_file('README.md')


def dfs_version():
    return print_file('VERSION.txt')


def dfs_clear(runner):
    stats = {}
    dataflows_shell.processors.clear_autonumbered_checkpoints(stats, checkpoint_path=runner.checkpoints_path)
    print(f'Cleared {stats["cleared_dfs_checkpoints"]} checkpoints', file=sys.stderr)
    return True


def dfs_list_checkpoints(runner):
    num_autonumbered = 0
    for file in iglob(f'{runner.checkpoints_path}/*'):
        file = file.replace(f'{runner.checkpoints_path}/', '')
        if file.startswith(dataflows_shell.processors.AUTONUMBER_CHECKPOINT_NAME_PREFIX):
            num_autonumbered += 1
        else:
            print(f'checkpoint:{file}', file=sys.stderr)
    print(f'{num_autonumbered} autonumbered checkpoints')
    return True


def dfs_run_processor(runner):
    return dataflows_shell.processors.dfs(*runner.args, **runner.kwargs, dfs_runner=runner).run()


class DfsRunner(object):

    def __init__(self, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs
        if dfs_features.USE_HOME_DIR_FOR_CHECKPOINTS:
            self.checkpoints_path = expanduser('~/.dataflows_shell/checkpoints')
            if '~/.dataflows_shell/' in self.checkpoints_path:
                os.makedirs(expanduser('~/.dataflows_shell'), exist_ok=True)
        else:
            self.checkpoints_path = '.dfs-checkpoints'

    def run(self, action):
        if action == 'shell':
            return dfs_shell(self)
        elif action == 'import':
            return dfs_import(self)
        elif action == 'get-checkpoint-path':
            return dfs_get_checkpoint_path(self)
        elif action == 'help':
            return dfs_help()
        elif action == 'clear':
            return dfs_clear(self)
        elif action == 'version':
            return dfs_version()
        elif action == 'list-checkpoints':
            return dfs_list_checkpoints(self)
        elif action == 'processor':
            return dfs_run_processor(self)
        else:
            raise NotImplementedError(f'Unknown dfs action: {action}')
