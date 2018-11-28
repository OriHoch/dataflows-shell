import os
import subprocess
import sys
import tempfile
from .processors import clear_autonumbered_checkpoints


def dfs_shell(args):
    if len(args) > 0:
        dfs_script = args[0]
        dfs_script_args = args[1:]
    else:
        dfs_script, dfs_script_args = None, None
    if not dfs_script:
        print('\nDataFlows Shell\n')
        print("press <CTRL + C> to exit the shell")
        print("press <Enter> to switch between DataFlows shell and system shell")
        print("type '--help' for the DataFlows Shell reference")
        print()
    stats = {}
    clear_autonumbered_checkpoints(stats)
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
        exit(0)
