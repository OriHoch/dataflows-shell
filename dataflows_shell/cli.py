import sys
from .dfs import dfs


def cli():
    exit(0 if dfs(sys.argv[1:]) else 1)


if __name__ == '__main__':
    cli()
