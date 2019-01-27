import argparse
import logging
import sys

from importlib import import_module


COMMANDS = {
    'fetch': {
        'args': ({
            'flag': 'catalog',
            'help': 'YAML file with regensis catalog config'
        }, {
            'flag': 'output',
            'help': 'Directory where to store cube data'
        }, {
            'flag': '--replace',
            'help': 'Replace existing (previously downloaded) cubes',
            'action': 'store_true',
            'default': False
        })
    },
    'fetch_attributes': {
        'args': ({
            'flag': 'catalog',
            'help': 'YAML file with catalog config'
        }, {
            'flag': 'output',
            'help': 'Directory where to store attributes data'
        }, {
            'flag': '--replace',
            'help': 'Replace existing (previously downloaded) attributes',
            'action': 'store_true',
            'default': False
        })
    },
    'build_schema': {
        'args': ({
            'flag': 'directory',
            'help': 'Directory with raw cubes downloaded via the `fetch` command'
        }, {
            'flag': '--attributes',
            'help': 'Directory where JSON files of attribute descriptions are stored'
        })
    },
    'build_markdown': {
        'args': ({
            'flag': 'schema',
            'help': 'JSON file from `build_schema` output'
        }, {
            'flag': 'output',
            'help': 'Output directory.'
        })
    },
    'build_es_template': {
        'args': ({
            'flag': 'schema',
            'help': 'JSON file from `build_schema` output'
        }, {
            'flag': '--index',
            'help': 'Name of elasticsearch index',
            'default': 'genesapi'
        }, {
            'flag': '--shards',
            'help': 'Number of shards for elasticsearch index',
            'type': int,
            'default': 5
        }, {
            'flag': '--replicas',
            'help': 'Number of replicas for elasticsearch index',
            'type': int,
            'default': 0
        })
    },
    'jsonify': {
        'args': ({
            'flag': 'directory',
            'help': 'Directory with raw cubes downloaded via the `fetch` command'
        }, {
            'flag': '--output',
            'help': 'Output directory. If none, print each record per line to stdout'
        }, {
            'flag': '--pretty',
            'help': 'Print pretty indented json (for debugging purposes)',
            'action': 'store_true',
            'default': False
        })
    }
}


def main():
    parser = argparse.ArgumentParser(prog='genesapi')
    parser.add_argument('--loglevel', default='INFO')
    subparsers = parser.add_subparsers(help='commands help')
    for name, opts in COMMANDS.items():
        subparser = subparsers.add_parser(name)
        subparser.set_defaults(func=name)
        for args in opts.get('args', []):
            flag = args.pop('flag')
            subparser.add_argument(flag, **args)

    args = parser.parse_args()
    logging.basicConfig(stream=sys.stderr, level=getattr(logging, args.loglevel))

    if hasattr(args, 'func'):
        try:
            func = import_module('genesapi.%s' % args.func)
            func.main(args)
        except ImportError:
            raise Exception('`%s` is not a valid command.' % args.func)
