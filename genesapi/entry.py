import argparse
import logging
import sys

from importlib import import_module


COMMANDS = {
    'clean': {
        'args': ({
            'flag': 'source',
            'help': 'CSV File or directory containing files (subdirectories will be included)'
        }, {
            'flag': 'defaults',
            'help': 'YAML File with default transform specification'
        }, {
            'flag': '--yaml',
            'help': 'YAML File with transform specification'
        }, {
            'flag': '--yaml-dir',
            'help': 'Look for yaml specifications in this directory (subdirectories will be included)'
        }, {
            'flag': '--target-dir',
            'help': 'Directory where to put cleaned csv files into'
        }, {
            'flag': '--head',
            'type': int,
            'help': 'Print only this amount of lines for testing purposes'
        }, {
            'flag': '--dtypes',
            'help': 'JSON file where to output dtypes metadata for keys'
        })
    },
    'transform': {
        'args': ({
            'flag': 'source',
            'help': 'CSV File or directory containing cleaned files (subdirectories will be included)'
        }, {
            'flag': '--dtypes',
            'help': 'JSON file with dtypes metadata for keys'
        }, {
            'flag': '--head',
            'type': int,
            'help': 'Print only this amount of lines for testing purposes'
        })
    },
    'build_tree': {
        'args': ({
            'flag': 'source',
            'help': 'CSV file of the transformed path data'
        }, {
            'flag': 'key_tree',
            'help': 'JSON file to output keys tree to'
        }, {
            'flag': '--split',
            'help': 'Split trees by id and save them as JSON to this directory'
        }, {
            'flag': '--fix',
            'help': 'YAML file with data fixes, currently only works without `--split` option'
        })
    },
    'sync_elasticsearch': {
        'args': ({
            'flag': 'source',
            'help': 'JSON tree file our directory containing trees'
        }, {
            'flag': '--host',
            'help': 'Elasticsearch host:port',
            'default': 'localhost:9200'
        }, {
            'flag': '--index',
            'help': 'Name of elasticsearch index',
            'default': 'genesapi'
        }, {
            'flag': '--overwrite',
            'help': 'Overwrite existing index',
            'action': 'store_true'
        }, {
            'flag': '--quiet',
            'help': 'Don\'t raise exceptions from elasticsearch client',
            'action': 'store_true',
            'default': False
        }, {
            'flag': '--jobs',
            'help': 'Thread count for `parallel_bulk`',
            'type': int,
            'default': 8
        }, {
            'flag': '--queue-size',
            'help': 'Queue size for `parallel_bulk`',
            'type': int,
            'default': 8
        }, {
            'flag': '--chunk-size',
            'help': 'Number of documents per chunk',
            'type': int,
            'default': 100
        }, {
            'flag': '--chunk-bytes',
            'help': 'Maximum bytes per chunk',
            'type': int,
            'default': 512000000
        })
    },
    'build_keys': {
        'args': ({
            'flag': 'source',
            'help': 'Directory where keys json are, including subdirectories'
        },)
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
