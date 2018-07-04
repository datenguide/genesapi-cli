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
            'flag': '--raw',
            'help': 'Store cube data raw (default)',
            'action': 'store_true',
            'default': True
        })
    },
    'fetch_attributes': {
        'args': ({
            'flag': 'catalog',
            'help': 'YAML file with catalog config'
        }, {
            'flag': 'output',
            'help': 'Directory where to store attributes data'
        })
    },
    'build_schema': {
        'args': ({
            'flag': 'directory',
            'help': 'Directory with raw cubes downloaded via the `fetch` command'
        }, {
            'flag': '--keys-directory',
            'help': 'Directory where JSON files of key description are'
        })
    },
    'sync_elasticsearch': {
        'args': ({
            'flag': 'directory',
            'help': 'Directory with raw cubes downloaded via the `fetch` command'
        }, {
            'flag': 'schema',
            'help': 'JSON file from `build_schema` output'
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
            'flag': '--shards',
            'help': 'Number of shards for elasticsearch index',
            'type': int,
            'default': 5
        }, {
            'flag': '--replicas',
            'help': 'Number of replicas for elasticsearch index',
            'type': int,
            'default': 0
        }, {
            'flag': '--jobs',
            'help': 'Thread count for `parallel_bulk`',
            'type': int,
            'default': 4
        }, {
            'flag': '--queue-size',
            'help': 'Queue size for `parallel_bulk`',
            'type': int,
            'default': 4
        }, {
            'flag': '--chunk-size',
            'help': 'Number of documents per chunk',
            'type': int,
            'default': 1000
        }, {
            'flag': '--chunk-bytes',
            'help': 'Maximum bytes per chunk',
            'type': int,
            'default': 512000000
        }, {
            'flag': '--split',
            'help': '''Split the overall indexing into several chunks
                       with breaks in between for the computer to cool down...''',
            'type': int,
            'default': 0
        }, {
            'flag': '--stop-on-error',
            'help': 'Stop indexing process if 1 fact could not be indexed',
            'action': 'store_true',
            'default': False
        })
    },
    'sync_extra_facts': {
        'args': ({
            'flag': 'source',
            'help': 'CSV File with extra data'
        }, {
            'flag': 'schema',
            'help': 'JSON File with schema from `build_schema`'
        }, {
            'flag': '--overwrite-schema',
            'help': 'Overwrite existing keys in schema',
            'action': 'store_true',
            'default': False
        }, {
            'flag': '--host',
            'help': 'Elasticsearch host:port',
            'default': 'localhost:9200'
        }, {
            'flag': '--index',
            'help': 'Name of elasticsearch index',
            'default': 'genesapi'
        }, {
            'flag': '--quiet',
            'help': 'Don\'t raise exceptions from elasticsearch client',
            'action': 'store_true',
            'default': False
        }, {
            'flag': '--jobs',
            'help': 'Thread count for `parallel_bulk`',
            'type': int,
            'default': 4
        }, {
            'flag': '--queue-size',
            'help': 'Queue size for `parallel_bulk`',
            'type': int,
            'default': 4
        }, {
            'flag': '--chunk-size',
            'help': 'Number of documents per chunk',
            'type': int,
            'default': 1000
        }, {
            'flag': '--chunk-bytes',
            'help': 'Maximum bytes per chunk',
            'type': int,
            'default': 512000000
        }, {
            'flag': '--stop-on-error',
            'help': 'Stop indexing process if 1 fact could not be indexed',
            'action': 'store_true',
            'default': False
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
