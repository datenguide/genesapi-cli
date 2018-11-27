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
            'help': 'Directory where JSON files of attribute descriptions are'
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
            'flag': '--fulltext',
            'help': 'Add a completion field for fulltext suggestions',
            'action': 'store_true',
            'default': False
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
            'help': 'Print pretty indented json',
            'action': 'store_true',
            'default': False
        }, {
            'flag': '--fulltext',
            'help': 'Create computed string for fulltext search for each fact',
            'action': 'store_true',
            'default': False
        }, {
            'flag': '--schema',
            'help': 'When using `--fulltext`, use this JSON file from `build_schema` output'
        }, {
            'flag': '--names',
            'help': 'When using `--fulltext`, obtain names from this json {id => name} mapping',
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
