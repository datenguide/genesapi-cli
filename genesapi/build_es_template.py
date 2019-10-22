"""
build elasticsearch index template
"""


import json
import logging
import sys


logger = logging.getLogger(__name__)


def _get_template(schema, args):
    mapping = {
        field: {'type': 'keyword'}
        for field in set(f for v in schema.values() for f in v.get('attributes', {}).keys()
                         | set(['region_id', 'year', 'nuts', 'lau', 'cube', 'statistic']))
    }
    return {
        'index_patterns': [args.index],
        'mappings': {
            'properties': mapping
        },
        'settings': {
            'index.mapping.total_fields.limit': 100000,
            'index.number_of_shards': args.shards,
            'index.number_of_replicas': args.replicas
        }
    }


def main(args):
    with open(args.schema) as f:
        schema = json.load(f)

    sys.stdout.write(json.dumps(_get_template(schema, args), indent=2))
