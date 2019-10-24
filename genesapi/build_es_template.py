"""
build elasticsearch index template
"""


import json
import logging
import sys


logger = logging.getLogger(__name__)


def _get_template(schema, args):
    return {
        'index_patterns': [args.index_pattern],
        'mappings': {
            'properties': {**{
                field: {'type': 'keyword'} for field in set(
                    dimension for statistic in schema.values()
                    for attribute in statistic.get('attributes', {}).values()
                    for dimension in attribute.get('dimensions', {}).keys()
                ) | set(['region_id', 'nuts', 'lau', 'cube', 'statistic'])
            }, **{'path': {'type': 'object'}, 'year': {'type': 'short'}}}
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
