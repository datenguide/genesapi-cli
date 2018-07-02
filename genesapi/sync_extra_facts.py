"""
load extra csv data into elasticsearch index,
create 1 document for each fact
"""


import json
import logging
import pandas as pd

from elasticsearch import Elasticsearch
from elasticsearch.helpers import parallel_bulk
from regenesis.util import make_key


logger = logging.getLogger(__name__)


def _get_fact(id_, key, value, index):
    action = {
        '_op_type': 'index',
        '_index': index,
        '_type': 'fact',
        '_id': make_key((id_, key, value)),
        'id': id_,
        key: value
    }
    return action


def _get_facts(df, index):
    """
    return generator for facts to index
    that `elasticsearch.helpers.parallel_bulk` will use
    """
    for _, fact in df.iterrows():
        fact = fact.to_dict()
        id_ = fact.pop('id')
        for key, value in fact.items():
            yield _get_fact(id_, key, value, index)


def _index(client, df, args):
    res = parallel_bulk(
        client,
        _get_facts(df, args.index),
        thread_count=args.jobs,
        chunk_size=args.chunk_size,
        queue_size=min((args.queue_size, args.jobs)),
        max_chunk_bytes=args.chunk_bytes,
        raise_on_error=not args.quiet,
        raise_on_exception=not args.quiet
    )
    success = 0
    errors = 0
    for doc in res:
        if doc[0]:
            success += 1
        if not doc[0]:
            errors += 1
            logger.log(logging.ERROR, 'Could not index document:\n%s' % json.dumps(doc[1], indent=2))
            if args.stop_on_error:
                break

    logger.log(logging.INFO, 'indexed %s facts.' % success)
    if errors:
        logger.log(logging.ERROR, 'errors: %s' % errors)


def main(args):
    client = Elasticsearch(hosts=[args.host], timeout=60)
    logger.log(logging.INFO, 'Using %s' % client)

    facts = pd.read_csv(args.source).fillna('')
    _index(client, facts, args)

    logger.log(logging.INFO, 'Updating schema in `%s` ...' % args.schema)
    with open(args.schema) as f:
        schema = json.load(f)
    del facts['id']
    for key in facts.columns:
        if key in schema and not args.overwrite_schema:
            logger.log(logging.ERROR, '`%s` already in schema' % key)
        else:
            schema[key] = {
                'name': key.title(),
                'source': 'extra'
            }
    with open(args.schema, 'w') as f:
        json.dump(schema, f)
