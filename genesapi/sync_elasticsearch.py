"""
load data into elasticsearch index,
create 1 document for each fact
"""


import gc
import json
import logging
import time
import os

from elasticsearch import Elasticsearch
from elasticsearch.helpers import parallel_bulk

from genesapi.util import get_chunks, get_files, get_cube, serialize_fact


logger = logging.getLogger(__name__)


def _get_fact(fact, cube_name, index):
    action = {
        '_op_type': 'index',
        '_index': index,
        '_type': 'fact',
    }
    body = serialize_fact(fact, cube_name)
    body['_id'] = body.get('fact_id')
    action.update(body)
    return action


def _get_facts(files, index):
    """
    return generator for facts to index
    that `elasticsearch.helpers.parallel_bulk` will use
    """
    for i, file in enumerate(files):
        logger.log(logging.INFO, 'Loading cube `%s` (%s of %s) ...' % (file, i+1, len(files)))
        cube = get_cube(file)
        for fact in cube.facts:
            yield _get_fact(fact, cube.name, index)


def _get_mapping(schema):
    return {
        'fact': {
            'properties': {
                field: {'type': 'keyword'}
                for field in set(f for v in schema.values() for f in v['args'].keys() | set(['id', 'year']))
            }
        }
    }


def _index_files(client, files, args):
    logger.log(logging.INFO, 'Indexing %s files ...' % len(files))
    res = parallel_bulk(
        client,
        _get_facts(files, args.index),
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

    if not os.path.isdir(args.directory):
        logger.log(logging.ERROR, 'data source `%s` not valid.' % args.directory)
        raise FileNotFoundError(args.directory)

    with open(args.schema) as f:
        schema = json.load(f)

    index_body = {
        'mappings': _get_mapping(schema),
        'settings': {
            'index.mapping.total_fields.limit': 100000,
            'index.number_of_shards': args.shards,
            'index.number_of_replicas': args.replicas
        }
    }

    if not client.indices.exists(args.index):
        client.indices.create(args.index, body=index_body)
    elif args.overwrite:
        client.indices.delete(args.index)
        client.indices.create(args.index, body=index_body)

    es_logger = logging.getLogger('elasticsearch')
    es_logger.setLevel(logging.WARNING)

    if args.split:
        files = get_files(args.directory, lambda x: x.endswith('.csv'))
        chunks = get_chunks(files, args.split)
        for chunk in chunks:
            _index_files(client, chunk, args)
            logger.log(logging.INFO, 'Waiting for 30sec to cool down ...')
            gc.collect()
            del gc.garbage[:]
            time.sleep(30)
    else:
        files = get_files(args.directory, lambda x: x.endswith('.csv'))
        _index_files(client, files, args)
