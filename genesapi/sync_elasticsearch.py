"""
load data into elasticsearch index,
create 1 document for each region
"""


import json
import logging
import os

from elasticsearch import Elasticsearch
from elasticsearch.helpers import parallel_bulk

from genesapi.util import get_files


logger = logging.getLogger(__name__)


def _get_doc(id_, body):
    logger.log(logging.DEBUG, 'indexing `%s` ...' % id_)
    action = {
        '_op_type': 'index',
        '_index': 'genesapi',
        '_type': 'region',
        '_id': id_
    }
    action.update(body)
    logger.log(logging.DEBUG, json.dumps(action))
    return action


def _get_docs(source):
    """
    return iterator for documents to index
    that `elasticsearch.helpers.parallel_bulk` will use
    """

    if os.path.isfile(source):
        with open(source) as f:
            data = json.load(f)
        for id_, body in data.items():
            yield _get_doc(id_, body)

    elif os.path.isdir(source):
        files = get_files(source, lambda x: x.endswith('.json'))
        for file in files:
            with open(file) as f:
                data = json.load(f)
            id_, body = data.items()
            yield _get_doc(id_, body)

    else:
        logger.log(logging.ERROR, 'data source `%s` not valid.' % source)


def main(args):
    client = Elasticsearch(hosts=[args.host])
    logger.log(logging.INFO, 'Using %s' % client)

    index_body = {}
    index_body['settings'] = {
        "index.mapping.total_fields.limit": 100000
    }

    if not client.indices.exists(args.index):
        client.indices.create(args.index, body=index_body)
    elif args.overwrite:
        client.indices.delete(args.index)
        client.indices.create(args.index, body=index_body)

    res = [i for i in parallel_bulk(
        client,
        _get_docs(args.source),
        thread_count=args.jobs,
        chunk_size=args.chunk_size,
        queue_size=args.queue_size,
        max_chunk_bytes=args.chunk_bytes,
        raise_on_error=not args.quiet,
        raise_on_exception=not args.quiet
    )]
    success = len([r for r in res if r[0]])
    errors = len(res) - success
    logger.log(logging.INFO, 'indexed %s regions.' % success)
    if errors:
        logger.log(logging.ERROR, 'errors: %s' % errors)
