"""
jsonify cubes records
"""


import json
import logging
import os
import sys

from genesapi.storage import Storage
from genesapi.util import (
    serialize_fact,
    parallelize,
    get_fulltext_data,
    unpack_fact
)


logger = logging.getLogger(__name__)


def _get_facts(facts, cube, args):
    res = []
    for fact in facts:
        i = 0
        for unpacked_fact in unpack_fact(fact, cube.schema):
            data = serialize_fact(unpacked_fact, cube)
            if args.fulltext:
                data.update(get_fulltext_data(data, cube))
            if args.output:
                path = os.path.join(args.output, cube.name)
                os.makedirs(path, exist_ok=True)
                with open(os.path.join(path, '%s.json' % unpacked_fact['fact_id']), 'w') as f:
                    if args.pretty:
                        json.dump(data, f, indent=2)
                    else:
                        json.dump(data, f)
            else:
                if args.pretty:
                    res.append(json.dumps(data, indent=2))
                else:
                    res.append(json.dumps(data))

            i += 1

        if i > 1:
            logger.log(logging.DEBUG, 'unpacked %s facts' % i)
    return res


def _serialize_cube(cubes, args):
    for i, cube in enumerate(cubes):
        logger.info('Loading cube `%s` (%s of %s) ...' % (cube, i + 1, len(cubes)))
        facts = parallelize(_get_facts, cube.export(args.force_export).facts, cube, args)
        for fact in facts:
            yield fact


def main(args):
    if args.output and not os.path.isdir(args.output):
        logger.error('output `%s` not valid.' % args.output)
        raise FileNotFoundError(args.output)

    storage = Storage(args.storage)
    cubes = storage.get_cubes_for_export(args.force_export)
    logger.info('Starting to serialize %s cubes from `%s` ...' % (len(cubes), storage))

    i = 0
    if len(cubes) == 0:
        logger.info('Everything seems up to date.')
    else:
        storage.touch('last_exported')  # set timestamp before to avoid potential race conditions
        for data in _serialize_cube(cubes, args):
            if not args.output:
                sys.stdout.write(data + '\n')
            i += 1
    logger.info('Serialized %s facts.' % i)
    logger.info('Finished serialize %s cubes from `%s` .' % (len(cubes), storage))
