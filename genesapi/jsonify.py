"""
jsonify cubes records
"""


import json
import logging
import os
import pandas as pd
import sys

from genesapi.storage import Storage
from genesapi.util import (
    serialize_fact,
    parallelize
)


logger = logging.getLogger(__name__)


def _get_fact(fact, cube_name, args):
    data = serialize_fact(fact, cube_name)
    if args.output:
        path = os.path.join(args.output, cube_name)
        os.makedirs(path, exist_ok=True)
        with open(os.path.join(path, '%s.json' % fact['fact_id']), 'w') as f:
            if args.pretty:
                json.dump(data, f, indent=2)
            else:
                json.dump(data, f)
    else:
        if args.pretty:
            return json.dumps(data, indent=2)
        else:
            return json.dumps(data)


def _get_facts(facts, cube_name, args):
    res = []
    for fact in facts:
        serialized_fact = _get_fact(fact, cube_name, args)
        res.append(serialized_fact)
    return res


def _long_format(cube, facts, args):
    data = {
        'cube': cube.name,
        'path': facts[0]['path'],  # FIXME
        'format': 'tabular',
        'facts': facts
    }
    if args.output:
        path = os.path.join(args.output, cube.name)
        os.makedirs(path, exist_ok=True)
        with open(os.path.join(path, 'facts_long.json'), 'w') as f:
            if args.pretty:
                json.dump(data, f, indent=2)
            else:
                json.dump(data, f)
    else:
        if args.pretty:
            return json.dumps(data, indent=2)
        else:
            return json.dumps(data)


def _serialize_cube(cubes, args):
    for i, cube in enumerate(cubes):
        logger.info('Loading cube `%s` (%s of %s) ...' % (cube, i + 1, len(cubes)))
        cube = cube.export(args.force_export)
        if args.long_format:
            df = pd.DataFrame(serialize_fact(f, flat=True) for f in cube.facts)
            # FIXME path dict / str
            df['path_str'] = df['path'].map(str)
            for path in df['path_str'].unique():
                data = df[df['path_str'] == path]
                yield _long_format(cube, list(data.T.to_dict().values()), args)
        else:
            facts = parallelize(_get_facts, cube.facts, cube.name, args)
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
    if not args.long_format:
        logger.info('Serialized %s facts.' % i)
    logger.info('Finished serialize %s cubes from `%s` .' % (len(cubes), storage))
