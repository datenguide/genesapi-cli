"""
jsonify cubes records
"""


import json
import logging
import os
import sys

from genesapi.storage import Storage
from genesapi.util import (
    compute_fact_id,
    serialize_fact,
    parallelize
)


logger = logging.getLogger(__name__)


def _get_fact(fact, cube_name, args):
    data = serialize_fact(fact, cube_name)
    id_ = compute_fact_id(data)
    data['fact_id'] = id_
    if args.output:
        path = os.path.join(args.output, cube_name)
        os.makedirs(path, exist_ok=True)
        with open(os.path.join(path, '%s.json' % id_), 'w') as f:
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


def _get_json_facts(cubes, args):
    for i, cube in enumerate(cubes):
        logger.log(logging.INFO, 'Loading cube `%s` (%s of %s) ...' % (cube, i+1, len(cubes)))
        cube = cube.export()
        facts = parallelize(_get_facts, cube.facts, cube.name, args)
        for fact in facts:
            yield fact


def main(args):
    if args.output and not os.path.isdir(args.output):
        logger.log(logging.ERROR, 'output `%s` not valid.' % args.output)
        raise FileNotFoundError(args.output)

    storage = Storage(args.storage)
    cubes = storage.get_cubes_for_export()
    storage.touch('last_exported')
    logger.log(logging.INFO, 'Starting to serialize %s cubes from `%s` ...' % (len(cubes), storage))

    if len(cubes) == 0:
        logger.log(logging.INFO, 'Everything seems up to date.')
    else:
        for fact in _get_json_facts(cubes, args):
            if not args.output:
                sys.stdout.write(fact + '\n')
