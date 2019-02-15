"""
jsonify cubes records
"""


import json
import logging
import os
import sys

from genesapi.util import (
    compute_fact_id,
    get_files,
    load_cube,
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


def _get_json_facts(files, args):
    for i, file in enumerate(files):
        logger.log(logging.INFO, 'Loading cube `%s` (%s of %s) ...' % (file, i+1, len(files)))
        cube = load_cube(file)
        facts = parallelize(_get_facts, cube.facts, cube.name, args)
        for fact in facts:
            yield fact


def main(args):
    if args.output and not os.path.isdir(args.output):
        logger.log(logging.ERROR, 'output `%s` not valid.' % args.output)
        raise FileNotFoundError(args.output)

    files = get_files(args.directory, lambda x: x.endswith('.csv'))

    facts = _get_json_facts(files, args)

    for fact in facts:
        if not args.output:
            sys.stdout.write(fact + '\n')
